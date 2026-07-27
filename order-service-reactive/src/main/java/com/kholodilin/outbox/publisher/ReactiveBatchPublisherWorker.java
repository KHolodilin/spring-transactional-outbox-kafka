package com.kholodilin.outbox.publisher;

import tools.jackson.databind.ObjectMapper;
import com.kholodilin.outbox.config.AppProperties;
import com.kholodilin.outbox.events.EventEnvelope;
import com.kholodilin.outbox.events.OutboxStatus;
import com.kholodilin.outbox.logging.StructuredLogContext;
import com.kholodilin.outbox.metrics.OutboxMetrics;
import com.kholodilin.outbox.persistence.OutboxR2dbcRepository;
import com.kholodilin.outbox.persistence.OutboxRow;
import com.kholodilin.outbox.queue.InMemoryEventQueue;
import jakarta.annotation.PostConstruct;
import jakarta.annotation.PreDestroy;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.ObjectProvider;
import org.springframework.stereotype.Component;
import reactor.core.Disposable;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Reactive batch publisher loop: poll ids → claim → publish (KafkaTemplate on boundedElastic) → mark SENT.
 */
@Slf4j
@Component
@RequiredArgsConstructor
public class ReactiveBatchPublisherWorker {

    private final InMemoryEventQueue eventQueue;
    private final OutboxR2dbcRepository outboxR2dbcRepository;
    private final ObjectProvider<KafkaBatchPublisher> kafkaBatchPublisher;
    private final OutboxMetrics metrics;
    private final AppProperties properties;
    private final ObjectMapper objectMapper;
    private final AtomicBoolean running = new AtomicBoolean(true);
    private Disposable subscription;

    @PostConstruct
    public void start() {
        subscription = Mono.defer(this::processOneBatch)
                .repeat(() -> running.get())
                .onErrorContinue((ex, obj) -> log.error("Batch publisher loop error", ex))
                .subscribe();
    }

    @PreDestroy
    public void stop() {
        running.set(false);
        if (subscription != null) {
            subscription.dispose();
        }
    }

    private Mono<Void> processOneBatch() {
        int batchSize = properties.getOutbox().getMemoryQueue().getBatchSize();
        return eventQueue.pollMono(properties.getOutbox().getMemoryQueue().getBatchWait())
                .flatMap(firstId -> {
                    List<Long> ids = new ArrayList<>();
                    ids.add(firstId);
                    ids.addAll(eventQueue.drainBatch(batchSize - 1));
                    return publishIds(ids).doFinally(signal -> eventQueue.acknowledge(ids));
                })
                .switchIfEmpty(Mono.empty());
    }

    private Mono<Void> publishIds(List<Long> ids) {
        Instant lockedUntil = Instant.now().plus(properties.getOutbox().getPublisher().getLeaseDuration());
        return outboxR2dbcRepository.claimByIds(ids, properties.getInstanceId(), lockedUntil)
                .collectList()
                .flatMap(claimed -> {
                    if (claimed.isEmpty()) {
                        log.debug("No outbox rows claimed for ids={}", ids);
                        return outboxR2dbcRepository.findReenqueueableIds(ids)
                                .doOnNext(eventQueue::enqueue)
                                .then();
                    }

                    StructuredLogContext.putInstanceFields(properties.getInstanceId());
                    StructuredLogContext.putBatchSize(claimed.size());
                    StructuredLogContext.putEventAction("outbox.batch.loaded");

                    List<EventEnvelope> envelopes = new ArrayList<>();
                    for (OutboxRow row : claimed) {
                        envelopes.add(outboxR2dbcRepository.toEnvelope(row, extractCorrelationId(row.payload())));
                    }

                    long start = System.nanoTime();
                    return kafkaBatchPublisher.getObject().publish(envelopes)
                            .then(Mono.defer(() -> {
                                long durationNs = System.nanoTime() - start;
                                metrics.recordPublishedBatch(envelopes.size(), durationNs);
                                return outboxR2dbcRepository.markSent(sentIds(claimed), Instant.now())
                                        .doOnSuccess(v -> {
                                            long durationMs = durationNs / 1_000_000;
                                            StructuredLogContext.putDurationMs(durationMs);
                                            StructuredLogContext.putEventAction("outbox.batch.published");
                                            log.info("Kafka batch published size={} durationMs={}",
                                                    envelopes.size(), durationMs);
                                        });
                            }))
                            .onErrorResume(ex -> {
                                metrics.incrementPublishFailures();
                                StructuredLogContext.putEventAction("outbox.publish.failed");
                                log.warn("Kafka batch publish failed size={} error={}", claimed.size(), ex.getMessage());
                                return handleFailures(claimed);
                            });
                });
    }

    private Mono<Void> handleFailures(List<OutboxRow> claimed) {
        int maxRetries = properties.getOutbox().getPublisher().getMaxRetries();
        return Flux.fromIterable(claimed)
                .concatMap(row -> {
                    int nextRetry = row.retryCount() + 1;
                    metrics.incrementRetryCount();
                    OutboxStatus status = nextRetry >= maxRetries ? OutboxStatus.DEAD : OutboxStatus.FAILED;
                    StructuredLogContext.putOutboxStatus(status.name(), status.getCode(), nextRetry);
                    StructuredLogContext.putEventAction("outbox.retry");
                    log.info("Outbox event marked {} eventId={} retryCount={}", status, row.id(), nextRetry);
                    return outboxR2dbcRepository.markFailed(row.id(), nextRetry, status);
                })
                .then();
    }

    private List<Long> sentIds(List<OutboxRow> claimed) {
        return claimed.stream().map(OutboxRow::id).toList();
    }

    @SuppressWarnings("unchecked")
    private String extractCorrelationId(String payloadJson) {
        try {
            Map<String, Object> payload = objectMapper.readValue(payloadJson, Map.class);
            Object correlationId = payload.get("correlationId");
            return correlationId == null ? null : correlationId.toString();
        } catch (Exception ex) {
            return null;
        }
    }
}
