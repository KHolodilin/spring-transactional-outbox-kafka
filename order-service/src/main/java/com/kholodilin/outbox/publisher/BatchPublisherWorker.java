package com.kholodilin.outbox.publisher;

import tools.jackson.databind.ObjectMapper;
import com.kholodilin.outbox.config.AppProperties;
import com.kholodilin.outbox.events.EventEnvelope;
import com.kholodilin.outbox.events.OutboxStatus;
import com.kholodilin.outbox.metrics.OutboxMetrics;
import com.kholodilin.outbox.persistence.OutboxJdbcRepository;
import com.kholodilin.outbox.persistence.OutboxRow;
import com.kholodilin.outbox.logging.StructuredLogContext;
import com.kholodilin.outbox.queue.InMemoryEventQueue;
import com.kholodilin.outbox.tracing.OutboxTracing;
import jakarta.annotation.PostConstruct;
import jakarta.annotation.PreDestroy;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.ObjectProvider;
import org.springframework.stereotype.Component;

import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Background worker that drains the in-memory queue and publishes outbox events to Kafka.
 * <p>
 * Pipeline: poll ids → claim rows in DB (lease) → load payloads → publish batch → archive SENT.
 * On publish failure events become FAILED (or DEAD after max retries) and are eligible for recovery.
 */
@Slf4j
@Component
@RequiredArgsConstructor
public class BatchPublisherWorker {

    private final InMemoryEventQueue eventQueue;
    private final OutboxJdbcRepository outboxJdbcRepository;
    private final ObjectProvider<KafkaBatchPublisher> kafkaBatchPublisher;
    private final OutboxMetrics metrics;
    private final OutboxTracing outboxTracing;
    private final AppProperties properties;
    private final ObjectMapper objectMapper;
    private final AtomicBoolean running = new AtomicBoolean(true);
    private ExecutorService executor;

    /**
     * Starts the single-threaded publisher loop after the Spring context is ready.
     */
    @PostConstruct
    public void start() {
        executor = Executors.newSingleThreadExecutor(r -> {
            Thread thread = new Thread(r, "batch-publisher-worker");
            thread.setDaemon(true);
            return thread;
        });
        executor.submit(this::loop);
    }

    /**
     * Signals the loop to exit and interrupts the publisher thread on context shutdown.
     */
    @PreDestroy
    public void stop() {
        running.set(false);
        if (executor != null) {
            executor.shutdownNow();
        }
    }

    private void loop() {
        long batchWaitMs = properties.getOutbox().getMemoryQueue().getBatchWait().toMillis();
        int batchSize = properties.getOutbox().getMemoryQueue().getBatchSize();
        while (running.get()) {
            try {
                Long firstId = eventQueue.poll(batchWaitMs);
                if (firstId == null) {
                    continue;
                }
                List<Long> ids = new ArrayList<>();
                ids.add(firstId);
                List<Long> drained = eventQueue.drainBatch(batchSize - 1);
                ids.addAll(drained);

                try {
                    // Multi-pod safety: only rows we successfully claim are published.
                    Instant lockedUntil = Instant.now().plus(properties.getOutbox().getPublisher().getLeaseDuration());
                    List<OutboxRow> claimed = outboxTracing.observe("outbox.batch.fetch", () -> outboxJdbcRepository.claimByIds(
                            ids,
                            properties.getInstanceId(),
                            lockedUntil
                    ));
                    if (claimed.isEmpty()) {
                        log.debug("No outbox rows claimed for ids={}", ids);
                        for (Long id : outboxJdbcRepository.findReenqueueableIds(ids)) {
                            eventQueue.enqueue(id);
                        }
                        continue;
                    }

                    StructuredLogContext.putInstanceFields(properties.getInstanceId());
                    StructuredLogContext.putBatchSize(claimed.size());
                    StructuredLogContext.putEventAction("outbox.batch.loaded");

                    List<EventEnvelope> envelopes = new ArrayList<>();
                    for (OutboxRow row : claimed) {
                        String correlationId = extractCorrelationId(row.getPayload());
                        envelopes.add(outboxJdbcRepository.toEnvelope(row, correlationId));
                    }

                    long start = System.nanoTime();
                    try {
                        String batchTraceParent = claimed.stream()
                                .map(OutboxRow::getTraceParent)
                                .filter(parent -> parent != null && !parent.isBlank())
                                .findFirst()
                                .orElse(null);
                        outboxTracing.observeWithTraceParent(batchTraceParent, "outbox.batch.publish", () -> {
                            kafkaBatchPublisher.getObject().publish(envelopes);
                            outboxJdbcRepository.markSent(sentIds(claimed), Instant.now());
                            metrics.publishLatency().record(System.nanoTime() - start, java.util.concurrent.TimeUnit.NANOSECONDS);
                            long durationMs = (System.nanoTime() - start) / 1_000_000;
                            StructuredLogContext.putDurationMs(durationMs);
                            StructuredLogContext.putEventAction("outbox.batch.published");
                            log.info("Kafka batch published size={} durationMs={}",
                                    envelopes.size(),
                                    durationMs);
                        });
                    } catch (Exception ex) {
                        metrics.incrementPublishFailures();
                        StructuredLogContext.putEventAction("outbox.publish.failed");
                        log.warn("Kafka batch publish failed size={} error={}", claimed.size(), ex.getMessage());
                        log.debug("Kafka publish failure", ex);
                        handleFailures(claimed);
                    }
                } finally {
                    eventQueue.acknowledge(ids);
                }
            } catch (InterruptedException ex) {
                Thread.currentThread().interrupt();
                return;
            } catch (Exception ex) {
                log.error("Batch publisher loop error", ex);
            }
        }
    }

    private List<Long> sentIds(List<OutboxRow> claimed) {
        return claimed.stream().map(OutboxRow::getId).toList();
    }

    private void handleFailures(List<OutboxRow> claimed) {
        int maxRetries = properties.getOutbox().getPublisher().getMaxRetries();
        for (OutboxRow row : claimed) {
            int nextRetry = row.getRetryCount() + 1;
            metrics.incrementRetryCount();
            OutboxStatus status = nextRetry >= maxRetries ? OutboxStatus.DEAD : OutboxStatus.FAILED;
            outboxJdbcRepository.markFailed(row.getId(), nextRetry, status);
            StructuredLogContext.putOutboxStatus(status.name(), status.getCode(), nextRetry);
            StructuredLogContext.putEventAction("outbox.retry");
            log.info("Outbox event marked {} eventId={} retryCount={}", status, row.getId(), nextRetry);
        }
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
