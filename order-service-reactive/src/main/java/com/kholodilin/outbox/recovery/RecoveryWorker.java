package com.kholodilin.outbox.recovery;

import com.kholodilin.outbox.config.AppProperties;
import com.kholodilin.outbox.logging.StructuredLogContext;
import com.kholodilin.outbox.metrics.OutboxMetrics;
import com.kholodilin.outbox.persistence.OutboxR2dbcRepository;
import com.kholodilin.outbox.queue.InMemoryEventQueue;
import jakarta.annotation.PostConstruct;
import jakarta.annotation.PreDestroy;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;
import reactor.core.Disposable;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.time.Instant;

/**
 * Recovery via {@link Flux#interval}: claimRecoverableIds → clearLease → enqueue.
 */
@Slf4j
@Component
@RequiredArgsConstructor
public class RecoveryWorker {

    private final AppProperties properties;
    private final OutboxR2dbcRepository outboxR2dbcRepository;
    private final InMemoryEventQueue eventQueue;
    private final OutboxMetrics metrics;
    private Disposable subscription;

    @PostConstruct
    public void start() {
        subscription = Flux.interval(properties.getOutbox().getRecovery().getInterval())
                .flatMap(tick -> recover())
                .onErrorContinue((ex, obj) -> log.error("Recovery tick failed", ex))
                .subscribe();
    }

    @PreDestroy
    public void stop() {
        if (subscription != null) {
            subscription.dispose();
        }
    }

    Mono<Void> recover() {
        if (!properties.getOutbox().getRecovery().isEnabled()) {
            return Mono.empty();
        }
        int batchSize = properties.getOutbox().getRecovery().getBatchSize();
        Instant lockedUntil = Instant.now().plus(properties.getOutbox().getPublisher().getLeaseDuration());
        return outboxR2dbcRepository.claimRecoverableIds(batchSize, properties.getInstanceId(), lockedUntil)
                .collectList()
                .flatMap(ids -> {
                    if (ids.isEmpty()) {
                        return Mono.empty();
                    }
                    return enqueueRecovered(ids, lockedUntil);
                });
    }

    private Mono<Void> enqueueRecovered(java.util.List<Long> ids, Instant lockedUntil) {
        StructuredLogContext.putInstanceFields(properties.getInstanceId());
        log.debug("Recovery claimed ids={} lockedBy={} lockedUntil={}", ids, properties.getInstanceId(), lockedUntil);

        return outboxR2dbcRepository.clearLease(ids)
                .then(Mono.fromRunnable(() -> {
                    int enqueued = 0;
                    for (Long id : ids) {
                        if (eventQueue.enqueue(id)) {
                            enqueued++;
                        }
                    }
                    metrics.incrementRecoveryCount(enqueued);
                    StructuredLogContext.putBatchSize(enqueued);
                    StructuredLogContext.putEventAction("outbox.recovery.completed");
                    log.info("Recovery enqueued eventIds count={}", enqueued);
                }));
    }
}
