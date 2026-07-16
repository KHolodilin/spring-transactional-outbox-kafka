package com.kholodilin.outbox.recovery;

import com.kholodilin.outbox.config.AppProperties;
import com.kholodilin.outbox.logging.StructuredLogContext;
import com.kholodilin.outbox.metrics.OutboxMetrics;
import com.kholodilin.outbox.persistence.OutboxJdbcRepository;
import com.kholodilin.outbox.queue.InMemoryEventQueue;
import com.kholodilin.outbox.tracing.OutboxTracing;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

import java.time.Instant;
import java.util.List;

/**
 * Recovery path for events that never reached Kafka (crash after commit, full queue, etc.).
 * <p>
 * Never publishes directly — only re-enqueues event ids into the same in-memory queue
 * used by the fast path so there is a single publishing pipeline.
 */
@Slf4j
@Component
@RequiredArgsConstructor
public class RecoveryWorker {

    private final AppProperties properties;
    private final OutboxJdbcRepository outboxJdbcRepository;
    private final InMemoryEventQueue eventQueue;
    private final OutboxMetrics metrics;
    private final OutboxTracing outboxTracing;

    /**
     * Periodically claims unpublished active outbox rows and re-enqueues their ids.
     * <p>
     * No-op when recovery is disabled or there is nothing to claim. Skips ids already
     * tracked in the in-memory queue ({@link InMemoryEventQueue#isTracked}). Automatic
     * {@code @Scheduled} spans are suppressed by
     * {@link com.kholodilin.outbox.tracing.RecoveryTracingConfig}; a dedicated
     * {@code outbox.recovery} span is opened only when work is performed.
     */
    @Scheduled(fixedDelayString = "${app.outbox.recovery.interval:10s}")
    public void recover() {
        if (!properties.getOutbox().getRecovery().isEnabled()) {
            return;
        }

        int batchSize = properties.getOutbox().getRecovery().getBatchSize();
        Instant lockedUntil = Instant.now().plus(properties.getOutbox().getPublisher().getLeaseDuration());
        List<Long> ids = outboxJdbcRepository.claimRecoverableIds(
                batchSize,
                properties.getInstanceId(),
                lockedUntil
        );
        if (ids.isEmpty()) {
            return;
        }

        outboxTracing.observe("outbox.recovery", () -> enqueueRecovered(ids, lockedUntil));
    }

    private void enqueueRecovered(List<Long> ids, Instant lockedUntil) {
        StructuredLogContext.putInstanceFields(properties.getInstanceId());
        log.debug("Recovery claimed ids={} lockedBy={} lockedUntil={}", ids, properties.getInstanceId(), lockedUntil);

        // Release lease before enqueue so the publisher can claim rows as soon as it polls an id.
        outboxJdbcRepository.clearLease(ids);

        int enqueued = 0;
        for (Long id : ids) {
            if (eventQueue.isTracked(id)) {
                continue;
            }
            if (eventQueue.enqueue(id)) {
                enqueued++;
            }
        }
        metrics.incrementRecoveryCount(enqueued);
        StructuredLogContext.putBatchSize(enqueued);
        StructuredLogContext.putEventAction("outbox.recovery.completed");
        log.info("Recovery enqueued eventIds count={}", enqueued);
        log.debug("Recovery id list={}", ids);
    }
}
