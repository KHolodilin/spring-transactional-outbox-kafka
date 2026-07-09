package com.kholodilin.outbox.recovery;

import com.kholodilin.outbox.config.AppProperties;
import com.kholodilin.outbox.metrics.OutboxMetrics;
import com.kholodilin.outbox.persistence.OutboxJdbcRepository;
import com.kholodilin.outbox.queue.InMemoryEventQueue;
import lombok.extern.slf4j.Slf4j;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Transactional;

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
public class RecoveryWorker {

    private final AppProperties properties;
    private final OutboxJdbcRepository outboxJdbcRepository;
    private final InMemoryEventQueue eventQueue;
    private final OutboxMetrics metrics;

    public RecoveryWorker(
            AppProperties properties,
            OutboxJdbcRepository outboxJdbcRepository,
            InMemoryEventQueue eventQueue,
            OutboxMetrics metrics
    ) {
        this.properties = properties;
        this.outboxJdbcRepository = outboxJdbcRepository;
        this.eventQueue = eventQueue;
        this.metrics = metrics;
    }

    @Scheduled(fixedDelayString = "${app.outbox.recovery.interval:10s}")
    @Transactional
    public void recover() {
        if (!properties.getOutbox().getRecovery().isEnabled()) {
            return;
        }
        int batchSize = properties.getOutbox().getRecovery().getBatchSize();
        List<Long> ids = outboxJdbcRepository.findRecoverableIds(batchSize);
        if (ids.isEmpty()) {
            return;
        }

        Instant lockedUntil = Instant.now().plus(properties.getOutbox().getPublisher().getLeaseDuration());
        outboxJdbcRepository.setLease(ids, properties.getInstanceId(), lockedUntil);
        log.debug("Recovery claimed ids={} lockedBy={} lockedUntil={}", ids, properties.getInstanceId(), lockedUntil);

        int enqueued = 0;
        for (Long id : ids) {
            if (eventQueue.enqueue(id)) {
                enqueued++;
            }
        }
        // Release lease immediately; publisher will claim again before sending to Kafka.
        outboxJdbcRepository.clearLease(ids);
        metrics.incrementRecoveryCount(enqueued);
        log.info("Recovery enqueued eventIds count={}", enqueued);
        log.debug("Recovery id list={}", ids);
    }
}
