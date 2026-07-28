package com.kholodilin.outbox.outbox;

import com.kholodilin.outbox.logging.StructuredLogContext;
import com.kholodilin.outbox.queue.InMemoryEventQueue;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;
import org.springframework.transaction.support.TransactionSynchronization;
import org.springframework.transaction.support.TransactionSynchronizationManager;

/**
 * Registers a post-commit hook that enqueues the outbox event id into the in-memory queue.
 * <p>
 * Enqueue must happen only after commit; otherwise a crash could publish an event
 * for a rolled-back order.
 */
@Slf4j
@Component
@RequiredArgsConstructor
public class OutboxEnqueueListener {

    private final InMemoryEventQueue eventQueue;

    /**
     * Schedules enqueue of {@code eventId} for after the current transaction commits.
     * <p>
     * If no synchronization is active (tests / non-transactional callers), enqueues immediately.
     * A full queue or duplicate id logs a warning; recovery will pick the row up later.
     *
     * @param eventId primary key of the persisted {@code outbox_events} row
     */
    public void enqueueAfterCommit(long eventId) {
        if (!TransactionSynchronizationManager.isSynchronizationActive()) {
            eventQueue.enqueue(eventId);
            return;
        }
        TransactionSynchronizationManager.registerSynchronization(new TransactionSynchronization() {
            @Override
            public void afterCommit() {
                boolean enqueued = eventQueue.enqueue(eventId);
                StructuredLogContext.putOrderFields(null, eventId);
                if (enqueued) {
                    StructuredLogContext.putEventAction("outbox.event.persisted");
                    log.info("Outbox event enqueued after commit eventId={}", eventId);
                } else {
                    log.warn("Outbox event not enqueued after commit eventId={} (queue full or duplicate)", eventId);
                }
                log.debug("afterCommit enqueue eventId={} enqueued={}", eventId, enqueued);
            }
        });
    }
}
