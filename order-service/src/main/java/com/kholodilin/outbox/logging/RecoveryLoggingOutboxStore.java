package com.kholodilin.outbox.logging;

import com.kholodilin.outbox.events.ObservabilityVocabulary;
import com.kholodilin.outbox.model.OutboxInsert;
import com.kholodilin.outbox.model.OutboxRecord;
import com.kholodilin.outbox.model.OutboxStatus;
import com.kholodilin.outbox.spi.OutboxStore;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

import java.time.Instant;
import java.util.List;

/** Delegates to the starter store and emits recovery {@code event.action} matching the reactive worker. */
@Slf4j
@RequiredArgsConstructor
public class RecoveryLoggingOutboxStore implements OutboxStore {

    private final OutboxStore delegate;
    private final String instanceId;

    @Override
    public long insert(OutboxInsert insert) {
        return delegate.insert(insert);
    }

    @Override
    public List<OutboxRecord> claimByIds(List<Long> eventIds, String lockedBy, Instant lockedUntil) {
        return delegate.claimByIds(eventIds, lockedBy, lockedUntil);
    }

    @Override
    public void markSent(List<Long> eventIds, Instant sentAt) {
        delegate.markSent(eventIds, sentAt);
    }

    @Override
    public void markFailed(long eventId, int retryCount, OutboxStatus status) {
        delegate.markFailed(eventId, retryCount, status);
    }

    @Override
    public List<Long> claimRecoverableIds(int batchSize, String lockedBy, Instant lockedUntil) {
        return delegate.claimRecoverableIds(batchSize, lockedBy, lockedUntil);
    }

    @Override
    public void clearLease(List<Long> eventIds) {
        delegate.clearLease(eventIds);
        if (eventIds == null || eventIds.isEmpty()) {
            return;
        }
        StructuredLogContext.putInstanceFields(instanceId);
        StructuredLogContext.enrichTracingAliases();
        StructuredLogContext.putBatchSize(eventIds.size());
        StructuredLogContext.putEventAction(ObservabilityVocabulary.OUTBOX_RECOVERY_COMPLETED);
        log.info("Recovery enqueued eventIds count={}", eventIds.size());
    }

    @Override
    public List<Long> findReenqueueableIds(List<Long> eventIds) {
        return delegate.findReenqueueableIds(eventIds);
    }

    @Override
    public List<OutboxRecord> findByIds(List<Long> eventIds) {
        return delegate.findByIds(eventIds);
    }
}
