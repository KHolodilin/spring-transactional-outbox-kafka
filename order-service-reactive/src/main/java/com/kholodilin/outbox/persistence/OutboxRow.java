package com.kholodilin.outbox.persistence;

import com.kholodilin.outbox.events.OutboxStatus;

/** Lightweight outbox row snapshot loaded for publishing. */
public record OutboxRow(
        Long id,
        Long orderId,
        Long customerId,
        String eventType,
        String payload,
        OutboxStatus status,
        int retryCount,
        String traceParent
) {
}
