package com.kholodilin.outbox.persistence;

import com.kholodilin.outbox.events.IdempotencyStatus;

import java.time.Instant;

/** Lightweight idempotency row snapshot for cache / conflict resolution. */
public record IdempotencyKeyRow(
        Long customerId,
        Long id,
        String idempotencyKey,
        String requestHash,
        IdempotencyStatus status,
        String responseBody,
        Instant createdAt,
        Instant updatedAt
) {
}
