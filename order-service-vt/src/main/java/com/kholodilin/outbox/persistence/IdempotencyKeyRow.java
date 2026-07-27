package com.kholodilin.outbox.persistence;

import com.kholodilin.outbox.events.IdempotencyStatus;

import java.time.Instant;

/**
 * Lightweight idempotency row snapshot loaded for cache / conflict resolution.
 */
public record IdempotencyKeyRow(
        /** Customer scope (hash partition key). */
        Long customerId,

        /** Surrogate row id. */
        Long id,

        /** Client {@code Idempotency-Key}. */
        String idempotencyKey,

        /** SHA-256 of the request body. */
        String requestHash,

        /** Current idempotency lifecycle status. */
        IdempotencyStatus status,

        /** Serialized response JSON when {@link IdempotencyStatus#COMPLETED}. */
        String responseBody,

        /** Row creation time. */
        Instant createdAt,

        /** Last update time. */
        Instant updatedAt
) {
}
