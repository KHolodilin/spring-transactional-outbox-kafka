package com.kholodilin.outbox.events;

/** Idempotency record state stored in {@code idempotency_keys}. */
public enum IdempotencyStatus {
    PROCESSING,
    COMPLETED,
    FAILED
}
