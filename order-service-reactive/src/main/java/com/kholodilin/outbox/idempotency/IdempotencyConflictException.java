package com.kholodilin.outbox.idempotency;

/** Thrown when the same {@code Idempotency-Key} cannot be safely reused (HTTP 409). */
public class IdempotencyConflictException extends RuntimeException {

    public IdempotencyConflictException(String message) {
        super(message);
    }
}
