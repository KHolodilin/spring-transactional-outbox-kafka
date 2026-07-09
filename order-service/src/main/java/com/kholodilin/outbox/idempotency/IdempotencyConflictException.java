package com.kholodilin.outbox.idempotency;

/** Thrown when the same Idempotency-Key is reused with a different request body (HTTP 409). */
public class IdempotencyConflictException extends RuntimeException {

    public IdempotencyConflictException(String message) {
        super(message);
    }
}
