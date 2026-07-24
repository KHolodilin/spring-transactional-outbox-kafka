package com.kholodilin.outbox.idempotency;

/**
 * Thrown when the same {@code Idempotency-Key} cannot be safely reused (HTTP 409).
 * <p>
 * Typical causes: different request body hash, or the original request still {@code PROCESSING}.
 * Mapped by {@link com.kholodilin.outbox.api.GlobalExceptionHandler}.
 */
public class IdempotencyConflictException extends RuntimeException {

    /**
     * @param message human-readable conflict reason returned in the Problem Detail body
     */
    public IdempotencyConflictException(String message) {
        super(message);
    }
}
