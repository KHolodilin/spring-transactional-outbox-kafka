package com.kholodilin.outbox.events;

/**
 * Idempotency record state stored in {@code idempotency_keys}.
 * <p>
 * Persisted as {@code INT} in PostgreSQL ({@code status} column).
 */
public enum IdempotencyStatus {
    PROCESSING(0),
    COMPLETED(1),
    FAILED(2);

    private final int code;

    IdempotencyStatus(int code) {
        this.code = code;
    }

    public int getCode() {
        return code;
    }

    public static IdempotencyStatus fromCode(int code) {
        for (IdempotencyStatus status : values()) {
            if (status.code == code) {
                return status;
            }
        }
        throw new IllegalArgumentException("Unknown IdempotencyStatus code: " + code);
    }
}
