package com.kholodilin.outbox.events;

/**
 * Idempotency record state stored in {@code idempotency_keys}.
 * <p>
 * Persisted as {@code INT} in PostgreSQL ({@code status} column).
 */
public enum IdempotencyStatus {

    /** Request accepted; order transaction is in progress. */
    PROCESSING(0),

    /** Order persisted and response body is available for replay. */
    COMPLETED(1),

    /** Processing failed; the idempotency slot may be retried or cleaned up. */
    FAILED(2);

    private final int code;

    IdempotencyStatus(int code) {
        this.code = code;
    }

    /** Returns the integer value stored in {@code idempotency_keys.status}. */
    public int getCode() {
        return code;
    }

    /**
     * Resolves an enum constant from its persisted integer code.
     *
     * @param code value read from {@code idempotency_keys.status}
     * @return matching status
     * @throws IllegalArgumentException when {@code code} is not recognized
     */
    public static IdempotencyStatus fromCode(int code) {
        for (IdempotencyStatus status : values()) {
            if (status.code == code) {
                return status;
            }
        }
        throw new IllegalArgumentException("Unknown IdempotencyStatus code: " + code);
    }
}
