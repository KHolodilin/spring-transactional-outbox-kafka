package com.kholodilin.outbox.events;

/**
 * Lifecycle of a row in {@code outbox_events} while it is being published.
 * <p>
 * Persisted as {@code INT} in PostgreSQL ({@code status} column).
 * Values {@code < 100} are active (hot-path); {@code >= 100} are archived finals.
 */
public enum OutboxStatus {

    /** Inserted with the business transaction; waiting for the publisher. */
    NEW(0),

    /** Claimed by a publisher worker under a lease; publish in progress. */
    PROCESSING(1),

    /** Publish failed but may be retried or recovered. */
    FAILED(2),

    /** Max retries exceeded; manual intervention required. */
    DEAD(101),

    /** Published successfully; row moves to the archive partition. */
    SENT(110);

    /** Status codes at or above this value belong to the archive partition. */
    public static final int ARCHIVE_THRESHOLD = 100;

    private final int code;

    OutboxStatus(int code) {
        this.code = code;
    }

    /** Returns the integer value stored in {@code outbox_events.status}. */
    public int getCode() {
        return code;
    }

    /** {@code true} when the row is in the active partition ({@code status < 100}). */
    public boolean isActive() {
        return code < ARCHIVE_THRESHOLD;
    }

    /** {@code true} when the row is in the archive partition ({@code status >= 100}). */
    public boolean isArchive() {
        return code >= ARCHIVE_THRESHOLD;
    }

    /**
     * Resolves an enum constant from its persisted integer code.
     *
     * @param code value read from {@code outbox_events.status}
     * @return matching status
     * @throws IllegalArgumentException when {@code code} is not recognized
     */
    public static OutboxStatus fromCode(int code) {
        for (OutboxStatus status : values()) {
            if (status.code == code) {
                return status;
            }
        }
        throw new IllegalArgumentException("Unknown OutboxStatus code: " + code);
    }
}
