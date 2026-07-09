package com.kholodilin.outbox.events;

/**
 * Lifecycle of a row in {@code outbox_events} while it is being published.
 * <p>
 * Persisted as {@code INT} in PostgreSQL ({@code status} column).
 * Values {@code < 100} are active (hot-path); {@code >= 100} are archived finals.
 */
public enum OutboxStatus {
    NEW(0),
    PROCESSING(1),
    FAILED(2),
    /** Max retries exceeded; manual intervention required. */
    DEAD(101),
    /** Published successfully; row moves to the archive partition. */
    SENT(110);

    public static final int ARCHIVE_THRESHOLD = 100;

    private final int code;

    OutboxStatus(int code) {
        this.code = code;
    }

    public int getCode() {
        return code;
    }

    public boolean isActive() {
        return code < ARCHIVE_THRESHOLD;
    }

    public boolean isArchive() {
        return code >= ARCHIVE_THRESHOLD;
    }

    public static OutboxStatus fromCode(int code) {
        for (OutboxStatus status : values()) {
            if (status.code == code) {
                return status;
            }
        }
        throw new IllegalArgumentException("Unknown OutboxStatus code: " + code);
    }
}
