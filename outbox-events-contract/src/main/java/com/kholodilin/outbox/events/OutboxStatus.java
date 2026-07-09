package com.kholodilin.outbox.events;

/** Lifecycle of a row in {@code outbox_events} while it is being published. */
public enum OutboxStatus {
    NEW,
    PROCESSING,
    FAILED,
    /** Max retries exceeded; manual intervention required. */
    DEAD,
    /** Published successfully; row is moved to ARCHIVE partition state. */
    SENT
}
