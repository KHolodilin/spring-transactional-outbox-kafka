package com.kholodilin.outbox.persistence;

import com.kholodilin.outbox.events.OutboxStatus;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Getter;

/**
 * Lightweight outbox row snapshot loaded for publishing.
 * <p>
 * Payload is kept as a JSON string until the publisher builds an {@link com.kholodilin.outbox.events.EventEnvelope}.
 */
@Getter
@Builder
@AllArgsConstructor
public class OutboxRow {

    /** Primary key of the {@code outbox_events} row. */
    private final Long id;

    /** Foreign business key to the {@code orders} table. */
    private final Long orderId;

    /** Customer identifier; also used as the Kafka partition key. */
    private final Long customerId;

    /** Domain event name (for example {@code OrderCreated}). */
    private final String eventType;

    /** Raw JSON payload as stored in PostgreSQL {@code jsonb}. */
    private final String payload;

    /** Current lifecycle status after claim or recovery. */
    private final OutboxStatus status;

    /** Number of failed publish attempts recorded for this row. */
    private final int retryCount;
}
