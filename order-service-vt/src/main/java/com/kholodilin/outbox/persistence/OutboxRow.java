package com.kholodilin.outbox.persistence;

import com.kholodilin.outbox.events.OutboxStatus;

/**
 * Lightweight outbox row snapshot loaded for publishing.
 * <p>
 * Payload is kept as a JSON string until the publisher builds an {@link com.kholodilin.outbox.events.EventEnvelope}.
 */
public record OutboxRow(
        /** Primary key of the {@code outbox_events} row. */
        Long id,

        /** Foreign business key to the {@code orders} table. */
        Long orderId,

        /** Customer identifier; also used as the Kafka partition key. */
        Long customerId,

        /** Domain event name (for example {@code OrderCreated}). */
        String eventType,

        /** Raw JSON payload as stored in PostgreSQL {@code jsonb}. */
        String payload,

        /** Current lifecycle status after claim or recovery. */
        OutboxStatus status,

        /** Number of failed publish attempts recorded for this row. */
        int retryCount,

        /** W3C traceparent captured when the outbox row was created. */
        String traceParent
) {
}
