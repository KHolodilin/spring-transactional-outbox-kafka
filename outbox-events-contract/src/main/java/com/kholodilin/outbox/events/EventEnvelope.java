package com.kholodilin.outbox.events;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonInclude;

import java.time.Instant;
import java.util.Map;

/**
 * Kafka message body published by {@code order-service} and consumed by {@code notification-stub}.
 */
@JsonInclude(JsonInclude.Include.NON_NULL)
public record EventEnvelope(
        /** Primary key of the corresponding {@code outbox_events} row. */
        Long eventId,

        /** Identifier of the business order that triggered this event. */
        Long orderId,

        /** Customer identifier; serialized as the Kafka record key for partition ordering. */
        Long customerId,

        /** Domain event name (for example {@link EventConstants#EVENT_TYPE_ORDER_CREATED}). */
        String eventType,

        /** Event-specific JSON payload (structure depends on {@link #eventType}). */
        Map<String, Object> payload,

        /** Optional trace id copied from the originating HTTP request. */
        String correlationId,

        /** UTC timestamp when the envelope was assembled for publishing. */
        Instant occurredAt,

        /** W3C traceparent restored from outbox row; not serialized in the Kafka JSON body. */
        @JsonIgnore
        String traceParent,

        /** Optional W3C tracestate; not serialized in the Kafka JSON body. */
        @JsonIgnore
        String traceState
) {
}
