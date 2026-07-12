package com.kholodilin.outbox.events;

import com.fasterxml.jackson.annotation.JsonInclude;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.time.Instant;
import java.util.Map;

/**
 * Kafka message body published by {@code order-service} and consumed by {@code notification-stub}.
 */
@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
@JsonInclude(JsonInclude.Include.NON_NULL)
public class EventEnvelope {

    /** Primary key of the corresponding {@code outbox_events} row. */
    private Long eventId;

    /** Identifier of the business order that triggered this event. */
    private Long orderId;

    /** Customer identifier; serialized as the Kafka record key for partition ordering. */
    private Long customerId;

    /** Domain event name (for example {@link EventConstants#EVENT_TYPE_ORDER_CREATED}). */
    private String eventType;

    /** Event-specific JSON payload (structure depends on {@link #eventType}). */
    private Map<String, Object> payload;

    /** Optional trace id copied from the originating HTTP request. */
    private String correlationId;

    /** UTC timestamp when the envelope was assembled for publishing. */
    private Instant occurredAt;
}
