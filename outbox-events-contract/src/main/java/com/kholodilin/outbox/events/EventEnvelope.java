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

    private Long eventId;
    private Long orderId;
    private Long customerId;
    private String eventType;
    private Map<String, Object> payload;
    private String correlationId;
    private Instant occurredAt;
}
