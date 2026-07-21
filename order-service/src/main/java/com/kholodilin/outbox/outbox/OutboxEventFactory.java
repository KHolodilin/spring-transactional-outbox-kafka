package com.kholodilin.outbox.outbox;

import tools.jackson.databind.ObjectMapper;
import com.kholodilin.outbox.events.CreateOrderRequest;
import com.kholodilin.outbox.events.EventConstants;
import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Component;

import java.util.HashMap;
import java.util.Map;

/**
 * Builds the JSON payload stored in {@code outbox_events} and later sent as {@link com.kholodilin.outbox.events.EventEnvelope}.
 */
@Component
@RequiredArgsConstructor
public class OutboxEventFactory {

    private final ObjectMapper objectMapper;

    /**
     * Serializes the order-created payload JSON stored in {@code outbox_events.payload}.
     *
     * @param orderId generated order id
     * @param request original create request (items, customer, optional correlation id)
     * @return JSON string suitable for jsonb storage and later Kafka envelope mapping
     */
    public String buildOrderCreatedPayload(long orderId, CreateOrderRequest request) {
        try {
            Map<String, Object> payload = new HashMap<>();
            payload.put("orderId", orderId);
            payload.put("customerId", request.customerId());
            payload.put("items", request.items());
            if (request.correlationId() != null) {
                payload.put("correlationId", request.correlationId());
            }
            return objectMapper.writeValueAsString(payload);
        } catch (Exception ex) {
            throw new IllegalStateException("Failed to build outbox payload", ex);
        }
    }

    /**
     * @return canonical event type written to {@code outbox_events.event_type}
     *         ({@link EventConstants#EVENT_TYPE_ORDER_CREATED})
     */
    public String eventType() {
        return EventConstants.EVENT_TYPE_ORDER_CREATED;
    }
}
