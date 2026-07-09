package com.kholodilin.outbox.outbox;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.kholodilin.outbox.events.CreateOrderRequest;
import com.kholodilin.outbox.events.EventConstants;
import org.springframework.stereotype.Component;

import java.util.HashMap;
import java.util.Map;

/**
 * Builds the JSON payload stored in {@code outbox_events} and later sent as {@link com.kholodilin.outbox.events.EventEnvelope}.
 */
@Component
public class OutboxEventFactory {

    private final ObjectMapper objectMapper;

    public OutboxEventFactory(ObjectMapper objectMapper) {
        this.objectMapper = objectMapper;
    }

    public String buildOrderCreatedPayload(long orderId, CreateOrderRequest request) {
        try {
            Map<String, Object> payload = new HashMap<>();
            payload.put("orderId", orderId);
            payload.put("customerId", request.getCustomerId());
            payload.put("items", request.getItems());
            if (request.getCorrelationId() != null) {
                payload.put("correlationId", request.getCorrelationId());
            }
            return objectMapper.writeValueAsString(payload);
        } catch (Exception ex) {
            throw new IllegalStateException("Failed to build outbox payload", ex);
        }
    }

    public String eventType() {
        return EventConstants.EVENT_TYPE_ORDER_CREATED;
    }
}
