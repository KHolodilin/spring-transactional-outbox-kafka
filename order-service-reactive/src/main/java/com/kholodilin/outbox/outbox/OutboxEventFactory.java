package com.kholodilin.outbox.outbox;

import tools.jackson.databind.ObjectMapper;
import com.kholodilin.outbox.events.CreateOrderRequest;
import com.kholodilin.outbox.events.EventConstants;
import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Component;

import java.util.HashMap;
import java.util.Map;

/** Builds the JSON payload stored in {@code outbox_events}. */
@Component
@RequiredArgsConstructor
public class OutboxEventFactory {

    private final ObjectMapper objectMapper;

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

    public String eventType() {
        return EventConstants.EVENT_TYPE_ORDER_CREATED;
    }
}
