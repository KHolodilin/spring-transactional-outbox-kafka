package com.kholodilin.outbox.outbox;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.kholodilin.outbox.events.CreateOrderRequest;
import com.kholodilin.outbox.events.EventConstants;
import com.kholodilin.outbox.events.OrderItemRequest;
import org.junit.jupiter.api.Test;

import java.math.BigDecimal;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

class OutboxEventFactoryTest {

    private final OutboxEventFactory factory = new OutboxEventFactory(new ObjectMapper().findAndRegisterModules());

    @Test
    void buildsOrderCreatedPayload() throws Exception {
        CreateOrderRequest request = CreateOrderRequest.builder()
                .customerId(10L)
                .items(List.of(OrderItemRequest.builder().productId("sku-1").quantity(1).price(BigDecimal.ONE).build()))
                .correlationId("corr")
                .build();
        String json = factory.buildOrderCreatedPayload(99L, request);
        @SuppressWarnings("unchecked")
        Map<String, Object> payload = new ObjectMapper().readValue(json, Map.class);
        assertThat(payload.get("orderId")).isEqualTo(99);
        assertThat(payload.get("customerId")).isEqualTo(10);
        assertThat(factory.eventType()).isEqualTo(EventConstants.EVENT_TYPE_ORDER_CREATED);
    }
}
