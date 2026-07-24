package com.kholodilin.outbox.outbox;

import tools.jackson.databind.ObjectMapper;
import tools.jackson.databind.json.JsonMapper;
import com.kholodilin.outbox.events.CreateOrderRequest;
import com.kholodilin.outbox.events.EventConstants;
import com.kholodilin.outbox.events.OrderItemRequest;
import org.junit.jupiter.api.Test;

import java.math.BigDecimal;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

class OutboxEventFactoryTest {

    private final OutboxEventFactory factory = new OutboxEventFactory(JsonMapper.builder().build());

    @Test
    void buildsOrderCreatedPayload() throws Exception {
        CreateOrderRequest request = new CreateOrderRequest(
                10L,
                List.of(new OrderItemRequest("sku-1", 1, BigDecimal.ONE)),
                "corr"
        );
        String json = factory.buildOrderCreatedPayload(99L, request);
        @SuppressWarnings("unchecked")
        Map<String, Object> payload = JsonMapper.builder().build().readValue(json, Map.class);
        assertThat(payload.get("orderId")).isEqualTo(99);
        assertThat(payload.get("customerId")).isEqualTo(10);
        assertThat(payload.get("correlationId")).isEqualTo("corr");
        assertThat(factory.eventType()).isEqualTo(EventConstants.EVENT_TYPE_ORDER_CREATED);
    }

    @Test
    void omitsCorrelationIdWhenNull() throws Exception {
        CreateOrderRequest request = new CreateOrderRequest(
                10L,
                List.of(new OrderItemRequest("sku-1", 1, BigDecimal.ONE)),
                null
        );
        String json = factory.buildOrderCreatedPayload(99L, request);
        @SuppressWarnings("unchecked")
        Map<String, Object> payload = JsonMapper.builder().build().readValue(json, Map.class);
        assertThat(payload).doesNotContainKey("correlationId");
    }

    @Test
    void wrapsSerializationFailures() throws Exception {
        ObjectMapper failingMapper = mock(ObjectMapper.class);
        when(failingMapper.writeValueAsString(any())).thenThrow(new RuntimeException("boom"));
        OutboxEventFactory failingFactory = new OutboxEventFactory(failingMapper);
        CreateOrderRequest request = new CreateOrderRequest(
                10L,
                List.of(new OrderItemRequest("sku-1", 1, BigDecimal.ONE)),
                null
        );

        assertThatThrownBy(() -> failingFactory.buildOrderCreatedPayload(1L, request))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("Failed to build outbox payload");
    }
}
