package com.kholodilin.outbox.events;

import org.junit.jupiter.api.Test;

import java.math.BigDecimal;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

class CreateOrderRequestTest {

    @Test
    void builderStoresCustomerAndItems() {
        CreateOrderRequest request = new CreateOrderRequest(
                42L,
                List.of(new OrderItemRequest("sku", 2, BigDecimal.TEN)),
                "corr"
        );

        assertThat(request.customerId()).isEqualTo(42L);
        assertThat(request.correlationId()).isEqualTo("corr");
        assertThat(request.items()).hasSize(1);
        assertThat(request.items().get(0).price()).isEqualByComparingTo(BigDecimal.TEN);
    }
}
