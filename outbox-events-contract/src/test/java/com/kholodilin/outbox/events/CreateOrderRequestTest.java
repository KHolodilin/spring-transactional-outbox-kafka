package com.kholodilin.outbox.events;

import org.junit.jupiter.api.Test;

import java.math.BigDecimal;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

class CreateOrderRequestTest {

    @Test
    void builderStoresCustomerAndItems() {
        CreateOrderRequest request = CreateOrderRequest.builder()
                .customerId(42L)
                .correlationId("corr")
                .items(List.of(OrderItemRequest.builder()
                        .productId("sku")
                        .quantity(2)
                        .price(BigDecimal.TEN)
                        .build()))
                .build();

        assertThat(request.getCustomerId()).isEqualTo(42L);
        assertThat(request.getCorrelationId()).isEqualTo("corr");
        assertThat(request.getItems()).hasSize(1);
        assertThat(request.getItems().get(0).getPrice()).isEqualByComparingTo(BigDecimal.TEN);
    }
}
