package com.kholodilin.outbox.events;

import org.junit.jupiter.api.Test;

import java.math.BigDecimal;

import static org.assertj.core.api.Assertions.assertThat;

class OrderItemRequestTest {

    @Test
    void builderStoresLineItemFields() {
        OrderItemRequest item = OrderItemRequest.builder()
                .productId("sku-1")
                .quantity(3)
                .price(BigDecimal.valueOf(9.99))
                .build();

        assertThat(item.getProductId()).isEqualTo("sku-1");
        assertThat(item.getQuantity()).isEqualTo(3);
        assertThat(item.getPrice()).isEqualByComparingTo("9.99");
    }
}
