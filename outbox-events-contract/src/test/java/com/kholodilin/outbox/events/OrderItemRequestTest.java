package com.kholodilin.outbox.events;

import org.junit.jupiter.api.Test;

import java.math.BigDecimal;

import static org.assertj.core.api.Assertions.assertThat;

class OrderItemRequestTest {

    @Test
    void builderStoresLineItemFields() {
        OrderItemRequest item = new OrderItemRequest("sku-1", 3, BigDecimal.valueOf(9.99));

        assertThat(item.productId()).isEqualTo("sku-1");
        assertThat(item.quantity()).isEqualTo(3);
        assertThat(item.price()).isEqualByComparingTo("9.99");
    }
}
