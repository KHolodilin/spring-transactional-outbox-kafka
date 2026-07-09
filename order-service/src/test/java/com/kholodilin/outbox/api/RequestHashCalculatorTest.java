package com.kholodilin.outbox.api;

import com.kholodilin.outbox.events.CreateOrderRequest;
import com.kholodilin.outbox.events.OrderItemRequest;
import org.junit.jupiter.api.Test;

import java.math.BigDecimal;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

class RequestHashCalculatorTest {

    private final RequestHashCalculator calculator = new RequestHashCalculator(new com.fasterxml.jackson.databind.ObjectMapper().findAndRegisterModules());

    @Test
    void sameRequestProducesSameHash() {
        CreateOrderRequest request = sampleRequest();
        assertThat(calculator.calculate(request)).isEqualTo(calculator.calculate(request));
    }

    @Test
    void differentRequestProducesDifferentHash() {
        CreateOrderRequest first = sampleRequest();
        CreateOrderRequest second = CreateOrderRequest.builder()
                .customerId(2L)
                .items(List.of(OrderItemRequest.builder().productId("p1").quantity(1).price(BigDecimal.TEN).build()))
                .build();
        assertThat(calculator.calculate(first)).isNotEqualTo(calculator.calculate(second));
    }

    private CreateOrderRequest sampleRequest() {
        return CreateOrderRequest.builder()
                .customerId(1L)
                .items(List.of(OrderItemRequest.builder().productId("p1").quantity(2).price(BigDecimal.valueOf(10)).build()))
                .correlationId("corr-1")
                .build();
    }
}
