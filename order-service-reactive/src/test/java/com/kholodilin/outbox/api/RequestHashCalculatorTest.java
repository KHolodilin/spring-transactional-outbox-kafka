package com.kholodilin.outbox.api;

import tools.jackson.databind.json.JsonMapper;
import com.kholodilin.outbox.events.CreateOrderRequest;
import com.kholodilin.outbox.events.OrderItemRequest;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.test.util.ReflectionTestUtils;

import java.math.BigDecimal;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

class RequestHashCalculatorTest {

    private RequestHashCalculator calculator;

    @BeforeEach
    void setUp() {
        calculator = new RequestHashCalculator(JsonMapper.builder().build());
        ReflectionTestUtils.invokeMethod(calculator, "init");
    }

    @Test
    void samePayloadProducesSameHash() {
        CreateOrderRequest a = sample();
        CreateOrderRequest b = sample();
        assertThat(calculator.calculate(a)).isEqualTo(calculator.calculate(b));
    }

    @Test
    void differentPayloadProducesDifferentHash() {
        CreateOrderRequest a = sample();
        CreateOrderRequest b = new CreateOrderRequest(
                43L,
                List.of(new OrderItemRequest("sku-1", 2, new BigDecimal("10.50"))),
                "demo"
        );
        assertThat(calculator.calculate(a)).isNotEqualTo(calculator.calculate(b));
    }

    private static CreateOrderRequest sample() {
        return new CreateOrderRequest(
                42L,
                List.of(new OrderItemRequest("sku-1", 2, new BigDecimal("10.50"))),
                "demo"
        );
    }
}
