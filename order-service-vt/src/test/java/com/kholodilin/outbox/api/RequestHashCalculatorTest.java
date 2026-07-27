package com.kholodilin.outbox.api;

import com.kholodilin.outbox.events.CreateOrderRequest;
import com.kholodilin.outbox.events.OrderItemRequest;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.test.util.ReflectionTestUtils;
import tools.jackson.databind.json.JsonMapper;

import java.math.BigDecimal;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

class RequestHashCalculatorTest {

    private final RequestHashCalculator calculator;

    RequestHashCalculatorTest() {
        calculator = new RequestHashCalculator(JsonMapper.builder().build());
        ReflectionTestUtils.invokeMethod(calculator, "init");
    }

    @Test
    void sameRequestProducesSameHash() {
        CreateOrderRequest request = sampleRequest();
        assertThat(calculator.calculate(request)).isEqualTo(calculator.calculate(request));
    }

    @Test
    void differentRequestProducesDifferentHash() {
        CreateOrderRequest first = sampleRequest();
        CreateOrderRequest second = new CreateOrderRequest(
                2L,
                List.of(new OrderItemRequest("p1", 1, BigDecimal.TEN)),
                null
        );
        assertThat(calculator.calculate(first)).isNotEqualTo(calculator.calculate(second));
    }

    private CreateOrderRequest sampleRequest() {
        return new CreateOrderRequest(
                1L,
                List.of(new OrderItemRequest("p1", 2, BigDecimal.valueOf(10))),
                "corr-1"
        );
    }
}
