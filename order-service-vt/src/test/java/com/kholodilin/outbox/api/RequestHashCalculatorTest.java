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

    @Test
    void wrapsSerializationFailures() throws Exception {
        tools.jackson.databind.ObjectMapper canonical = org.mockito.Mockito.mock(tools.jackson.databind.ObjectMapper.class);
        org.mockito.Mockito.when(canonical.writeValueAsString(org.mockito.ArgumentMatchers.any()))
                .thenThrow(new RuntimeException("boom"));

        RequestHashCalculator failingCalculator = new RequestHashCalculator(JsonMapper.builder().build());
        ReflectionTestUtils.invokeMethod(failingCalculator, "init");
        ReflectionTestUtils.setField(failingCalculator, "canonicalMapper", canonical);

        org.assertj.core.api.Assertions.assertThatThrownBy(() -> failingCalculator.calculate(sampleRequest()))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("Failed to calculate request hash");
    }

    private CreateOrderRequest sampleRequest() {
        return new CreateOrderRequest(
                1L,
                List.of(new OrderItemRequest("p1", 2, BigDecimal.valueOf(10))),
                "corr-1"
        );
    }
}
