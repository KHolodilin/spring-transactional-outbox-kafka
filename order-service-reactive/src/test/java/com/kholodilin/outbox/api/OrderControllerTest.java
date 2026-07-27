package com.kholodilin.outbox.api;

import com.kholodilin.outbox.events.CreateOrderRequest;
import com.kholodilin.outbox.events.CreateOrderResponse;
import com.kholodilin.outbox.events.OrderItemRequest;
import com.kholodilin.outbox.order.OrderCreateOutcome;
import com.kholodilin.outbox.order.OrderTransactionService;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.slf4j.MDC;
import org.springframework.http.HttpStatus;
import reactor.core.publisher.Mono;
import reactor.test.StepVerifier;

import java.math.BigDecimal;
import java.time.Instant;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class OrderControllerTest {

    @Mock
    private OrderTransactionService orderTransactionService;

    @Mock
    private RequestHashCalculator requestHashCalculator;

    private OrderController controller;

    @BeforeEach
    void setUp() {
        controller = new OrderController(orderTransactionService, requestHashCalculator);
    }

    @AfterEach
    void tearDown() {
        MDC.clear();
    }

    @Test
    void returnsCachedResponseWhenIdempotentReplay() {
        CreateOrderRequest request = sampleRequest("corr-1");
        CreateOrderResponse cached = new CreateOrderResponse(1L, 2L, "ACCEPTED", Instant.now());
        when(requestHashCalculator.calculate(request)).thenReturn("hash");
        when(orderTransactionService.createOrder(request, "idem", "hash"))
                .thenReturn(Mono.just(new OrderCreateOutcome(cached, false)));

        StepVerifier.create(controller.createOrder("idem", request))
                .assertNext(response -> {
                    assertThat(response.getStatusCode()).isEqualTo(HttpStatus.OK);
                    assertThat(response.getBody()).isEqualTo(cached);
                })
                .verifyComplete();
    }

    @Test
    void createsOrderWhenClaimSucceeds() {
        CreateOrderRequest request = sampleRequest(null);
        CreateOrderResponse created = new CreateOrderResponse(10L, 20L, "ACCEPTED", Instant.now());
        when(requestHashCalculator.calculate(request)).thenReturn("hash");
        when(orderTransactionService.createOrder(request, "idem", "hash"))
                .thenReturn(Mono.just(new OrderCreateOutcome(created, true)));

        StepVerifier.create(controller.createOrder("idem", request))
                .assertNext(response -> {
                    assertThat(response.getStatusCode()).isEqualTo(HttpStatus.CREATED);
                    assertThat(response.getBody()).isEqualTo(created);
                })
                .verifyComplete();

        verify(orderTransactionService).createOrder(request, "idem", "hash");
    }

    private static CreateOrderRequest sampleRequest(String correlationId) {
        return new CreateOrderRequest(
                42L,
                List.of(new OrderItemRequest("sku", 1, BigDecimal.ONE)),
                correlationId
        );
    }
}
