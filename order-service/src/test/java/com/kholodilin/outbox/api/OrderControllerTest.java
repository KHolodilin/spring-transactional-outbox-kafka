package com.kholodilin.outbox.api;

import com.kholodilin.outbox.events.CreateOrderRequest;
import com.kholodilin.outbox.events.CreateOrderResponse;
import com.kholodilin.outbox.events.OrderItemRequest;
import com.kholodilin.outbox.idempotency.IdempotencyService;
import com.kholodilin.outbox.order.OrderTransactionService;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.slf4j.MDC;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;

import java.math.BigDecimal;
import java.time.Instant;
import java.util.List;
import java.util.Optional;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class OrderControllerTest {

    @Mock
    private IdempotencyService idempotencyService;

    @Mock
    private OrderTransactionService orderTransactionService;

    @Mock
    private RequestHashCalculator requestHashCalculator;

    private OrderController controller;

    @BeforeEach
    void setUp() {
        controller = new OrderController(idempotencyService, orderTransactionService, requestHashCalculator);
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
        when(idempotencyService.findCachedResponse(42L, "idem", "hash")).thenReturn(Optional.of(cached));

        ResponseEntity<CreateOrderResponse> response = controller.createOrder("idem", request);

        assertThat(response.getStatusCode()).isEqualTo(HttpStatus.OK);
        assertThat(response.getBody()).isEqualTo(cached);
    }

    @Test
    void createsOrderWhenNoCachedResponse() {
        CreateOrderRequest request = sampleRequest(null);
        CreateOrderResponse created = new CreateOrderResponse(10L, 20L, "ACCEPTED", Instant.now());
        when(requestHashCalculator.calculate(request)).thenReturn("hash");
        when(idempotencyService.findCachedResponse(42L, "idem", "hash")).thenReturn(Optional.empty());
        when(orderTransactionService.createOrder(request, "idem", "hash")).thenReturn(created);

        ResponseEntity<CreateOrderResponse> response = controller.createOrder("idem", request);

        assertThat(response.getStatusCode()).isEqualTo(HttpStatus.CREATED);
        assertThat(response.getBody()).isEqualTo(created);
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
