package com.kholodilin.outbox.order;

import tools.jackson.databind.json.JsonMapper;
import com.kholodilin.outbox.events.CreateOrderRequest;
import com.kholodilin.outbox.events.CreateOrderResponse;
import com.kholodilin.outbox.events.OrderItemRequest;
import com.kholodilin.outbox.outbox.OutboxEnqueueListener;
import com.kholodilin.outbox.outbox.OutboxEventFactory;
import com.kholodilin.outbox.persistence.IdempotencyJdbcRepository;
import com.kholodilin.outbox.persistence.OrderJdbcRepository;
import com.kholodilin.outbox.persistence.OutboxJdbcRepository;
import com.kholodilin.outbox.tracing.TraceContextSupport;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.math.BigDecimal;
import java.time.Instant;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class OrderTransactionServiceTest {

    @Mock
    private OrderJdbcRepository orderJdbcRepository;

    @Mock
    private OutboxJdbcRepository outboxJdbcRepository;

    @Mock
    private IdempotencyJdbcRepository idempotencyJdbcRepository;

    @Mock
    private OutboxEventFactory outboxEventFactory;

    @Mock
    private OutboxEnqueueListener outboxEnqueueListener;

    @Mock
    private TraceContextSupport traceContextSupport;

    private OrderTransactionService service;

    @BeforeEach
    void setUp() {
        service = new OrderTransactionService(
                orderJdbcRepository,
                outboxJdbcRepository,
                idempotencyJdbcRepository,
                outboxEventFactory,
                outboxEnqueueListener,
                JsonMapper.builder().build(),
                traceContextSupport
        );
    }

    @Test
    void createOrderPersistsDomainDataAndEnqueuesAfterCommit() {
        CreateOrderRequest request = CreateOrderRequest.builder()
                .customerId(42L)
                .correlationId("corr-1")
                .items(List.of(OrderItemRequest.builder()
                        .productId("sku-1")
                        .quantity(2)
                        .price(BigDecimal.valueOf(5))
                        .build()))
                .build();

        when(orderJdbcRepository.insertOrder(eq(42L), eq(BigDecimal.valueOf(10)), any(Instant.class))).thenReturn(100L);
        when(outboxEventFactory.buildOrderCreatedPayload(100L, request)).thenReturn("{\"orderId\":100}");
        when(outboxEventFactory.eventType()).thenReturn("OrderCreated");
        when(traceContextSupport.captureTraceParent()).thenReturn("00-trace");
        when(outboxJdbcRepository.insertEvent(eq(100L), eq(42L), eq("OrderCreated"), eq("{\"orderId\":100}"), eq("00-trace"), any(Instant.class)))
                .thenReturn(200L);

        CreateOrderResponse response = service.createOrder(request, "idem-key", "hash-1");

        assertThat(response.getOrderId()).isEqualTo(100L);
        assertThat(response.getEventId()).isEqualTo(200L);
        assertThat(response.getStatus()).isEqualTo("ACCEPTED");
        verify(idempotencyJdbcRepository).insertProcessing(eq(42L), eq("idem-key"), eq("hash-1"), any(Instant.class));
        verify(orderJdbcRepository).insertOrderItem(eq(100L), eq(42L), eq("sku-1"), eq(2), eq(BigDecimal.valueOf(5)), any(Instant.class));
        verify(idempotencyJdbcRepository).complete(eq(42L), eq("idem-key"), any(String.class), any(Instant.class));
        verify(outboxEnqueueListener).enqueueAfterCommit(200L);
    }
}
