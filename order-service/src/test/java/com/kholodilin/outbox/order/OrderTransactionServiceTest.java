package com.kholodilin.outbox.order;

import com.kholodilin.idempotency.ExecutionResult;
import com.kholodilin.idempotency.IdempotencyService;
import com.kholodilin.idempotency.exception.IdempotencyConflictException;
import com.kholodilin.idempotency.model.IdempotencyKey;
import com.kholodilin.outbox.events.CreateOrderRequest;
import com.kholodilin.outbox.events.CreateOrderResponse;
import com.kholodilin.outbox.events.OrderItemRequest;
import com.kholodilin.outbox.metrics.OutboxMetrics;
import com.kholodilin.outbox.outbox.OutboxEnqueueListener;
import com.kholodilin.outbox.outbox.OutboxEventFactory;
import com.kholodilin.outbox.persistence.OrderJdbcRepository;
import com.kholodilin.outbox.persistence.OutboxJdbcRepository;
import com.kholodilin.outbox.tracing.TraceContextSupport;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.test.util.ReflectionTestUtils;

import java.math.BigDecimal;
import java.time.Instant;
import java.util.List;
import java.util.function.Supplier;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class OrderTransactionServiceTest {

    @Mock
    private OrderJdbcRepository orderJdbcRepository;

    @Mock
    private OutboxJdbcRepository outboxJdbcRepository;

    @Mock
    private IdempotencyService idempotencyService;

    @Mock
    private OutboxEventFactory outboxEventFactory;

    @Mock
    private OutboxEnqueueListener outboxEnqueueListener;

    @Mock
    private TraceContextSupport traceContextSupport;

    private OrderTransactionService service;

    @BeforeEach
    void setUp() {
        OutboxMetrics metrics = new OutboxMetrics(new SimpleMeterRegistry());
        ReflectionTestUtils.invokeMethod(metrics, "registerMeters");
        service = new OrderTransactionService(
                orderJdbcRepository,
                outboxJdbcRepository,
                idempotencyService,
                outboxEventFactory,
                outboxEnqueueListener,
                traceContextSupport,
                metrics
        );
    }

    @Test
    void createOrderPersistsDomainDataAndEnqueuesAfterCommit() {
        CreateOrderRequest request = new CreateOrderRequest(
                42L,
                List.of(new OrderItemRequest("sku-1", 2, BigDecimal.valueOf(5))),
                "corr-1"
        );

        when(orderJdbcRepository.insertOrder(eq(42L), eq(BigDecimal.valueOf(10)), any(Instant.class))).thenReturn(100L);
        when(outboxEventFactory.buildOrderCreatedPayload(100L, request)).thenReturn("{\"orderId\":100}");
        when(outboxEventFactory.eventType()).thenReturn("OrderCreated");
        when(traceContextSupport.captureTraceParent()).thenReturn("00-trace");
        when(outboxJdbcRepository.insertEvent(eq(100L), eq(42L), eq("OrderCreated"), eq("{\"orderId\":100}"), eq("00-trace"), any(Instant.class)))
                .thenReturn(200L);
        when(idempotencyService.execute(
                eq("CREATE_ORDER:42"),
                eq("idem-key"),
                eq(request),
                eq(CreateOrderResponse.class),
                any()
        )).thenAnswer(invocation -> {
            Supplier<ExecutionResult<CreateOrderResponse>> action = invocation.getArgument(4);
            return action.get();
        });

        OrderCreateOutcome outcome = service.createOrder(request, "idem-key");

        assertThat(outcome.created()).isTrue();
        assertThat(outcome.response().orderId()).isEqualTo(100L);
        assertThat(outcome.response().eventId()).isEqualTo(200L);
        assertThat(outcome.response().status()).isEqualTo("ACCEPTED");
        verify(orderJdbcRepository).insertOrderItem(eq(100L), eq(42L), eq("sku-1"), eq(2), eq(BigDecimal.valueOf(5)), any(Instant.class));
        verify(outboxEnqueueListener).enqueueAfterCommit(200L);
    }

    @Test
    void createOrderReturnsCachedResponseWhenReplay() {
        CreateOrderRequest request = new CreateOrderRequest(
                42L,
                List.of(new OrderItemRequest("sku-1", 1, BigDecimal.ONE)),
                "corr-1"
        );
        CreateOrderResponse cached = new CreateOrderResponse(1L, 2L, "ACCEPTED", Instant.now());
        when(idempotencyService.execute(
                eq("CREATE_ORDER:42"),
                eq("idem-key"),
                eq(request),
                eq(CreateOrderResponse.class),
                any()
        )).thenReturn(ExecutionResult.success(cached));

        OrderCreateOutcome outcome = service.createOrder(request, "idem-key");

        assertThat(outcome.created()).isFalse();
        assertThat(outcome.response()).isEqualTo(cached);
        verify(orderJdbcRepository, never()).insertOrder(any(Long.class), any(BigDecimal.class), any(Instant.class));
        verify(outboxEnqueueListener, never()).enqueueAfterCommit(any(Long.class));
    }

    @Test
    void createOrderPropagatesConflict() {
        CreateOrderRequest request = new CreateOrderRequest(
                42L,
                List.of(new OrderItemRequest("sku-1", 1, BigDecimal.ONE)),
                "corr-1"
        );
        when(idempotencyService.execute(
                eq("CREATE_ORDER:42"),
                eq("idem-key"),
                eq(request),
                eq(CreateOrderResponse.class),
                any()
        )).thenThrow(new IdempotencyConflictException(
                new IdempotencyKey("CREATE_ORDER:42", "idem-key"),
                "hash-a",
                "hash-b"
        ));

        assertThatThrownBy(() -> service.createOrder(request, "idem-key"))
                .isInstanceOf(IdempotencyConflictException.class);
        verify(orderJdbcRepository, never()).insertOrder(any(Long.class), any(BigDecimal.class), any(Instant.class));
    }
}
