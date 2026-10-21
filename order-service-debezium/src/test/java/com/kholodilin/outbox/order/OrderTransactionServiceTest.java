package com.kholodilin.outbox.order;

import com.kholodilin.idempotency.ExecutionResult;
import com.kholodilin.idempotency.IdempotencyCall;
import com.kholodilin.idempotency.IdempotencyService;
import com.kholodilin.idempotency.exception.IdempotencyConflictException;
import com.kholodilin.idempotency.model.IdempotencyKey;
import com.kholodilin.outbox.events.CreateOrderRequest;
import com.kholodilin.outbox.events.CreateOrderResponse;
import com.kholodilin.outbox.events.OrderItemRequest;
import com.kholodilin.outbox.metrics.OrderServiceMetrics;
import com.kholodilin.outbox.outbox.OutboxEventFactory;
import com.kholodilin.outbox.persistence.OrderJdbcRepository;
import com.kholodilin.outbox.persistence.OutboxJdbcRepository;
import com.kholodilin.outbox.tracing.TraceContextSupport;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.slf4j.MDC;
import org.springframework.test.util.ReflectionTestUtils;
import org.springframework.transaction.PlatformTransactionManager;
import org.springframework.transaction.support.SimpleTransactionStatus;

import java.math.BigDecimal;
import java.time.Instant;
import java.util.List;
import java.util.function.Supplier;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.ArgumentMatchers.isNull;
import static org.mockito.ArgumentMatchers.nullable;
import static org.mockito.Mockito.lenient;
import static org.mockito.Mockito.mock;
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
    private TraceContextSupport traceContextSupport;

    @Mock
    private IdempotencyCall idempotencyCall;

    @AfterEach
    void tearDown() {
        MDC.clear();
    }

    @Test
    void createOrderPersistsDomainDataAndInsertsOutbox() {
        CreateOrderRequest request = new CreateOrderRequest(
                42L,
                List.of(
                        new OrderItemRequest("sku-1", 2, BigDecimal.valueOf(5)),
                        new OrderItemRequest("sku-2", 1, BigDecimal.valueOf(3))
                ),
                "corr-1"
        );
        SimpleMeterRegistry registry = new SimpleMeterRegistry();
        OrderServiceMetrics metrics = metrics(registry);
        OrderTransactionService service = service(metrics);
        when(orderJdbcRepository.insertOrder(eq(42L), eq(BigDecimal.valueOf(13)), any(Instant.class))).thenReturn(100L);
        when(outboxEventFactory.buildOrderCreatedPayload(100L, request)).thenReturn("{\"orderId\":100}");
        when(outboxEventFactory.eventType()).thenReturn("OrderCreated");
        when(traceContextSupport.captureTraceParent()).thenReturn("00-trace");
        when(outboxJdbcRepository.insert(any(), eq("100"), eq("42"), eq("OrderCreated"), eq("{\"orderId\":100}"), eq("00-trace")))
                .thenReturn(200L);
        stubIdempotencyExecuteAction();

        OrderCreateOutcome outcome = service.createOrder(request, "idem-key");

        assertThat(outcome.created()).isTrue();
        assertThat(outcome.response().orderId()).isEqualTo(100L);
        assertThat(outcome.response().eventId()).isEqualTo(200L);
        assertThat(outcome.response().status()).isEqualTo("ACCEPTED");
        verify(orderJdbcRepository).insertOrderItem(eq(100L), eq(42L), eq("sku-1"), eq(2), eq(BigDecimal.valueOf(5)), any(Instant.class));
        verify(orderJdbcRepository).insertOrderItem(eq(100L), eq(42L), eq("sku-2"), eq(1), eq(BigDecimal.valueOf(3)), any(Instant.class));
        assertThat(MDC.get("event.type")).isEqualTo("OrderCreated");
        assertThat(MDC.get("event.action")).isEqualTo("outbox.event.persisted");
        assertThat(registry.get("order.transaction").timer().count()).isEqualTo(1);
    }

    @Test
    void createOrderStoresNullTraceParent() {
        CreateOrderRequest request = new CreateOrderRequest(
                42L,
                List.of(new OrderItemRequest("sku-1", 1, BigDecimal.ONE)),
                null
        );
        OrderTransactionService service = service();
        when(orderJdbcRepository.insertOrder(eq(42L), eq(BigDecimal.ONE), any(Instant.class))).thenReturn(100L);
        when(outboxEventFactory.buildOrderCreatedPayload(100L, request)).thenReturn("{}");
        when(outboxEventFactory.eventType()).thenReturn("OrderCreated");
        when(traceContextSupport.captureTraceParent()).thenReturn(null);
        when(outboxJdbcRepository.insert(any(), eq("100"), eq("42"), eq("OrderCreated"), eq("{}"), isNull()))
                .thenReturn(200L);
        stubIdempotencyExecuteAction();

        OrderCreateOutcome outcome = service.createOrder(request, "idem-key");

        assertThat(outcome.created()).isTrue();
        verify(outboxJdbcRepository).insert(any(), eq("100"), eq("42"), eq("OrderCreated"), eq("{}"), isNull());
    }

    @Test
    void createOrderReturnsCachedResponseWhenReplay() {
        CreateOrderRequest request = new CreateOrderRequest(
                42L,
                List.of(new OrderItemRequest("sku-1", 1, BigDecimal.ONE)),
                "corr-1"
        );
        CreateOrderResponse cached = new CreateOrderResponse(1L, 2L, "ACCEPTED", Instant.now());
        when(idempotencyService.operation("CREATE_ORDER:42")).thenReturn(idempotencyCall);
        when(idempotencyCall.key("idem-key")).thenReturn(idempotencyCall);
        when(idempotencyCall.request(request)).thenReturn(idempotencyCall);
        when(idempotencyCall.execute(eq(CreateOrderResponse.class), any())).thenReturn(ExecutionResult.success(cached));

        OrderCreateOutcome outcome = service().createOrder(request, "idem-key");

        assertThat(outcome.created()).isFalse();
        assertThat(outcome.response()).isEqualTo(cached);
        verify(orderJdbcRepository, never()).insertOrder(any(), any(), any());
        verify(outboxJdbcRepository, never()).insert(any(), anyString(), anyString(), anyString(), anyString(), any());
    }

    @Test
    void createOrderPropagatesConflict() {
        CreateOrderRequest request = new CreateOrderRequest(
                42L,
                List.of(new OrderItemRequest("sku-1", 1, BigDecimal.ONE)),
                "corr-1"
        );
        when(idempotencyService.operation("CREATE_ORDER:42")).thenReturn(idempotencyCall);
        when(idempotencyCall.key("idem-key")).thenReturn(idempotencyCall);
        when(idempotencyCall.request(request)).thenReturn(idempotencyCall);
        when(idempotencyCall.execute(eq(CreateOrderResponse.class), any())).thenThrow(new IdempotencyConflictException(
                new IdempotencyKey("CREATE_ORDER:42", "idem-key"),
                "hash-a",
                "hash-b"
        ));

        assertThatThrownBy(() -> service().createOrder(request, "idem-key"))
                .isInstanceOf(IdempotencyConflictException.class);
        verify(orderJdbcRepository, never()).insertOrder(any(), any(), any());
    }

    private OrderTransactionService service() {
        return service(metrics(new SimpleMeterRegistry()));
    }

    private OrderTransactionService service(OrderServiceMetrics metrics) {
        lenient().when(traceContextSupport.runWithTraceParent(nullable(String.class), anyString(), any(Supplier.class)))
                .thenAnswer(invocation -> invocation.<Supplier<?>>getArgument(2).get());
        PlatformTransactionManager transactionManager = mock(PlatformTransactionManager.class);
        lenient().when(transactionManager.getTransaction(any())).thenReturn(new SimpleTransactionStatus());
        return new OrderTransactionService(
                orderJdbcRepository,
                outboxJdbcRepository,
                idempotencyService,
                outboxEventFactory,
                traceContextSupport,
                metrics,
                transactionManager
        );
    }

    private static OrderServiceMetrics metrics(SimpleMeterRegistry registry) {
        OrderServiceMetrics metrics = new OrderServiceMetrics(registry);
        ReflectionTestUtils.invokeMethod(metrics, "registerMeters");
        return metrics;
    }

    private void stubIdempotencyExecuteAction() {
        when(idempotencyService.operation(anyString())).thenReturn(idempotencyCall);
        when(idempotencyCall.key(anyString())).thenReturn(idempotencyCall);
        when(idempotencyCall.request(any())).thenReturn(idempotencyCall);
        when(idempotencyCall.execute(eq(CreateOrderResponse.class), any())).thenAnswer(invocation -> {
            Supplier<ExecutionResult<CreateOrderResponse>> action = invocation.getArgument(1);
            return action.get();
        });
    }
}
