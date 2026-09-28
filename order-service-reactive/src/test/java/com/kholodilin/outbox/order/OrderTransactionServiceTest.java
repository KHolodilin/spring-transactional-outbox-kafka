package com.kholodilin.outbox.order;

import tools.jackson.databind.json.JsonMapper;

import com.kholodilin.idempotency.reactive.ReactiveIdempotencyService;
import com.kholodilin.outbox.events.CreateOrderRequest;
import com.kholodilin.outbox.events.CreateOrderResponse;
import com.kholodilin.outbox.events.OrderItemRequest;
import com.kholodilin.outbox.metrics.OutboxMetrics;
import com.kholodilin.outbox.outbox.OutboxEventFactory;
import com.kholodilin.outbox.persistence.OrderR2dbcRepository;
import com.kholodilin.outbox.persistence.OutboxR2dbcRepository;
import com.kholodilin.outbox.queue.InMemoryEventQueue;
import com.kholodilin.outbox.tracing.TraceContextSupport;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.test.util.ReflectionTestUtils;
import org.springframework.transaction.reactive.TransactionalOperator;
import reactor.core.publisher.Mono;
import reactor.test.StepVerifier;

import java.math.BigDecimal;
import java.time.Instant;
import java.util.List;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class OrderTransactionServiceTest {

    @Mock
    private OrderR2dbcRepository orderR2dbcRepository;

    @Mock
    private OutboxR2dbcRepository outboxR2dbcRepository;

   

    @Mock
    private ReactiveIdempotencyService idempotencyService;

    @Mock
    private OutboxEventFactory outboxEventFactory;

    @Mock
    private InMemoryEventQueue eventQueue;

    @Mock
    private TraceContextSupport traceContextSupport;

    @Mock
    private TransactionalOperator transactionalOperator;

    private OrderTransactionService service;
    private OutboxMetrics metrics;

    @BeforeEach
    void setUp() {
        metrics = new OutboxMetrics(new SimpleMeterRegistry());
        ReflectionTestUtils.invokeMethod(metrics, "registerMeters");
        when(transactionalOperator.transactional(any(Mono.class))).thenAnswer(inv -> inv.getArgument(0));
        // switchIfEmpty evaluates the alternate publisher eagerly
       
        service = new OrderTransactionService(
                orderR2dbcRepository,
                outboxR2dbcRepository,
                idempotencyService,
                outboxEventFactory,
                eventQueue,
                JsonMapper.builder().build(),
                traceContextSupport,
                metrics,
                transactionalOperator
        );
    }

    @Test
    void createOrderPersistsAndEnqueues() {
        CreateOrderRequest request = new CreateOrderRequest(
                42L,
                List.of(new OrderItemRequest("sku-1", 2, BigDecimal.valueOf(5))),
                "corr-1"
        );

        
        when(orderR2dbcRepository.insertOrder(eq(42L), eq(BigDecimal.valueOf(10)), any(Instant.class)))
                .thenReturn(Mono.just(100L));
        when(orderR2dbcRepository.insertOrderItem(
                eq(100L), eq(42L), eq("sku-1"), eq(2), eq(BigDecimal.valueOf(5)), any(Instant.class)))
                .thenReturn(Mono.empty());
        when(outboxEventFactory.buildOrderCreatedPayload(100L, request)).thenReturn("{\"orderId\":100}");
        when(outboxEventFactory.eventType()).thenReturn("OrderCreated");
        when(traceContextSupport.captureTraceParent()).thenReturn("00-trace");
        when(outboxR2dbcRepository.insertEvent(
                eq(100L), eq(42L), eq("OrderCreated"), eq("{\"orderId\":100}"), eq("00-trace"), any(Instant.class)))
                .thenReturn(Mono.just(200L));
        
        when(eventQueue.enqueue(200L)).thenReturn(true);

        StepVerifier.create(service.createOrder(request, "idem-key"))
                .assertNext(outcome -> {
                    org.assertj.core.api.Assertions.assertThat(outcome.created()).isTrue();
                    org.assertj.core.api.Assertions.assertThat(outcome.response().orderId()).isEqualTo(100L);
                    org.assertj.core.api.Assertions.assertThat(outcome.response().eventId()).isEqualTo(200L);
                    org.assertj.core.api.Assertions.assertThat(outcome.response().status()).isEqualTo("ACCEPTED");
                })
                .verifyComplete();

        verify(eventQueue).enqueue(200L);
        verify(orderR2dbcRepository).insertOrderItem(
                eq(100L), eq(42L), eq("sku-1"), eq(2), eq(BigDecimal.valueOf(5)), any(Instant.class));
    }

    @Test
    void createOrderReturnsCachedResponseWhenInsertConflicts() {
        CreateOrderRequest request = new CreateOrderRequest(
                42L,
                List.of(new OrderItemRequest("sku-1", 1, BigDecimal.ONE)),
                "corr-1"
        );
        CreateOrderResponse cached = new CreateOrderResponse(1L, 2L, "ACCEPTED", Instant.now());
       
        StepVerifier.create(service.createOrder(request, "idem-key"))
                .assertNext(outcome -> {
                    org.assertj.core.api.Assertions.assertThat(outcome.created()).isFalse();
                    org.assertj.core.api.Assertions.assertThat(outcome.response()).isEqualTo(cached);
                })
                .verifyComplete();

        verify(orderR2dbcRepository, never()).insertOrder(any(Long.class), any(BigDecimal.class), any(Instant.class));
        verify(eventQueue, never()).enqueue(any(Long.class));
    }

    @Test
    void createOrderPropagatesConflictWhenExistingRowConflicts() {
        CreateOrderRequest request = new CreateOrderRequest(
                42L,
                List.of(new OrderItemRequest("sku-1", 1, BigDecimal.ONE)),
                "corr-1"
        );
        
       
        verify(orderR2dbcRepository, never()).insertOrder(any(Long.class), any(BigDecimal.class), any(Instant.class));
    }
}
