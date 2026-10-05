package com.kholodilin.outbox.order;

import tools.jackson.databind.ObjectMapper;
import com.kholodilin.outbox.events.CreateOrderRequest;
import com.kholodilin.outbox.events.CreateOrderResponse;
import com.kholodilin.idempotency.ExecutionResult;
import com.kholodilin.idempotency.reactive.ReactiveIdempotencyService;
import com.kholodilin.outbox.logging.StructuredLogContext;
import com.kholodilin.outbox.metrics.OutboxMetrics;
import com.kholodilin.outbox.outbox.OutboxEventFactory;
import com.kholodilin.outbox.persistence.OrderR2dbcRepository;
import com.kholodilin.outbox.persistence.OutboxR2dbcRepository;
import com.kholodilin.outbox.queue.InMemoryEventQueue;
import com.kholodilin.outbox.tracing.TraceContextSupport;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.boot.sql.init.dependency.DependsOnDatabaseInitialization;
import org.springframework.stereotype.Service;
import org.springframework.transaction.reactive.TransactionalOperator;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.math.BigDecimal;
import java.time.Instant;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * R2DBC transactional create: idempotency claim → order → items → outbox → complete idempotency,
 * then enqueue AFTER commit.
 */
@Slf4j
@Service
@RequiredArgsConstructor
@DependsOnDatabaseInitialization
public class OrderTransactionService {

    private final OrderR2dbcRepository orderR2dbcRepository;
    private final OutboxR2dbcRepository outboxR2dbcRepository;
    private final ReactiveIdempotencyService idempotencyService;
    private final OutboxEventFactory outboxEventFactory;
    private final InMemoryEventQueue eventQueue;
    private final ObjectMapper objectMapper;
    private final TraceContextSupport traceContextSupport;
    private final OutboxMetrics metrics;
    private final TransactionalOperator transactionalOperator;

    public Mono<OrderCreateOutcome> createOrder(CreateOrderRequest request, String idempotencyKey) {
        long startNs = System.nanoTime();
        return persistOrder(request, idempotencyKey)
                .doOnSuccess(outcome -> {
                    metrics.orderTransaction().record(System.nanoTime() - startNs, TimeUnit.NANOSECONDS);
                    if (outcome.created()) {
                        long eventId = outcome.response().eventId();
                        boolean enqueued = eventQueue.enqueue(eventId);
                        StructuredLogContext.putOrderFields(outcome.response().orderId(), eventId);
                        if (enqueued) {
                            StructuredLogContext.putEventAction("outbox.event.persisted");
                            log.info("Outbox event enqueued after commit eventId={}", eventId);
                        } else {
                            log.warn("Outbox event not enqueued after commit eventId={} (queue full or duplicate)", eventId);
                        }
                    }
                });
    }

    private Mono<OrderCreateOutcome> persistOrder(CreateOrderRequest request, String idempotencyKey) {
        AtomicBoolean executed = new AtomicBoolean(false);

        return transactionalOperator.transactional(
                        idempotencyService
                                .operation("CREATE_ORDER")
                                .key(idempotencyKey)
                                .request(request)
                                .execute(CreateOrderResponse.class, () -> {
                                    executed.set(true);
                                    return createNewOrder(request)
                                            .map(ExecutionResult::success);
                                }))
                .map(result -> new OrderCreateOutcome(result.valueOrThrow(), executed.get()));

    }

    private Mono<CreateOrderResponse> createNewOrder(
            CreateOrderRequest request  
    ) {
        BigDecimal total = request.items().stream()
                .map(item -> item.price().multiply(BigDecimal.valueOf(item.quantity())))
                .reduce(BigDecimal.ZERO, BigDecimal::add);
        Instant now = Instant.now();
        return orderR2dbcRepository.insertOrder(request.customerId(), total, now)
                .flatMap(orderId -> insertItems(orderId, request, now)
                        .then(Mono.defer(() -> {
                            String payload = outboxEventFactory.buildOrderCreatedPayload(orderId, request);
                            String traceParent = traceContextSupport.captureTraceParent();
                            return outboxR2dbcRepository.insertEvent(
                                    orderId,
                                    request.customerId(),
                                    outboxEventFactory.eventType(),
                                    payload,
                                    traceParent,
                                    now
                            ).map(eventId -> MapEntry.of(orderId, eventId));
                        })))
                .flatMap(pair -> {
                    CreateOrderResponse response = new CreateOrderResponse(pair.orderId(), pair.eventId(), "ACCEPTED", now);
                    return Mono.just(response);
                })
                .doOnNext(response -> {
                    StructuredLogContext.putOrderFields(response.orderId(), response.eventId());
                    StructuredLogContext.putEventType(outboxEventFactory.eventType());
                    StructuredLogContext.putEventAction("outbox.event.persisted");
                    log.info("Order persisted orderId={} eventId={} customerId={}",
                            response.orderId(), response.eventId(), request.customerId());
                });
               
    }

    private Mono<Void> insertItems(long orderId, CreateOrderRequest request, Instant now) {
        return Flux.fromIterable(request.items())
                .concatMap(item -> orderR2dbcRepository.insertOrderItem(
                        orderId,
                        request.customerId(),
                        item.productId(),
                        item.quantity(),
                        item.price(),
                        now
                ))
                .then();
    }

    private record MapEntry(long orderId, long eventId) {
        static MapEntry of(long orderId, long eventId) {
            return new MapEntry(orderId, eventId);
        }
    }
}