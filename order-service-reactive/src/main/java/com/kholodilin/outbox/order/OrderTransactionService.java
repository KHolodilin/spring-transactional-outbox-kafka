package com.kholodilin.outbox.order;

import tools.jackson.databind.ObjectMapper;
import com.kholodilin.outbox.events.CreateOrderRequest;
import com.kholodilin.outbox.events.CreateOrderResponse;
import com.kholodilin.outbox.idempotency.IdempotencyService;
import com.kholodilin.outbox.logging.StructuredLogContext;
import com.kholodilin.outbox.metrics.OutboxMetrics;
import com.kholodilin.outbox.outbox.OutboxEventFactory;
import com.kholodilin.outbox.persistence.IdempotencyR2dbcRepository;
import com.kholodilin.outbox.persistence.OrderR2dbcRepository;
import com.kholodilin.outbox.persistence.OutboxR2dbcRepository;
import com.kholodilin.outbox.queue.InMemoryEventQueue;
import com.kholodilin.outbox.tracing.TraceContextSupport;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;
import org.springframework.transaction.reactive.TransactionalOperator;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.math.BigDecimal;
import java.time.Instant;
import java.util.concurrent.TimeUnit;

/**
 * R2DBC transactional create: idempotency claim → order → items → outbox → complete idempotency,
 * then enqueue AFTER commit.
 */
@Slf4j
@Service
@RequiredArgsConstructor
public class OrderTransactionService {

    private final OrderR2dbcRepository orderR2dbcRepository;
    private final OutboxR2dbcRepository outboxR2dbcRepository;
    private final IdempotencyR2dbcRepository idempotencyR2dbcRepository;
    private final IdempotencyService idempotencyService;
    private final OutboxEventFactory outboxEventFactory;
    private final InMemoryEventQueue eventQueue;
    private final ObjectMapper objectMapper;
    private final TraceContextSupport traceContextSupport;
    private final OutboxMetrics metrics;
    private final TransactionalOperator transactionalOperator;

    public Mono<OrderCreateOutcome> createOrder(CreateOrderRequest request, String idempotencyKey, String requestHash) {
        long startNs = System.nanoTime();
        return transactionalOperator.transactional(persistOrder(request, idempotencyKey, requestHash))
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

    private Mono<OrderCreateOutcome> persistOrder(CreateOrderRequest request, String idempotencyKey, String requestHash) {
        Instant now = Instant.now();
        return idempotencyR2dbcRepository.tryInsertProcessing(request.customerId(), idempotencyKey, requestHash, now)
                .flatMap(claimedId -> createNewOrder(request, idempotencyKey, now, claimedId))
                .switchIfEmpty(idempotencyService.findCachedResponse(request.customerId(), idempotencyKey, requestHash)
                        .map(cached -> new OrderCreateOutcome(cached, false)));
    }

    private Mono<OrderCreateOutcome> createNewOrder(
            CreateOrderRequest request,
            String idempotencyKey,
            Instant now,
            Long claimedId
    ) {
        log.debug("Idempotency key inserted id={} customerId={} idempotencyKey={}",
                claimedId, request.customerId(), idempotencyKey);

        BigDecimal total = request.items().stream()
                .map(item -> item.price().multiply(BigDecimal.valueOf(item.quantity())))
                .reduce(BigDecimal.ZERO, BigDecimal::add);

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
                    String responseJson;
                    try {
                        responseJson = objectMapper.writeValueAsString(response);
                    } catch (Exception ex) {
                        return Mono.error(new IllegalStateException("Failed to persist idempotent response", ex));
                    }
                    return idempotencyR2dbcRepository.complete(
                                    request.customerId(),
                                    idempotencyKey,
                                    responseJson,
                                    now
                            )
                            .thenReturn(response);
                })
                .doOnNext(response -> {
                    StructuredLogContext.putOrderFields(response.orderId(), response.eventId());
                    StructuredLogContext.putEventType(outboxEventFactory.eventType());
                    StructuredLogContext.putEventAction("outbox.event.persisted");
                    log.info("Order persisted orderId={} eventId={} customerId={}",
                            response.orderId(), response.eventId(), request.customerId());
                })
                .map(response -> new OrderCreateOutcome(response, true));
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
