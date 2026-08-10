package com.kholodilin.outbox.order;

import com.kholodilin.idempotency.ExecutionResult;
import com.kholodilin.idempotency.IdempotencyService;
import com.kholodilin.outbox.events.CreateOrderRequest;
import com.kholodilin.outbox.events.CreateOrderResponse;
import com.kholodilin.outbox.events.OrderItemRequest;
import com.kholodilin.outbox.logging.StructuredLogContext;
import com.kholodilin.outbox.metrics.OutboxMetrics;
import com.kholodilin.outbox.outbox.OutboxEnqueueListener;
import com.kholodilin.outbox.outbox.OutboxEventFactory;
import com.kholodilin.outbox.persistence.OrderJdbcRepository;
import com.kholodilin.outbox.persistence.OutboxJdbcRepository;
import com.kholodilin.outbox.tracing.TraceContextSupport;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.math.BigDecimal;
import java.time.Instant;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Writes order domain data and the outbox row in a single database transaction.
 * <p>
 * The outbox event is enqueued only after commit ({@link com.kholodilin.outbox.outbox.OutboxEnqueueListener})
 * so Kafka never sees events that were rolled back with the business transaction.
 * <p>
 * Idempotency is handled by {@link IdempotencyService} in the same transaction:
 * first execution persists the outcome; replays return the stored response without re-running business logic.
 */
@Slf4j
@Service
@RequiredArgsConstructor
public class OrderTransactionService {

    private final OrderJdbcRepository orderJdbcRepository;
    private final OutboxJdbcRepository outboxJdbcRepository;
    private final IdempotencyService idempotencyService;
    private final OutboxEventFactory outboxEventFactory;
    private final OutboxEnqueueListener outboxEnqueueListener;
    private final TraceContextSupport traceContextSupport;
    private final OutboxMetrics metrics;

    /**
     * Claims the idempotency key and either creates a new order or returns a cached replay.
     * <p>
     * Captures W3C {@code traceparent} onto the outbox row and registers post-commit enqueue
     * so the publisher only sees committed events. On any failure while creating, the whole unit
     * rolls back (including the idempotency record).
     *
     * @param request        validated create-order payload
     * @param idempotencyKey client key scoped per customer via operation name
     * @return outcome with response body and whether a new order was created
     */
    @Transactional
    public OrderCreateOutcome createOrder(CreateOrderRequest request, String idempotencyKey) {
        return metrics.orderTransaction().record(() -> persistOrder(request, idempotencyKey));
    }

    private OrderCreateOutcome persistOrder(CreateOrderRequest request, String idempotencyKey) {
        AtomicBoolean executed = new AtomicBoolean(false);
        String operation = "CREATE_ORDER:" + request.customerId();

        ExecutionResult<CreateOrderResponse> result = idempotencyService.execute(
                operation,
                idempotencyKey,
                request,
                CreateOrderResponse.class,
                () -> {
                    executed.set(true);
                    return ExecutionResult.success(createOrderInternal(request));
                }
        );

        CreateOrderResponse response = result.valueOrThrow();
        return new OrderCreateOutcome(response, executed.get());
    }

    private CreateOrderResponse createOrderInternal(CreateOrderRequest request) {
        Instant now = Instant.now();

        BigDecimal total = request.items().stream()
                .map(item -> item.price().multiply(BigDecimal.valueOf(item.quantity())))
                .reduce(BigDecimal.ZERO, BigDecimal::add);

        long orderId = orderJdbcRepository.insertOrder(request.customerId(), total, now);
        log.debug("Order inserted orderId={} customerId={}", orderId, request.customerId());
        for (OrderItemRequest item : request.items()) {
            orderJdbcRepository.insertOrderItem(
                    orderId,
                    request.customerId(),
                    item.productId(),
                    item.quantity(),
                    item.price(),
                    now
            );
        }

        String payload = outboxEventFactory.buildOrderCreatedPayload(orderId, request);
        String traceParent = traceContextSupport.captureTraceParent();
        long eventId = outboxJdbcRepository.insertEvent(
                orderId,
                request.customerId(),
                outboxEventFactory.eventType(),
                payload,
                traceParent,
                now
        );
        log.debug("Outbox event inserted orderId={} eventId={}", orderId, eventId);

        CreateOrderResponse response = new CreateOrderResponse(orderId, eventId, "ACCEPTED", now);
        outboxEnqueueListener.enqueueAfterCommit(eventId);
        StructuredLogContext.putOrderFields(orderId, eventId);
        StructuredLogContext.putEventType(outboxEventFactory.eventType());
        StructuredLogContext.putEventAction("outbox.event.persisted");
        log.info("Order persisted orderId={} eventId={} customerId={}", orderId, eventId, request.customerId());
        return response;
    }
}
