package com.kholodilin.outbox.order;

import com.kholodilin.idempotency.ExecutionResult;
import com.kholodilin.idempotency.IdempotencyService;
import com.kholodilin.outbox.events.CreateOrderRequest;
import com.kholodilin.outbox.events.CreateOrderResponse;
import com.kholodilin.outbox.events.ObservabilityVocabulary;
import com.kholodilin.outbox.events.OrderItemRequest;
import com.kholodilin.outbox.logging.StructuredLogContext;
import com.kholodilin.outbox.metrics.OrderServiceMetrics;
import com.kholodilin.outbox.outbox.OutboxEventFactory;
import com.kholodilin.outbox.persistence.OrderJdbcRepository;
import com.kholodilin.outbox.persistence.OutboxJdbcRepository;
import com.kholodilin.outbox.tracing.TraceContextSupport;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;
import org.springframework.transaction.PlatformTransactionManager;
import org.springframework.transaction.support.TransactionTemplate;

import java.math.BigDecimal;
import java.time.Instant;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Writes the order and an insert-only outbox row in one database transaction.
 * <p>
 * {@code db.persist} is opened outside {@link TransactionTemplate} so the span includes
 * transaction start and commit. {@code outbox.save} covers the inserts inside that transaction.
 * Idempotency uses {@link IdempotencyService} in that same transaction. The callback runs only
 * on the first request, so a replay does not insert a second order or outbox row.
 */
@Slf4j
@Service
@RequiredArgsConstructor
public class OrderTransactionService {

    private final OrderJdbcRepository orderJdbcRepository;
    private final OutboxJdbcRepository outboxJdbcRepository;
    private final IdempotencyService idempotencyService;
    private final OutboxEventFactory outboxEventFactory;
    private final TraceContextSupport traceContextSupport;
    private final OrderServiceMetrics metrics;
    private final PlatformTransactionManager transactionManager;

    /**
     * Claims the idempotency key and either creates a new order or returns a cached replay.
     * <p>
     * The {@code db.persist} span stays open until {@link TransactionTemplate} commits or rolls back.
     *
     * @param request        validated create-order payload
     * @param idempotencyKey client key scoped per customer via operation name
     * @return outcome with response body and whether a new order was created
     */
    public OrderCreateOutcome createOrder(CreateOrderRequest request, String idempotencyKey) {
        return traceContextSupport.runWithTraceParent(
                null,
                ObservabilityVocabulary.SPAN_DB_PERSIST,
                () -> metrics.orderTransaction().record(() ->
                        new TransactionTemplate(transactionManager).execute(status -> persistOrder(request, idempotencyKey)))
        );
    }

    private OrderCreateOutcome persistOrder(CreateOrderRequest request, String idempotencyKey) {
        AtomicBoolean executed = new AtomicBoolean(false);

        ExecutionResult<CreateOrderResponse> result = idempotencyService
                .operation("CREATE_ORDER:" + request.customerId())
                .key(idempotencyKey)
                .request(request)
                .execute(CreateOrderResponse.class, () -> {
                    executed.set(true);
                    return ExecutionResult.success(createOrderInternal(request));
                });

        return new OrderCreateOutcome(result.valueOrThrow(), executed.get());
    }

    private CreateOrderResponse createOrderInternal(CreateOrderRequest request) {
        return traceContextSupport.runWithTraceParent(
                null,
                ObservabilityVocabulary.SPAN_OUTBOX_SAVE,
                () -> insertOrderAndOutbox(request)
        );
    }

    private CreateOrderResponse insertOrderAndOutbox(CreateOrderRequest request) {
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
        long eventId = outboxJdbcRepository.insert(
                now,
                String.valueOf(orderId),
                String.valueOf(request.customerId()),
                outboxEventFactory.eventType(),
                payload,
                traceParent
        );
        log.debug("Outbox event inserted orderId={} eventId={} traceParent={}", orderId, eventId, traceParent);

        CreateOrderResponse response = new CreateOrderResponse(orderId, eventId, "ACCEPTED", now);
        StructuredLogContext.putOrderFields(orderId, eventId);
        StructuredLogContext.putEventType(outboxEventFactory.eventType());
        StructuredLogContext.putEventAction("outbox.event.persisted");
        log.info("Order persisted orderId={} eventId={} customerId={}", orderId, eventId, request.customerId());
        return response;
    }
}
