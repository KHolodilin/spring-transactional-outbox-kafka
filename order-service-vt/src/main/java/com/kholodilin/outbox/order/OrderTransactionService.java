package com.kholodilin.outbox.order;

import tools.jackson.databind.ObjectMapper;
import com.kholodilin.outbox.events.CreateOrderRequest;
import com.kholodilin.outbox.events.CreateOrderResponse;
import com.kholodilin.outbox.events.OrderItemRequest;
import com.kholodilin.outbox.idempotency.IdempotencyService;
import com.kholodilin.outbox.persistence.IdempotencyJdbcRepository;
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
import java.util.Optional;

/**
 * Writes order domain data and the outbox row in a single database transaction.
 * <p>
 * The outbox event is enqueued only after commit ({@link com.kholodilin.outbox.outbox.OutboxEnqueueListener})
 * so Kafka never sees events that were rolled back with the business transaction.
 * <p>
 * Idempotency claim uses {@code INSERT … ON CONFLICT DO NOTHING RETURNING id} in this same transaction;
 * on conflict the existing row is loaded and resolved to a cached response or a 409 conflict.
 */
@Slf4j
@Service
@RequiredArgsConstructor
public class OrderTransactionService {

    private final OrderJdbcRepository orderJdbcRepository;
    private final OutboxJdbcRepository outboxJdbcRepository;
    private final IdempotencyJdbcRepository idempotencyJdbcRepository;
    private final IdempotencyService idempotencyService;
    private final OutboxEventFactory outboxEventFactory;
    private final OutboxEnqueueListener outboxEnqueueListener;
    private final ObjectMapper objectMapper;
    private final TraceContextSupport traceContextSupport;
    private final OutboxMetrics metrics;

    /**
     * Claims the idempotency key and either creates a new order or returns a cached replay.
     * <p>
     * Captures W3C {@code traceparent} onto the outbox row and registers post-commit enqueue
     * so the publisher only sees committed events. On any failure while creating, the whole unit
     * rolls back (including the PROCESSING idempotency insert).
     *
     * @param request        validated create-order payload
     * @param idempotencyKey client key stored with the request hash
     * @param requestHash    SHA-256 of the canonical request body
     * @return outcome with response body and whether a new order was created
     */
    @Transactional
    public OrderCreateOutcome createOrder(CreateOrderRequest request, String idempotencyKey, String requestHash) {
        return metrics.orderTransaction().record(() -> persistOrder(request, idempotencyKey, requestHash));
    }

    private OrderCreateOutcome persistOrder(CreateOrderRequest request, String idempotencyKey, String requestHash) {
        Instant now = Instant.now();
        Optional<Long> claimedId = idempotencyJdbcRepository.tryInsertProcessing(
                request.customerId(), idempotencyKey, requestHash, now);

        if (claimedId.isEmpty()) {
            CreateOrderResponse cached = idempotencyService.findCachedResponse(
                            request.customerId(), idempotencyKey, requestHash)
                    .orElseThrow(() -> new IllegalStateException(
                            "Idempotency key conflicted but no usable row was found"));
            return new OrderCreateOutcome(cached, false);
        }

        log.debug("Idempotency key inserted id={} customerId={} idempotencyKey={}",
                claimedId.get(), request.customerId(), idempotencyKey);

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
        try {
            // Store response for idempotent replays before the transaction commits.
            idempotencyJdbcRepository.complete(
                    request.customerId(),
                    idempotencyKey,
                    objectMapper.writeValueAsString(response),
                    now
            );
        } catch (Exception ex) {
            throw new IllegalStateException("Failed to persist idempotent response", ex);
        }

        outboxEnqueueListener.enqueueAfterCommit(eventId);
        StructuredLogContext.putOrderFields(orderId, eventId);
        StructuredLogContext.putEventType(outboxEventFactory.eventType());
        StructuredLogContext.putEventAction("outbox.event.persisted");
        log.info("Order persisted orderId={} eventId={} customerId={}", orderId, eventId, request.customerId());
        return new OrderCreateOutcome(response, true);
    }
}
