package com.kholodilin.outbox.order;

import tools.jackson.databind.ObjectMapper;
import com.kholodilin.outbox.events.CreateOrderRequest;
import com.kholodilin.outbox.events.CreateOrderResponse;
import com.kholodilin.outbox.events.OrderItemRequest;
import com.kholodilin.outbox.persistence.IdempotencyJdbcRepository;
import com.kholodilin.outbox.logging.StructuredLogContext;
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

/**
 * Writes order domain data and the outbox row in a single database transaction.
 * <p>
 * The outbox event is enqueued only after commit ({@link com.kholodilin.outbox.outbox.OutboxEnqueueListener})
 * so Kafka never sees events that were rolled back with the business transaction.
 */
@Slf4j
@Service
@RequiredArgsConstructor
public class OrderTransactionService {

    private final OrderJdbcRepository orderJdbcRepository;
    private final OutboxJdbcRepository outboxJdbcRepository;
    private final IdempotencyJdbcRepository idempotencyJdbcRepository;
    private final OutboxEventFactory outboxEventFactory;
    private final OutboxEnqueueListener outboxEnqueueListener;
    private final ObjectMapper objectMapper;
    private final TraceContextSupport traceContextSupport;

    /**
     * Persists the order, items, outbox row, and completed idempotency record in one transaction.
     * <p>
     * Captures W3C {@code traceparent} onto the outbox row and registers post-commit enqueue
     * so the publisher only sees committed events. On any failure the whole unit rolls back
     * (including the PROCESSING idempotency insert).
     *
     * @param request        validated create-order payload
     * @param idempotencyKey client key stored with the request hash
     * @param requestHash    SHA-256 of the canonical request body
     * @return accepted response stored for later idempotent replays
     */
    @Transactional
    public CreateOrderResponse createOrder(CreateOrderRequest request, String idempotencyKey, String requestHash) {
        Instant now = Instant.now();
        long idempotencyId = idempotencyJdbcRepository.insertProcessing(
                request.getCustomerId(), idempotencyKey, requestHash, now);
        log.debug("Idempotency key inserted id={} customerId={} idempotencyKey={}",
                idempotencyId, request.getCustomerId(), idempotencyKey);

        BigDecimal total = request.getItems().stream()
                .map(item -> item.getPrice().multiply(BigDecimal.valueOf(item.getQuantity())))
                .reduce(BigDecimal.ZERO, BigDecimal::add);

        long orderId = orderJdbcRepository.insertOrder(request.getCustomerId(), total, now);
        log.debug("Order inserted orderId={} customerId={}", orderId, request.getCustomerId());
        for (OrderItemRequest item : request.getItems()) {
            orderJdbcRepository.insertOrderItem(
                    orderId,
                    request.getCustomerId(),
                    item.getProductId(),
                    item.getQuantity(),
                    item.getPrice(),
                    now
            );
        }

        String payload = outboxEventFactory.buildOrderCreatedPayload(orderId, request);
        String traceParent = traceContextSupport.captureTraceParent();
        long eventId = outboxJdbcRepository.insertEvent(
                orderId,
                request.getCustomerId(),
                outboxEventFactory.eventType(),
                payload,
                traceParent,
                now
        );
        log.debug("Outbox event inserted orderId={} eventId={}", orderId, eventId);

        CreateOrderResponse response = CreateOrderResponse.builder()
                .orderId(orderId)
                .eventId(eventId)
                .status("ACCEPTED")
                .createdAt(now)
                .build();
        try {
            // Store response for idempotent replays before the transaction commits.
            idempotencyJdbcRepository.complete(
                    request.getCustomerId(),
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
        log.info("Order persisted orderId={} eventId={} customerId={}", orderId, eventId, request.getCustomerId());
        return response;
    }
}
