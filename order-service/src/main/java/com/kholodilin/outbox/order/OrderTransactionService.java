package com.kholodilin.outbox.order;

import tools.jackson.databind.ObjectMapper;
import com.kholodilin.outbox.events.CreateOrderRequest;
import com.kholodilin.outbox.events.CreateOrderResponse;
import com.kholodilin.outbox.events.OrderItemRequest;
import com.kholodilin.outbox.persistence.IdempotencyJdbcRepository;
import com.kholodilin.outbox.outbox.OutboxEnqueueListener;
import com.kholodilin.outbox.outbox.OutboxEventFactory;
import com.kholodilin.outbox.persistence.OrderJdbcRepository;
import com.kholodilin.outbox.persistence.OutboxJdbcRepository;
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

    @Transactional
    public CreateOrderResponse createOrder(CreateOrderRequest request, String idempotencyKey, String requestHash) {
        Instant now = Instant.now();
        idempotencyJdbcRepository.insertProcessing(request.getCustomerId(), idempotencyKey, requestHash, now);

        BigDecimal total = request.getItems().stream()
                .map(item -> item.getPrice().multiply(BigDecimal.valueOf(item.getQuantity())))
                .reduce(BigDecimal.ZERO, BigDecimal::add);

        long orderId = orderJdbcRepository.insertOrder(request.getCustomerId(), total, now);
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
        long eventId = outboxJdbcRepository.insertEvent(
                orderId,
                request.getCustomerId(),
                outboxEventFactory.eventType(),
                payload,
                now
        );

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
        log.info("Order persisted orderId={} eventId={} customerId={}", orderId, eventId, request.getCustomerId());
        return response;
    }
}
