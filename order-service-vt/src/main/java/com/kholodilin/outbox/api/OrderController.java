package com.kholodilin.outbox.api;

import com.kholodilin.outbox.events.CreateOrderRequest;
import com.kholodilin.outbox.events.CreateOrderResponse;
import com.kholodilin.outbox.events.EventConstants;
import com.kholodilin.outbox.logging.StructuredLogContext;
import com.kholodilin.outbox.order.OrderCreateOutcome;
import com.kholodilin.outbox.order.OrderTransactionService;
import jakarta.validation.Valid;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestHeader;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.util.UUID;

/**
 * HTTP entry point for order creation with idempotency support.
 * <p>
 * Flow: rate limit (filter) → transactional claim/create → 201/200/409.
 * <p>
 * Idempotency is handled by {@code spring-boot-idempotency-starter} inside
 * {@link OrderTransactionService#createOrder}: same key + same body → 200 with stored response;
 * new key → create (201); same key + different body →
 * {@link com.kholodilin.idempotency.exception.IdempotencyConflictException} mapped to HTTP 409 by
 * {@link GlobalExceptionHandler}.
 */
@Slf4j
@RestController
@RequestMapping("/api/v1/orders")
@RequiredArgsConstructor
public class OrderController {

    private final OrderTransactionService orderTransactionService;

    /**
     * Creates an order or returns a cached idempotent response.
     *
     * @param idempotencyKey client-supplied key ({@code Idempotency-Key} header); scoped per customer
     * @param request        validated order payload (also included in the request fingerprint)
     * @return {@code 201} for a new order, {@code 200} when replaying the same key + body;
     *         {@code 409} is thrown as {@link com.kholodilin.idempotency.exception.IdempotencyConflictException}
     *         and mapped by {@link GlobalExceptionHandler}
     */
    @PostMapping
    public ResponseEntity<CreateOrderResponse> createOrder(
            @RequestHeader(EventConstants.IDEMPOTENCY_KEY_HEADER) String idempotencyKey,
            @Valid @RequestBody CreateOrderRequest request
    ) {
        String correlationId = request.correlationId() != null ? request.correlationId() : UUID.randomUUID().toString();
        StructuredLogContext.putCorrelation(correlationId, request.customerId(), idempotencyKey);
        StructuredLogContext.putEventAction("http.request.accepted");

        log.info("Order request accepted customerId={} idempotencyKey={}", request.customerId(), idempotencyKey);
        log.debug("Order request body customerId={} items={}", request.customerId(), request.items().size());

        // May throw IdempotencyConflictException (409) when the key exists with a different request fingerprint;
        // not caught here — see GlobalExceptionHandler.
        OrderCreateOutcome outcome = orderTransactionService.createOrder(request, idempotencyKey);
        CreateOrderResponse response = outcome.response();
        StructuredLogContext.putOrderFields(response.orderId(), response.eventId());
        StructuredLogContext.putEventAction("http.request.completed");
        if (outcome.created()) {
            return ResponseEntity.status(HttpStatus.CREATED).body(response);
        }
        return ResponseEntity.ok(response);
    }
}
