package com.kholodilin.outbox.api;

import com.kholodilin.outbox.events.CreateOrderRequest;
import com.kholodilin.outbox.events.CreateOrderResponse;
import com.kholodilin.outbox.events.EventConstants;
import com.kholodilin.outbox.idempotency.IdempotencyService;
import com.kholodilin.outbox.order.OrderTransactionService;
import jakarta.validation.Valid;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.slf4j.MDC;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestHeader;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.net.URI;
import java.util.Optional;
import java.util.UUID;

/**
 * HTTP entry point for order creation with idempotency support.
 * <p>
 * Flow: rate limit (filter) → idempotency lookup → transactional write → 201/200/409.
 * <p>
 * Idempotency outcomes are handled in {@link IdempotencyService#findCachedResponse}:
 * same key + same body hash → 200 with stored response; no prior key → continue to create (201);
 * same key + different body hash (or key still PROCESSING) → {@link com.kholodilin.outbox.idempotency.IdempotencyConflictException}
 * mapped to HTTP 409 by {@link GlobalExceptionHandler}.
 */
@Slf4j
@RestController
@RequestMapping("/api/v1/orders")
@RequiredArgsConstructor
public class OrderController {

    private final IdempotencyService idempotencyService;
    private final OrderTransactionService orderTransactionService;
    private final RequestHashCalculator requestHashCalculator;

    @PostMapping
    public ResponseEntity<CreateOrderResponse> createOrder(
            @RequestHeader(EventConstants.IDEMPOTENCY_KEY_HEADER) String idempotencyKey,
            @Valid @RequestBody CreateOrderRequest request
    ) {
        String correlationId = request.getCorrelationId() != null ? request.getCorrelationId() : UUID.randomUUID().toString();
        // MDC fields are referenced in logback pattern for structured logging.
        MDC.put("correlationId", correlationId);
        MDC.put("customerId", String.valueOf(request.getCustomerId()));
        MDC.put("idempotencyKey", idempotencyKey);

        log.info("Order request accepted customerId={} idempotencyKey={}", request.getCustomerId(), idempotencyKey);
        log.debug("Order request body customerId={} items={}", request.getCustomerId(), request.getItems().size());

        String requestHash = requestHashCalculator.calculate(request);
        log.debug("Request hash calculated hash={}", requestHash);

        // May throw IdempotencyConflictException (409) when the key exists with a different request hash
        // or the original request is still PROCESSING; not caught here — see GlobalExceptionHandler.
        Optional<CreateOrderResponse> cached = idempotencyService.findCachedResponse(
                request.getCustomerId(),
                idempotencyKey,
                requestHash
        );
        if (cached.isPresent()) {
            // Same key + same body hash → return previously stored response.
            CreateOrderResponse response = cached.get();
            MDC.put("orderId", String.valueOf(response.getOrderId()));
            MDC.put("eventId", String.valueOf(response.getEventId()));
            return ResponseEntity.ok(response);
        }

        CreateOrderResponse created = orderTransactionService.createOrder(request, idempotencyKey, requestHash);
        MDC.put("orderId", String.valueOf(created.getOrderId()));
        MDC.put("eventId", String.valueOf(created.getEventId()));
        return ResponseEntity.status(HttpStatus.CREATED).body(created);
    }
}
