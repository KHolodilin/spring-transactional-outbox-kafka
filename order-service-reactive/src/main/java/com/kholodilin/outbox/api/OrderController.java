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
import reactor.core.publisher.Mono;

import java.util.UUID;

/** Reactive HTTP entry point for order creation with idempotency support. */
@Slf4j
@RestController
@RequestMapping("/api/v1/orders")
@RequiredArgsConstructor
public class OrderController {

    private final OrderTransactionService orderTransactionService;
    private final RequestHashCalculator requestHashCalculator;

    @PostMapping
    public Mono<ResponseEntity<CreateOrderResponse>> createOrder(
            @RequestHeader(EventConstants.IDEMPOTENCY_KEY_HEADER) String idempotencyKey,
            @Valid @RequestBody CreateOrderRequest request
    ) {
        String correlationId = request.correlationId() != null ? request.correlationId() : UUID.randomUUID().toString();
        StructuredLogContext.putCorrelation(correlationId, request.customerId(), idempotencyKey);
        StructuredLogContext.putEventAction("http.request.accepted");

        log.info("Order request accepted customerId={} idempotencyKey={}", request.customerId(), idempotencyKey);

        String requestHash = requestHashCalculator.calculate(request);
        return orderTransactionService.createOrder(request, idempotencyKey, requestHash)
                .map(this::toResponse)
                .doFinally(signal -> StructuredLogContext.clearRequestContext());
    }

    private ResponseEntity<CreateOrderResponse> toResponse(OrderCreateOutcome outcome) {
        CreateOrderResponse response = outcome.response();
        StructuredLogContext.putOrderFields(response.orderId(), response.eventId());
        StructuredLogContext.putEventAction("http.request.completed");
        if (outcome.created()) {
            return ResponseEntity.status(HttpStatus.CREATED).body(response);
        }
        return ResponseEntity.ok(response);
    }
}
