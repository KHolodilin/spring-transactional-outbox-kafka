package com.kholodilin.outbox.idempotency;

import tools.jackson.databind.ObjectMapper;
import com.kholodilin.outbox.events.CreateOrderResponse;
import com.kholodilin.outbox.events.IdempotencyStatus;
import com.kholodilin.outbox.logging.StructuredLogContext;
import com.kholodilin.outbox.persistence.IdempotencyKeyRow;
import com.kholodilin.outbox.persistence.IdempotencyR2dbcRepository;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;
import reactor.core.publisher.Mono;

/**
 * Resolves an existing idempotency row after {@code INSERT … ON CONFLICT DO NOTHING} returned empty.
 */
@Slf4j
@Service
@RequiredArgsConstructor
public class IdempotencyService {

    private final IdempotencyR2dbcRepository repository;
    private final ObjectMapper objectMapper;

    public Mono<CreateOrderResponse> findCachedResponse(Long customerId, String idempotencyKey, String requestHash) {
        return repository.findByCustomerIdAndKey(customerId, idempotencyKey)
                .flatMap(row -> resolve(row, customerId, idempotencyKey, requestHash))
                .switchIfEmpty(Mono.error(new IllegalStateException(
                        "Idempotency key conflicted but no usable row was found")));
    }

    private Mono<CreateOrderResponse> resolve(
            IdempotencyKeyRow row,
            Long customerId,
            String idempotencyKey,
            String requestHash
    ) {
        if (!row.requestHash().equals(requestHash)) {
            return Mono.error(new IdempotencyConflictException("Idempotency key reused with different request body"));
        }
        if (row.status() == IdempotencyStatus.COMPLETED && row.responseBody() != null) {
            try {
                CreateOrderResponse response = objectMapper.readValue(row.responseBody(), CreateOrderResponse.class);
                StructuredLogContext.putEventAction("idempotency.response.reused");
                log.info("Idempotent response returned customerId={} idempotencyKey={}", customerId, idempotencyKey);
                return Mono.just(response);
            } catch (Exception ex) {
                return Mono.error(new IllegalStateException("Failed to deserialize idempotent response", ex));
            }
        }
        if (row.status() == IdempotencyStatus.PROCESSING) {
            return Mono.error(new IdempotencyConflictException(
                    "Request with this idempotency key is already being processed"));
        }
        return Mono.error(new IllegalStateException(
                "Idempotency key conflicted but no usable row was found"));
    }
}
