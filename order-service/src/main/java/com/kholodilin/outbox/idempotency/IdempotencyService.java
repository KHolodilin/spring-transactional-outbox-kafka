package com.kholodilin.outbox.idempotency;

import tools.jackson.databind.ObjectMapper;
import com.kholodilin.outbox.events.CreateOrderResponse;
import com.kholodilin.outbox.events.IdempotencyStatus;
import com.kholodilin.outbox.logging.StructuredLogContext;
import com.kholodilin.outbox.persistence.IdempotencyJdbcRepository;
import com.kholodilin.outbox.persistence.entity.IdempotencyKeyEntity;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;

import java.util.Optional;

/**
 * Read-only idempotency check before starting a new database transaction.
 * <p>
 * Same key + same body hash → return stored response (HTTP 200).
 * Same key + different hash → {@link IdempotencyConflictException} (HTTP 409).
 */
@Slf4j
@Service
@RequiredArgsConstructor
public class IdempotencyService {

    private final IdempotencyJdbcRepository repository;
    private final ObjectMapper objectMapper;

    public Optional<CreateOrderResponse> findCachedResponse(Long customerId, String idempotencyKey, String requestHash) {
        Optional<IdempotencyKeyEntity> existing = repository.findByCustomerIdAndKey(customerId, idempotencyKey);
        if (existing.isEmpty()) {
            return Optional.empty();
        }
        IdempotencyKeyEntity record = existing.get();
        if (!record.getRequestHash().equals(requestHash)) {
            throw new IdempotencyConflictException("Idempotency key reused with different request body");
        }
        if (record.getStatus() == IdempotencyStatus.COMPLETED && record.getResponseBody() != null) {
            try {
                CreateOrderResponse response = objectMapper.readValue(record.getResponseBody(), CreateOrderResponse.class);
                StructuredLogContext.putEventAction("idempotency.response.reused");
                log.info("Idempotent response returned customerId={} idempotencyKey={}", customerId, idempotencyKey);
                return Optional.of(response);
            } catch (Exception ex) {
                throw new IllegalStateException("Failed to deserialize idempotent response", ex);
            }
        }
        if (record.getStatus() == IdempotencyStatus.PROCESSING) {
            throw new IdempotencyConflictException("Request with this idempotency key is already being processed");
        }
        return Optional.empty();
    }
}
