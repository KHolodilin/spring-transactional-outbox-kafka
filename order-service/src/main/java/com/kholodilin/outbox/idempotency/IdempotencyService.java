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
 * Resolves an existing idempotency row after {@code INSERT … ON CONFLICT DO NOTHING} returned no id.
 * <p>
 * Same key + same body hash + {@code COMPLETED} → return stored response (HTTP 200).
 * Same key + different hash → {@link IdempotencyConflictException} (HTTP 409).
 * Same key still {@code PROCESSING} → {@link IdempotencyConflictException} (HTTP 409).
 */
@Slf4j
@Service
@RequiredArgsConstructor
public class IdempotencyService {

    private final IdempotencyJdbcRepository repository;
    private final ObjectMapper objectMapper;

    /**
     * Maps an already-persisted idempotency row to a cached response or a conflict.
     *
     * @param customerId     customer scope for the key
     * @param idempotencyKey client-supplied {@code Idempotency-Key}
     * @param requestHash    hash of the current request body
     * @return cached response when the key exists, is {@code COMPLETED}, and the hash matches;
     *         empty when the key is missing or not in a reusable {@code COMPLETED} state
     * @throws IdempotencyConflictException when the key exists with a different hash, or is still {@code PROCESSING}
     */
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
