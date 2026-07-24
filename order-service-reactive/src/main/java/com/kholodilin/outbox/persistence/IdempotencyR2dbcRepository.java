package com.kholodilin.outbox.persistence;

import com.kholodilin.outbox.events.IdempotencyStatus;
import io.r2dbc.spi.Readable;
import lombok.RequiredArgsConstructor;
import org.springframework.r2dbc.core.DatabaseClient;
import org.springframework.stereotype.Repository;
import reactor.core.publisher.Mono;

import java.time.Instant;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;

/** R2DBC access to hash-partitioned {@code idempotency_keys}. */
@Repository
@RequiredArgsConstructor
public class IdempotencyR2dbcRepository {

    private final DatabaseClient databaseClient;

    public Mono<IdempotencyKeyRow> findByCustomerIdAndKey(Long customerId, String idempotencyKey) {
        return databaseClient.sql("""
                        SELECT customer_id, id, idempotency_key, request_hash, status, response_body::text AS response_body,
                               created_at, updated_at
                        FROM idempotency_keys
                        WHERE customer_id = :customerId AND idempotency_key = :idempotencyKey
                        """)
                .bind("customerId", customerId)
                .bind("idempotencyKey", idempotencyKey)
                .map((row, metadata) -> mapRow(row))
                .one();
    }

    public Mono<Long> tryInsertProcessing(Long customerId, String idempotencyKey, String requestHash, Instant now) {
        OffsetDateTime ts = toOffset(now);
        return databaseClient.sql("""
                        INSERT INTO idempotency_keys (customer_id, idempotency_key, request_hash, status, created_at, updated_at)
                        VALUES (:customerId, :idempotencyKey, :requestHash, :status, :createdAt, :updatedAt)
                        ON CONFLICT (customer_id, idempotency_key) DO NOTHING
                        RETURNING id
                        """)
                .bind("customerId", customerId)
                .bind("idempotencyKey", idempotencyKey)
                .bind("requestHash", requestHash)
                .bind("status", IdempotencyStatus.PROCESSING.getCode())
                .bind("createdAt", ts)
                .bind("updatedAt", ts)
                .map((row, metadata) -> row.get("id", Long.class))
                .one();
    }

    public Mono<Void> complete(Long customerId, String idempotencyKey, String responseBody, Instant now) {
        return databaseClient.sql("""
                        UPDATE idempotency_keys
                        SET status = :status, response_body = CAST(:responseBody AS jsonb), updated_at = :updatedAt
                        WHERE customer_id = :customerId AND idempotency_key = :idempotencyKey
                        """)
                .bind("status", IdempotencyStatus.COMPLETED.getCode())
                .bind("responseBody", responseBody)
                .bind("updatedAt", toOffset(now))
                .bind("customerId", customerId)
                .bind("idempotencyKey", idempotencyKey)
                .fetch()
                .rowsUpdated()
                .then();
    }

    private static IdempotencyKeyRow mapRow(Readable row) {
        return new IdempotencyKeyRow(
                row.get("customer_id", Long.class),
                row.get("id", Long.class),
                row.get("idempotency_key", String.class),
                row.get("request_hash", String.class),
                IdempotencyStatus.fromCode(row.get("status", Integer.class)),
                row.get("response_body", String.class),
                toInstant(row.get("created_at", OffsetDateTime.class)),
                toInstant(row.get("updated_at", OffsetDateTime.class))
        );
    }

    private static OffsetDateTime toOffset(Instant instant) {
        return instant.atOffset(ZoneOffset.UTC);
    }

    private static Instant toInstant(OffsetDateTime value) {
        return value == null ? null : value.toInstant();
    }
}
