package com.kholodilin.outbox.persistence;

import com.kholodilin.outbox.events.IdempotencyStatus;
import com.kholodilin.outbox.persistence.entity.IdempotencyKeyEntity;
import lombok.RequiredArgsConstructor;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.RowMapper;
import org.springframework.stereotype.Repository;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.time.Instant;
import java.util.Optional;

/** JDBC access to hash-partitioned {@code idempotency_keys} table. */
@Repository
@RequiredArgsConstructor
public class IdempotencyJdbcRepository {

    private static final RowMapper<IdempotencyKeyEntity> ROW_MAPPER = new RowMapper<>() {
        @Override
        public IdempotencyKeyEntity mapRow(ResultSet rs, int rowNum) throws SQLException {
            IdempotencyKeyEntity entity = new IdempotencyKeyEntity();
            entity.setCustomerId(rs.getLong("customer_id"));
            entity.setId(rs.getLong("id"));
            entity.setIdempotencyKey(rs.getString("idempotency_key"));
            entity.setRequestHash(rs.getString("request_hash"));
            entity.setStatus(IdempotencyStatus.fromCode(rs.getInt("status")));
            entity.setResponseBody(rs.getString("response_body"));
            entity.setCreatedAt(rs.getTimestamp("created_at").toInstant());
            entity.setUpdatedAt(rs.getTimestamp("updated_at").toInstant());
            return entity;
        }
    };

    private final JdbcTemplate jdbcTemplate;

    /**
     * Finds an idempotency record by customer + key.
     *
     * @param customerId     partition key / customer scope
     * @param idempotencyKey client {@code Idempotency-Key}
     * @return entity when present
     */
    public Optional<IdempotencyKeyEntity> findByCustomerIdAndKey(Long customerId, String idempotencyKey) {
        var list = jdbcTemplate.query(
                """
                        SELECT customer_id, id, idempotency_key, request_hash, status, response_body, created_at, updated_at
                        FROM idempotency_keys
                        WHERE customer_id = ? AND idempotency_key = ?
                        """,
                ROW_MAPPER,
                customerId,
                idempotencyKey
        );
        return list.stream().findFirst();
    }

    /**
     * Inserts a {@link IdempotencyStatus#PROCESSING} row at the start of order creation.
     * <p>
     * Concurrent inserts with the same key fail on the unique constraint and surface as a DB error;
     * callers rely on the pre-check in {@link com.kholodilin.outbox.idempotency.IdempotencyService}.
     *
     * @param customerId     customer scope
     * @param idempotencyKey client key
     * @param requestHash    SHA-256 of the request body
     * @param now            created_at / updated_at
     * @return generated {@code idempotency_keys.id}
     */
    public long insertProcessing(Long customerId, String idempotencyKey, String requestHash, Instant now) {
        Long id = jdbcTemplate.queryForObject(
                """
                        INSERT INTO idempotency_keys (customer_id, idempotency_key, request_hash, status, created_at, updated_at)
                        VALUES (?, ?, ?, ?, ?, ?)
                        RETURNING id
                        """,
                Long.class,
                customerId,
                idempotencyKey,
                requestHash,
                IdempotencyStatus.PROCESSING.getCode(),
                Timestamp.from(now),
                Timestamp.from(now)
        );
        return id;
    }

    /**
     * Marks the key {@link IdempotencyStatus#COMPLETED} and stores the JSON response for replays.
     *
     * @param customerId     customer scope
     * @param idempotencyKey client key
     * @param responseBody   serialized {@link com.kholodilin.outbox.events.CreateOrderResponse}
     * @param now            updated_at
     */
    public void complete(Long customerId, String idempotencyKey, String responseBody, Instant now) {
        jdbcTemplate.update(
                """
                        UPDATE idempotency_keys
                        SET status = ?, response_body = ?::jsonb, updated_at = ?
                        WHERE customer_id = ? AND idempotency_key = ?
                        """,
                IdempotencyStatus.COMPLETED.getCode(),
                responseBody,
                Timestamp.from(now),
                customerId,
                idempotencyKey
        );
    }
}
