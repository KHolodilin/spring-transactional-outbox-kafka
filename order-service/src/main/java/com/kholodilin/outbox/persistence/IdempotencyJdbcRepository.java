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

    public void insertProcessing(Long customerId, String idempotencyKey, String requestHash, Instant now) {
        jdbcTemplate.update(
                """
                        INSERT INTO idempotency_keys (customer_id, idempotency_key, request_hash, status, created_at, updated_at)
                        VALUES (?, ?, ?, ?, ?, ?)
                        """,
                customerId,
                idempotencyKey,
                requestHash,
                IdempotencyStatus.PROCESSING.getCode(),
                Timestamp.from(now),
                Timestamp.from(now)
        );
    }

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
