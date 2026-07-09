package com.kholodilin.outbox.persistence;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.kholodilin.outbox.events.EventEnvelope;
import com.kholodilin.outbox.events.OutboxStatus;
import com.kholodilin.outbox.events.PartitionState;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.RowMapper;
import org.springframework.stereotype.Repository;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * JDBC access to {@code outbox_events} for the publisher and recovery worker.
 * <p>
 * Uses batch-style updates and {@code FOR UPDATE SKIP LOCKED} for multi-pod coordination.
 * Hot-path reads for publishing go through ACTIVE rows only; successful events move to ARCHIVE.
 */
@Repository
public class OutboxJdbcRepository {

    /** Lightweight row loaded for publishing — payload kept as JSON string until send time. */
    public record OutboxRow(
            Long id,
            Long orderId,
            Long customerId,
            String eventType,
            String payload,
            OutboxStatus status,
            int retryCount
    ) {
    }

    private static final RowMapper<OutboxRow> ROW_MAPPER = new RowMapper<>() {
        @Override
        public OutboxRow mapRow(ResultSet rs, int rowNum) throws SQLException {
            return new OutboxRow(
                    rs.getLong("id"),
                    rs.getLong("order_id"),
                    rs.getLong("customer_id"),
                    rs.getString("event_type"),
                    rs.getString("payload"),
                    OutboxStatus.valueOf(rs.getString("status")),
                    rs.getInt("retry_count")
            );
        }
    };

    private final JdbcTemplate jdbcTemplate;
    private final ObjectMapper objectMapper;

    public OutboxJdbcRepository(JdbcTemplate jdbcTemplate, ObjectMapper objectMapper) {
        this.jdbcTemplate = jdbcTemplate;
        this.objectMapper = objectMapper;
    }

    public long insertEvent(Long orderId, Long customerId, String eventType, String payload, Instant now) {
        Long id = jdbcTemplate.queryForObject(
                """
                        INSERT INTO outbox_events (order_id, customer_id, event_type, payload, status, partition_state, retry_count, created_at)
                        VALUES (?, ?, ?, ?::jsonb, ?, ?, 0, ?)
                        RETURNING id
                        """,
                Long.class,
                orderId,
                customerId,
                eventType,
                payload,
                OutboxStatus.NEW.name(),
                PartitionState.ACTIVE.name(),
                now
        );
        return id;
    }

    /**
     * Atomically moves eligible rows to PROCESSING and returns their payloads.
     * Rows already claimed by another pod or still leased are skipped.
     */
    public List<OutboxRow> claimByIds(List<Long> ids, String lockedBy, Instant lockedUntil) {
        if (ids.isEmpty()) {
            return List.of();
        }
        String placeholders = String.join(",", ids.stream().map(id -> "?").toList());
        List<Object> params = new ArrayList<>();
        params.add(OutboxStatus.PROCESSING.name());
        params.add(lockedBy);
        params.add(lockedUntil);
        params.addAll(ids);

        return jdbcTemplate.query(
                """
                        UPDATE outbox_events
                        SET status = ?, locked_by = ?, locked_until = ?
                        WHERE id IN (%s)
                          AND partition_state = 'ACTIVE'
                          AND status IN ('NEW', 'FAILED')
                          AND (locked_until IS NULL OR locked_until < NOW())
                        RETURNING id, order_id, customer_id, event_type, payload::text AS payload, status, retry_count
                        """.formatted(placeholders),
                ROW_MAPPER,
                params.toArray()
        );
    }

    public void markSent(List<Long> ids, Instant sentAt) {
        if (ids.isEmpty()) {
            return;
        }
        String placeholders = String.join(",", ids.stream().map(id -> "?").toList());
        List<Object> params = new ArrayList<>();
        params.add(OutboxStatus.SENT.name());
        params.add(PartitionState.ARCHIVE.name());
        params.add(sentAt);
        params.addAll(ids);
        jdbcTemplate.update(
                """
                        UPDATE outbox_events
                        SET status = ?, partition_state = ?, sent_at = ?, locked_by = NULL, locked_until = NULL
                        WHERE id IN (%s)
                        """.formatted(placeholders),
                params.toArray()
        );
    }

    public void markFailed(Long id, int retryCount, OutboxStatus status) {
        jdbcTemplate.update(
                """
                        UPDATE outbox_events
                        SET status = ?, retry_count = ?, locked_by = NULL, locked_until = NULL
                        WHERE id = ?
                        """,
                status.name(),
                retryCount,
                id
        );
    }

    /** Selects stale NEW/FAILED events for recovery; locks rows until the transaction ends. */
    public List<Long> findRecoverableIds(int batchSize) {
        return jdbcTemplate.query(
                """
                        SELECT id FROM outbox_events
                        WHERE partition_state = 'ACTIVE'
                          AND status IN ('NEW', 'FAILED')
                          AND (locked_until IS NULL OR locked_until < NOW())
                        ORDER BY id
                        LIMIT ?
                        FOR UPDATE SKIP LOCKED
                        """,
                (rs, rowNum) -> rs.getLong("id"),
                batchSize
        );
    }

    public void setLease(List<Long> ids, String lockedBy, Instant lockedUntil) {
        if (ids.isEmpty()) {
            return;
        }
        String placeholders = String.join(",", ids.stream().map(id -> "?").toList());
        List<Object> params = new ArrayList<>();
        params.add(lockedBy);
        params.add(lockedUntil);
        params.addAll(ids);
        jdbcTemplate.update(
                """
                        UPDATE outbox_events SET locked_by = ?, locked_until = ? WHERE id IN (%s)
                        """.formatted(placeholders),
                params.toArray()
        );
    }

    public void clearLease(List<Long> ids) {
        if (ids.isEmpty()) {
            return;
        }
        String placeholders = String.join(",", ids.stream().map(id -> "?").toList());
        jdbcTemplate.update(
                """
                        UPDATE outbox_events SET locked_by = NULL, locked_until = NULL WHERE id IN (%s)
                        """.formatted(placeholders),
                ids.toArray()
        );
    }

    public long countActivePending() {
        Long count = jdbcTemplate.queryForObject(
                """
                        SELECT COUNT(*) FROM outbox_events
                        WHERE partition_state = 'ACTIVE' AND status IN ('NEW', 'FAILED', 'PROCESSING')
                        """,
                Long.class
        );
        return count == null ? 0L : count;
    }

    public EventEnvelope toEnvelope(OutboxRow row, String correlationId) {
        try {
            @SuppressWarnings("unchecked")
            Map<String, Object> payload = objectMapper.readValue(row.payload(), Map.class);
            return EventEnvelope.builder()
                    .eventId(row.id())
                    .orderId(row.orderId())
                    .customerId(row.customerId())
                    .eventType(row.eventType())
                    .payload(payload)
                    .correlationId(correlationId)
                    .occurredAt(Instant.now())
                    .build();
        } catch (Exception ex) {
            throw new IllegalStateException("Failed to parse outbox payload for id=" + row.id(), ex);
        }
    }
}
