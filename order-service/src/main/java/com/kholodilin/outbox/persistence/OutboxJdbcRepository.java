package com.kholodilin.outbox.persistence;

import tools.jackson.databind.ObjectMapper;
import com.kholodilin.outbox.events.EventEnvelope;
import com.kholodilin.outbox.events.OutboxStatus;
import com.kholodilin.outbox.tracing.OutboxTracing;
import lombok.RequiredArgsConstructor;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.RowMapper;
import org.springframework.stereotype.Repository;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;

/**
 * JDBC access to {@code outbox_events} for the publisher and recovery worker.
 * <p>
 * Uses batch-style updates and {@code FOR UPDATE SKIP LOCKED} for multi-pod coordination.
 * Hot-path reads use active rows only ({@code status < 100}); finals move to archive ({@code status >= 100}).
 */
@Repository
@RequiredArgsConstructor
public class OutboxJdbcRepository {

    private static final String OUTBOX_ROW_COLUMNS =
            "id, order_id, customer_id, event_type, payload::text AS payload, status, retry_count, trace_parent";

    private static final RowMapper<OutboxRow> ROW_MAPPER = new RowMapper<>() {
        @Override
        public OutboxRow mapRow(ResultSet rs, int rowNum) throws SQLException {
            return OutboxRow.builder()
                    .id(rs.getLong("id"))
                    .orderId(rs.getLong("order_id"))
                    .customerId(rs.getLong("customer_id"))
                    .eventType(rs.getString("event_type"))
                    .payload(rs.getString("payload"))
                    .status(OutboxStatus.fromCode(rs.getInt("status")))
                    .retryCount(rs.getInt("retry_count"))
                    .traceParent(rs.getString("trace_parent"))
                    .build();
        }
    };

    private final JdbcTemplate jdbcTemplate;
    private final ObjectMapper objectMapper;
    private final OutboxTracing outboxTracing;

    public long insertEvent(Long orderId, Long customerId, String eventType, String payload, String traceParent, Instant now) {
        return outboxTracing.observe("outbox.save", () -> {
            Long id = jdbcTemplate.queryForObject(
                    """
                            INSERT INTO outbox_events (order_id, customer_id, event_type, payload, status, retry_count, trace_parent, created_at)
                            VALUES (?, ?, ?, ?::jsonb, ?, 0, ?, ?)
                            RETURNING id
                            """,
                    Long.class,
                    orderId,
                    customerId,
                    eventType,
                    payload,
                    OutboxStatus.NEW.getCode(),
                    traceParent,
                    Timestamp.from(now)
            );
            return id;
        });
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
        params.add(OutboxStatus.PROCESSING.getCode());
        params.add(lockedBy);
        params.add(Timestamp.from(lockedUntil));
        params.addAll(ids);
        params.add(OutboxStatus.ARCHIVE_THRESHOLD);

        return jdbcTemplate.query(
                """
                        UPDATE outbox_events
                        SET status = ?, locked_by = ?, locked_until = ?
                        WHERE id IN (%s)
                          AND status < ?
                          AND (locked_until IS NULL OR locked_until < NOW())
                        RETURNING %s
                        """.formatted(placeholders, OUTBOX_ROW_COLUMNS),
                ROW_MAPPER,
                params.toArray()
        );
    }

    public void markSent(List<Long> ids, Instant sentAt) {
        if (ids.isEmpty()) {
            return;
        }
        outboxTracing.observe("outbox.mark.sent", () -> {
            String placeholders = String.join(",", ids.stream().map(id -> "?").toList());
            List<Object> params = new ArrayList<>();
            params.add(OutboxStatus.SENT.getCode());
            params.add(Timestamp.from(sentAt));
            params.addAll(ids);
            jdbcTemplate.update(
                    """
                            UPDATE outbox_events
                            SET status = ?, sent_at = ?, locked_by = NULL, locked_until = NULL
                            WHERE id IN (%s)
                            """.formatted(placeholders),
                    params.toArray()
            );
        });
    }

    public void markFailed(Long id, int retryCount, OutboxStatus status) {
        outboxTracing.observe("outbox.retry", () -> jdbcTemplate.update(
                """
                        UPDATE outbox_events
                        SET status = ?, retry_count = ?, locked_by = NULL, locked_until = NULL
                        WHERE id = ?
                        """,
                status.getCode(),
                retryCount,
                id
        ));
    }

    /** Atomically selects recoverable rows and applies a short-lived lease; returns claimed ids. */
    public List<Long> claimRecoverableIds(int batchSize, String lockedBy, Instant lockedUntil) {
        return jdbcTemplate.query(
                """
                        WITH candidates AS (
                            SELECT id
                            FROM outbox_events
                            WHERE status < ?
                              AND (locked_until IS NULL OR locked_until < NOW())
                            ORDER BY id
                            LIMIT ?
                            FOR UPDATE SKIP LOCKED
                        )
                        UPDATE outbox_events AS o
                        SET locked_by = ?,
                            locked_until = ?
                        FROM candidates AS c
                        WHERE o.id = c.id
                        RETURNING o.id
                        """,
                (rs, rowNum) -> rs.getLong("id"),
                OutboxStatus.ARCHIVE_THRESHOLD,
                batchSize,
                lockedBy,
                Timestamp.from(lockedUntil)
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

    /** Returns ids that are still active and not leased — safe to re-enqueue after a failed claim. */
    public List<Long> findReenqueueableIds(List<Long> ids) {
        if (ids.isEmpty()) {
            return List.of();
        }
        String placeholders = String.join(",", ids.stream().map(id -> "?").toList());
        List<Object> params = new ArrayList<>(ids);
        params.add(OutboxStatus.ARCHIVE_THRESHOLD);
        return jdbcTemplate.query(
                """
                        SELECT id FROM outbox_events
                        WHERE id IN (%s)
                          AND status < ?
                          AND (locked_until IS NULL OR locked_until < NOW())
                        """.formatted(placeholders),
                (rs, rowNum) -> rs.getLong("id"),
                params.toArray()
        );
    }

    public long countActivePending() {
        Long count = jdbcTemplate.queryForObject(
                """
                        SELECT COUNT(*) FROM outbox_events
                        WHERE status < ?
                        """,
                Long.class,
                OutboxStatus.ARCHIVE_THRESHOLD
        );
        return count == null ? 0L : count;
    }

    public Optional<OutboxRow> findById(long id) {
        List<OutboxRow> rows = jdbcTemplate.query(
                """
                        SELECT %s
                        FROM outbox_events
                        WHERE id = ?
                        """.formatted(OUTBOX_ROW_COLUMNS),
                ROW_MAPPER,
                id
        );
        return rows.stream().findFirst();
    }

    public EventEnvelope toEnvelope(OutboxRow row, String correlationId) {
        try {
            @SuppressWarnings("unchecked")
            Map<String, Object> payload = objectMapper.readValue(row.getPayload(), Map.class);
            return EventEnvelope.builder()
                    .eventId(row.getId())
                    .orderId(row.getOrderId())
                    .customerId(row.getCustomerId())
                    .eventType(row.getEventType())
                    .payload(payload)
                    .correlationId(correlationId)
                    .traceParent(row.getTraceParent())
                    .occurredAt(Instant.now())
                    .build();
        } catch (Exception ex) {
            throw new IllegalStateException("Failed to parse outbox payload for id=" + row.getId(), ex);
        }
    }
}
