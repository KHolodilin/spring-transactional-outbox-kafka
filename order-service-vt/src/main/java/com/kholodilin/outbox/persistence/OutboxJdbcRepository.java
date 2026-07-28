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
            return new OutboxRow(
                    rs.getLong("id"),
                    rs.getLong("order_id"),
                    rs.getLong("customer_id"),
                    rs.getString("event_type"),
                    rs.getString("payload"),
                    OutboxStatus.fromCode(rs.getInt("status")),
                    rs.getInt("retry_count"),
                    rs.getString("trace_parent")
            );
        }
    };

    private final JdbcTemplate jdbcTemplate;
    private final ObjectMapper objectMapper;
    private final OutboxTracing outboxTracing;

    /**
     * Inserts a NEW outbox row and returns its generated id.
     * <p>
     * Wrapped in an {@code outbox.save} span. {@code traceParent} may be {@code null}
     * when tracing is disabled.
     *
     * @param orderId     related order id
     * @param customerId  partition / ordering key for Kafka
     * @param eventType   business event type
     * @param payload     JSON body stored as jsonb
     * @param traceParent W3C traceparent captured at insert time, or {@code null}
     * @param now         created_at timestamp
     * @return generated {@code outbox_events.id}
     */
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
     *
     * @param ids         candidate event ids from the in-memory queue
     * @param lockedBy    this instance id written to {@code locked_by}
     * @param lockedUntil lease expiry for the claim
     * @return rows successfully claimed for publishing (may be a subset of {@code ids})
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

    /**
     * Marks claimed rows as SENT (archive partition) and clears the lease.
     * <p>
     * Wrapped in an {@code outbox.mark.sent} span. No-op for an empty id list.
     *
     * @param ids    outbox event ids that were successfully published
     * @param sentAt timestamp written to {@code sent_at}
     */
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

    /**
     * Records a failed publish attempt ({@link OutboxStatus#FAILED} or {@link OutboxStatus#DEAD})
     * and clears the lease so recovery can retry.
     * <p>
     * Wrapped in an {@code outbox.retry} span.
     *
     * @param id         outbox event id
     * @param retryCount updated retry counter stored on the row
     * @param status     next status after the failure
     */
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

    /**
     * Atomically selects recoverable rows and applies a short-lived lease; returns claimed ids.
     *
     * @param batchSize   max rows to claim in one recovery tick
     * @param lockedBy    this instance id
     * @param lockedUntil lease expiry (cleared again before enqueue)
     * @return claimed outbox ids, possibly empty
     */
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

    /**
     * Clears {@code locked_by} / {@code locked_until} for the given ids.
     * <p>
     * Used by recovery after claiming ids so the publisher can re-claim immediately.
     *
     * @param ids outbox event ids whose lease should be released
     */
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

    /**
     * Returns ids that are still active and not leased — safe to re-enqueue after a failed claim.
     *
     * @param ids candidate ids that were polled but not claimed
     * @return subset still eligible for enqueue
     */
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

    /**
     * Counts active (non-archive) outbox rows for health / backlog checks.
     *
     * @return number of rows with {@code status <} archive threshold
     */
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

    /**
     * Loads a single outbox row by id (any partition / status).
     *
     * @param id outbox event id
     * @return row when present
     */
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

    /**
     * Maps a persisted outbox row to the Kafka {@link EventEnvelope} contract.
     *
     * @param row           claimed outbox row
     * @param correlationId optional correlation id extracted from the payload JSON
     * @return envelope ready for {@link com.kholodilin.outbox.publisher.KafkaBatchPublisher}
     * @throws IllegalStateException when payload JSON cannot be parsed
     */
    public EventEnvelope toEnvelope(OutboxRow row, String correlationId) {
        try {
            @SuppressWarnings("unchecked")
            Map<String, Object> payload = objectMapper.readValue(row.payload(), Map.class);
            return new EventEnvelope(
                    row.id(),
                    row.orderId(),
                    row.customerId(),
                    row.eventType(),
                    payload,
                    correlationId,
                    Instant.now(),
                    row.traceParent()
            );
        } catch (Exception ex) {
            throw new IllegalStateException("Failed to parse outbox payload for id=" + row.id(), ex);
        }
    }
}
