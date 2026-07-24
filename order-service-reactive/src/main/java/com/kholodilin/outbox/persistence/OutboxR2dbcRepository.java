package com.kholodilin.outbox.persistence;

import tools.jackson.databind.ObjectMapper;
import com.kholodilin.outbox.events.EventEnvelope;
import com.kholodilin.outbox.events.OutboxStatus;
import io.r2dbc.spi.Readable;
import lombok.RequiredArgsConstructor;
import org.springframework.r2dbc.core.DatabaseClient;
import org.springframework.stereotype.Repository;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.time.Instant;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

/** R2DBC access to {@code outbox_events} for the publisher and recovery worker. */
@Repository
@RequiredArgsConstructor
public class OutboxR2dbcRepository {

    private final DatabaseClient databaseClient;
    private final ObjectMapper objectMapper;

    public Mono<Long> insertEvent(
            Long orderId,
            Long customerId,
            String eventType,
            String payload,
            String traceParent,
            Instant now
    ) {
        var spec = databaseClient.sql("""
                        INSERT INTO outbox_events (order_id, customer_id, event_type, payload, status, retry_count, trace_parent, created_at)
                        VALUES (:orderId, :customerId, :eventType, CAST(:payload AS jsonb), :status, 0, :traceParent, :createdAt)
                        RETURNING id
                        """)
                .bind("orderId", orderId)
                .bind("customerId", customerId)
                .bind("eventType", eventType)
                .bind("payload", payload)
                .bind("status", OutboxStatus.NEW.getCode())
                .bind("createdAt", toOffset(now));
        if (traceParent == null) {
            spec = spec.bindNull("traceParent", String.class);
        } else {
            spec = spec.bind("traceParent", traceParent);
        }
        return spec.map((row, metadata) -> row.get("id", Long.class)).one();
    }

    public Flux<OutboxRow> claimByIds(List<Long> ids, String lockedBy, Instant lockedUntil) {
        if (ids.isEmpty()) {
            return Flux.empty();
        }
        String placeholders = namedPlaceholders("id", ids.size());
        DatabaseClient.GenericExecuteSpec spec = databaseClient.sql("""
                        UPDATE outbox_events
                        SET status = :status, locked_by = :lockedBy, locked_until = :lockedUntil
                        WHERE id IN (%s)
                          AND status < :archiveThreshold
                          AND (locked_until IS NULL OR locked_until < NOW())
                        RETURNING id, order_id, customer_id, event_type, payload::text AS payload, status, retry_count, trace_parent
                        """.formatted(placeholders))
                .bind("status", OutboxStatus.PROCESSING.getCode())
                .bind("lockedBy", lockedBy)
                .bind("lockedUntil", toOffset(lockedUntil))
                .bind("archiveThreshold", OutboxStatus.ARCHIVE_THRESHOLD);
        spec = bindIds(spec, "id", ids);
        return spec.map((row, metadata) -> mapRow(row)).all();
    }

    public Mono<Void> markSent(List<Long> ids, Instant sentAt) {
        if (ids.isEmpty()) {
            return Mono.empty();
        }
        String placeholders = namedPlaceholders("id", ids.size());
        DatabaseClient.GenericExecuteSpec spec = databaseClient.sql("""
                        UPDATE outbox_events
                        SET status = :status, sent_at = :sentAt, locked_by = NULL, locked_until = NULL
                        WHERE id IN (%s)
                        """.formatted(placeholders))
                .bind("status", OutboxStatus.SENT.getCode())
                .bind("sentAt", toOffset(sentAt));
        spec = bindIds(spec, "id", ids);
        return spec.fetch().rowsUpdated().then();
    }

    public Mono<Void> markFailed(Long id, int retryCount, OutboxStatus status) {
        return databaseClient.sql("""
                        UPDATE outbox_events
                        SET status = :status, retry_count = :retryCount, locked_by = NULL, locked_until = NULL
                        WHERE id = :id
                        """)
                .bind("status", status.getCode())
                .bind("retryCount", retryCount)
                .bind("id", id)
                .fetch()
                .rowsUpdated()
                .then();
    }

    public Flux<Long> claimRecoverableIds(int batchSize, String lockedBy, Instant lockedUntil) {
        return databaseClient.sql("""
                        WITH candidates AS (
                            SELECT id
                            FROM outbox_events
                            WHERE status < :archiveThreshold
                              AND (locked_until IS NULL OR locked_until < NOW())
                            ORDER BY id
                            LIMIT :batchSize
                            FOR UPDATE SKIP LOCKED
                        )
                        UPDATE outbox_events AS o
                        SET locked_by = :lockedBy,
                            locked_until = :lockedUntil
                        FROM candidates AS c
                        WHERE o.id = c.id
                        RETURNING o.id
                        """)
                .bind("archiveThreshold", OutboxStatus.ARCHIVE_THRESHOLD)
                .bind("batchSize", batchSize)
                .bind("lockedBy", lockedBy)
                .bind("lockedUntil", toOffset(lockedUntil))
                .map((row, metadata) -> row.get("id", Long.class))
                .all();
    }

    public Mono<Void> clearLease(List<Long> ids) {
        if (ids.isEmpty()) {
            return Mono.empty();
        }
        String placeholders = namedPlaceholders("id", ids.size());
        DatabaseClient.GenericExecuteSpec spec = databaseClient.sql("""
                        UPDATE outbox_events SET locked_by = NULL, locked_until = NULL WHERE id IN (%s)
                        """.formatted(placeholders));
        spec = bindIds(spec, "id", ids);
        return spec.fetch().rowsUpdated().then();
    }

    public Flux<Long> findReenqueueableIds(List<Long> ids) {
        if (ids.isEmpty()) {
            return Flux.empty();
        }
        String placeholders = namedPlaceholders("id", ids.size());
        DatabaseClient.GenericExecuteSpec spec = databaseClient.sql("""
                        SELECT id FROM outbox_events
                        WHERE id IN (%s)
                          AND status < :archiveThreshold
                          AND (locked_until IS NULL OR locked_until < NOW())
                        """.formatted(placeholders))
                .bind("archiveThreshold", OutboxStatus.ARCHIVE_THRESHOLD);
        spec = bindIds(spec, "id", ids);
        return spec.map((row, metadata) -> row.get("id", Long.class)).all();
    }

    public Mono<Long> countActivePending() {
        return databaseClient.sql("""
                        SELECT COUNT(*) AS cnt FROM outbox_events WHERE status < :archiveThreshold
                        """)
                .bind("archiveThreshold", OutboxStatus.ARCHIVE_THRESHOLD)
                .map((row, metadata) -> row.get("cnt", Long.class))
                .one()
                .defaultIfEmpty(0L);
    }

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

    private static OutboxRow mapRow(Readable row) {
        return new OutboxRow(
                row.get("id", Long.class),
                row.get("order_id", Long.class),
                row.get("customer_id", Long.class),
                row.get("event_type", String.class),
                row.get("payload", String.class),
                OutboxStatus.fromCode(row.get("status", Integer.class)),
                row.get("retry_count", Integer.class),
                row.get("trace_parent", String.class)
        );
    }

    private static String namedPlaceholders(String prefix, int size) {
        return IntStream.range(0, size)
                .mapToObj(i -> ":" + prefix + i)
                .collect(Collectors.joining(","));
    }

    private static DatabaseClient.GenericExecuteSpec bindIds(
            DatabaseClient.GenericExecuteSpec spec,
            String prefix,
            Collection<Long> ids
    ) {
        int i = 0;
        for (Long id : ids) {
            spec = spec.bind(prefix + i, id);
            i++;
        }
        return spec;
    }

    private static OffsetDateTime toOffset(Instant instant) {
        return instant.atOffset(ZoneOffset.UTC);
    }
}
