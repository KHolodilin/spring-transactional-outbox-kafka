package com.kholodilin.outbox.persistence;

import tools.jackson.databind.json.JsonMapper;
import com.kholodilin.outbox.events.EventEnvelope;
import com.kholodilin.outbox.events.OutboxStatus;
import com.kholodilin.outbox.tracing.OutboxTracing;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.RowMapper;

import java.time.Instant;
import java.util.List;
import java.util.Optional;
import java.util.function.Supplier;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

class OutboxJdbcRepositoryTest {

    private final JdbcTemplate jdbcTemplate = mock(JdbcTemplate.class);
    private final OutboxTracing outboxTracing = mock(OutboxTracing.class);
    private final OutboxJdbcRepository repository =
            new OutboxJdbcRepository(jdbcTemplate, JsonMapper.builder().build(), outboxTracing);

    @BeforeEach
    void setUpOutboxTracingPassthrough() {
        when(outboxTracing.observe(anyString(), any(Supplier.class))).thenAnswer(invocation -> {
            Supplier<?> supplier = invocation.getArgument(1);
            return supplier.get();
        });
        doAnswer(invocation -> {
            Runnable runnable = invocation.getArgument(1);
            runnable.run();
            return null;
        }).when(outboxTracing).observe(anyString(), any(Runnable.class));
    }

    @Test
    void toEnvelopeBuildsKafkaMessage() {
        OutboxRow row = new OutboxRow(
                1L,
                99L,
                10L,
                "OrderCreated",
                "{\"orderId\":99,\"customerId\":10}",
                OutboxStatus.NEW,
                0,
                "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01"
        );

        EventEnvelope envelope = repository.toEnvelope(row, "corr-1");

        assertThat(envelope.eventId()).isEqualTo(1L);
        assertThat(envelope.orderId()).isEqualTo(99L);
        assertThat(envelope.customerId()).isEqualTo(10L);
        assertThat(envelope.eventType()).isEqualTo("OrderCreated");
        assertThat(envelope.correlationId()).isEqualTo("corr-1");
        assertThat(envelope.traceParent()).isEqualTo("00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01");
        assertThat(envelope.payload()).containsEntry("orderId", 99);
    }

    @Test
    void toEnvelopeFailsOnInvalidPayload() {
        OutboxRow row = new OutboxRow(1L, null, null, null, "not-json", null, 0, null);

        assertThatThrownBy(() -> repository.toEnvelope(row, "corr"))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("Failed to parse outbox payload");
    }

    @Test
    void claimByIdsReturnsEmptyForEmptyInput() {
        assertThat(repository.claimByIds(List.of(), "pod", Instant.now())).isEmpty();
    }

    @Test
    void markSentSkipsEmptyIds() {
        repository.markSent(List.of(), Instant.now());
        verify(jdbcTemplate, org.mockito.Mockito.never()).update(anyString(), any(Object[].class));
    }

    @Test
    void countActivePendingReturnsZeroWhenNull() {
        when(jdbcTemplate.queryForObject(anyString(), eq(Long.class), any())).thenReturn(null);
        assertThat(repository.countActivePending()).isZero();
    }

    @Test
    void countActivePendingReturnsCount() {
        when(jdbcTemplate.queryForObject(anyString(), eq(Long.class), any())).thenReturn(7L);
        assertThat(repository.countActivePending()).isEqualTo(7L);
    }

    @Test
    void insertEventReturnsGeneratedId() {
        Instant now = Instant.parse("2026-01-01T00:00:00Z");
        when(jdbcTemplate.queryForObject(anyString(), eq(Long.class), any(), any(), any(), any(), any(), any(), any()))
                .thenReturn(55L);

        long id = repository.insertEvent(1L, 2L, "OrderCreated", "{}", "trace", now);

        assertThat(id).isEqualTo(55L);
    }

    @Test
    void claimRecoverableIdsDelegatesToJdbc() {
        when(jdbcTemplate.query(anyString(), any(RowMapper.class), any(), any(), any(), any()))
                .thenReturn(List.of(10L, 11L));

        List<Long> ids = repository.claimRecoverableIds(5, "pod-1", Instant.now());

        assertThat(ids).containsExactly(10L, 11L);
    }

    @Test
    void findByIdReturnsFirstRow() {
        OutboxRow row = new OutboxRow(1L, 2L, 3L, "OrderCreated", "{}", OutboxStatus.NEW, 0, null);
        when(jdbcTemplate.query(anyString(), any(RowMapper.class), eq(1L))).thenReturn(List.of(row));

        Optional<OutboxRow> found = repository.findById(1L);

        assertThat(found).contains(row);
    }

    @Test
    void markFailedUpdatesRow() {
        repository.markFailed(9L, 2, OutboxStatus.FAILED);

        verify(jdbcTemplate).update(anyString(), eq(OutboxStatus.FAILED.getCode()), eq(2), eq(9L));
    }

    @Test
    void clearLeaseSkipsEmptyIds() {
        repository.clearLease(List.of());
        verify(jdbcTemplate, org.mockito.Mockito.never()).update(anyString(), any(Object[].class));
    }

    @Test
    void findReenqueueableIdsReturnsEmptyForEmptyInput() {
        assertThat(repository.findReenqueueableIds(List.of())).isEmpty();
    }

    @Test
    void markSentUpdatesRowsWhenIdsPresent() {
        repository.markSent(List.of(1L, 2L), Instant.parse("2026-01-01T00:00:00Z"));
        verify(jdbcTemplate).update(anyString(), any(Object[].class));
    }

    @Test
    void clearLeaseUpdatesRowsWhenIdsPresent() {
        repository.clearLease(List.of(3L));
        verify(jdbcTemplate).update(anyString(), eq(3L));
    }

    @Test
    void claimByIdsQueriesDatabase() {
        when(jdbcTemplate.query(anyString(), any(RowMapper.class), any(Object[].class))).thenReturn(List.of());
        assertThat(repository.claimByIds(List.of(9L), "pod", Instant.now())).isEmpty();
    }

    @Test
    void claimByIdsMapsRows() throws Exception {
        when(jdbcTemplate.query(anyString(), any(RowMapper.class), any(Object[].class)))
                .thenAnswer(invocation -> {
                    RowMapper<OutboxRow> mapper = invocation.getArgument(1);
                    java.sql.ResultSet rs = mock(java.sql.ResultSet.class);
                    when(rs.getLong("id")).thenReturn(9L);
                    when(rs.getLong("order_id")).thenReturn(10L);
                    when(rs.getLong("customer_id")).thenReturn(11L);
                    when(rs.getString("event_type")).thenReturn("OrderCreated");
                    when(rs.getString("payload")).thenReturn("{\"orderId\":10}");
                    when(rs.getInt("status")).thenReturn(OutboxStatus.NEW.getCode());
                    when(rs.getInt("retry_count")).thenReturn(0);
                    when(rs.getString("trace_parent")).thenReturn("trace");
                    return List.of(mapper.mapRow(rs, 0));
                });

        List<OutboxRow> rows = repository.claimByIds(List.of(9L), "pod", Instant.now());

        assertThat(rows).hasSize(1);
        assertThat(rows.get(0).id()).isEqualTo(9L);
    }

    @Test
    void findReenqueueableIdsReturnsIds() {
        when(jdbcTemplate.query(anyString(), any(RowMapper.class), any(Object[].class)))
                .thenReturn(List.of(4L));

        assertThat(repository.findReenqueueableIds(List.of(4L))).containsExactly(4L);
    }
}
