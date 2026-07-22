package com.kholodilin.outbox.persistence;

import com.kholodilin.outbox.events.IdempotencyStatus;
import com.kholodilin.outbox.persistence.entity.IdempotencyKeyEntity;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.RowMapper;

import java.sql.ResultSet;
import java.sql.Timestamp;
import java.time.Instant;
import java.util.List;
import java.util.Optional;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class IdempotencyJdbcRepositoryTest {

    @Mock
    private JdbcTemplate jdbcTemplate;

    @Test
    void findByCustomerIdAndKeyMapsRow() throws Exception {
        IdempotencyJdbcRepository repository = new IdempotencyJdbcRepository(jdbcTemplate);
        when(jdbcTemplate.query(anyString(), any(RowMapper.class), eq(42L), eq("key-1")))
                .thenAnswer(invocation -> {
                    RowMapper<IdempotencyKeyEntity> mapper = invocation.getArgument(1);
                    ResultSet rs = mock(ResultSet.class);
                    when(rs.getLong("customer_id")).thenReturn(42L);
                    when(rs.getLong("id")).thenReturn(7L);
                    when(rs.getString("idempotency_key")).thenReturn("key-1");
                    when(rs.getString("request_hash")).thenReturn("hash");
                    when(rs.getInt("status")).thenReturn(IdempotencyStatus.COMPLETED.getCode());
                    when(rs.getString("response_body")).thenReturn("{\"orderId\":1}");
                    Instant now = Instant.parse("2026-01-01T00:00:00Z");
                    when(rs.getTimestamp("created_at")).thenReturn(Timestamp.from(now));
                    when(rs.getTimestamp("updated_at")).thenReturn(Timestamp.from(now));
                    return List.of(mapper.mapRow(rs, 0));
                });

        Optional<IdempotencyKeyEntity> found = repository.findByCustomerIdAndKey(42L, "key-1");

        assertThat(found).isPresent();
        assertThat(found.get().getStatus()).isEqualTo(IdempotencyStatus.COMPLETED);
        assertThat(found.get().getRequestHash()).isEqualTo("hash");
    }

    @Test
    void tryInsertProcessingReturnsIdWhenInsertWins() {
        IdempotencyJdbcRepository repository = new IdempotencyJdbcRepository(jdbcTemplate);
        Instant now = Instant.parse("2026-01-01T00:00:00Z");
        when(jdbcTemplate.query(anyString(), any(RowMapper.class), any(), any(), any(), any(), any(), any()))
                .thenReturn(List.of(11L));

        Optional<Long> id = repository.tryInsertProcessing(1L, "key", "hash", now);

        assertThat(id).contains(11L);
        ArgumentCaptor<String> sql = ArgumentCaptor.forClass(String.class);
        verify(jdbcTemplate).query(
                sql.capture(),
                any(RowMapper.class),
                eq(1L),
                eq("key"),
                eq("hash"),
                eq(IdempotencyStatus.PROCESSING.getCode()),
                any(Timestamp.class),
                any(Timestamp.class)
        );
        assertThat(sql.getValue()).contains("ON CONFLICT");
        assertThat(sql.getValue()).contains("DO NOTHING");
    }

    @Test
    void tryInsertProcessingReturnsEmptyOnConflict() {
        IdempotencyJdbcRepository repository = new IdempotencyJdbcRepository(jdbcTemplate);
        Instant now = Instant.parse("2026-01-01T00:00:00Z");
        when(jdbcTemplate.query(anyString(), any(RowMapper.class), any(), any(), any(), any(), any(), any()))
                .thenReturn(List.of());

        assertThat(repository.tryInsertProcessing(1L, "key", "hash", now)).isEmpty();
    }

    @Test
    void completeUpdatesStatusAndResponse() {
        IdempotencyJdbcRepository repository = new IdempotencyJdbcRepository(jdbcTemplate);
        Instant now = Instant.parse("2026-01-01T00:00:00Z");

        repository.complete(1L, "key", "{\"ok\":true}", now);

        ArgumentCaptor<Object[]> params = ArgumentCaptor.forClass(Object[].class);
        verify(jdbcTemplate).update(anyString(), params.capture());
        assertThat(params.getValue()[0]).isEqualTo(IdempotencyStatus.COMPLETED.getCode());
        assertThat(params.getValue()[1]).isEqualTo("{\"ok\":true}");
    }
}
