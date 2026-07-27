package com.kholodilin.outbox.persistence;

import com.kholodilin.outbox.events.IdempotencyStatus;
import io.r2dbc.spi.Row;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.r2dbc.core.DatabaseClient;
import org.springframework.r2dbc.core.FetchSpec;
import org.springframework.r2dbc.core.RowsFetchSpec;
import reactor.core.publisher.Mono;
import reactor.test.StepVerifier;

import java.time.Instant;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.util.Map;
import java.util.function.BiFunction;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.lenient;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class IdempotencyR2dbcRepositoryTest {

    @Mock
    private DatabaseClient databaseClient;

    @Mock
    private DatabaseClient.GenericExecuteSpec spec;

    @Mock
    private RowsFetchSpec<Object> rowsFetchSpec;

    @Mock
    private FetchSpec<Map<String, Object>> fetchSpec;

    private IdempotencyR2dbcRepository repository;

    @BeforeEach
    void setUp() {
        repository = new IdempotencyR2dbcRepository(databaseClient);
        lenient().when(databaseClient.sql(anyString())).thenReturn(spec);
        lenient().when(spec.bind(anyString(), any())).thenReturn(spec);
    }

    @Test
    @SuppressWarnings("unchecked")
    void findByCustomerIdAndKeyMapsRow() {
        Instant now = Instant.parse("2026-01-01T00:00:00Z");
        when(spec.map(any(BiFunction.class))).thenAnswer(invocation -> {
            BiFunction<Row, Object, IdempotencyKeyRow> mapper = invocation.getArgument(0);
            Row row = mock(Row.class);
            when(row.get("customer_id", Long.class)).thenReturn(42L);
            when(row.get("id", Long.class)).thenReturn(7L);
            when(row.get("idempotency_key", String.class)).thenReturn("key-1");
            when(row.get("request_hash", String.class)).thenReturn("hash");
            when(row.get("status", Integer.class)).thenReturn(IdempotencyStatus.COMPLETED.getCode());
            when(row.get("response_body", String.class)).thenReturn("{\"orderId\":1}");
            when(row.get("created_at", OffsetDateTime.class)).thenReturn(now.atOffset(ZoneOffset.UTC));
            when(row.get("updated_at", OffsetDateTime.class)).thenReturn(null);
            IdempotencyKeyRow mapped = mapper.apply(row, null);
            RowsFetchSpec<IdempotencyKeyRow> fetch = mock(RowsFetchSpec.class);
            when(fetch.one()).thenReturn(Mono.just(mapped));
            return fetch;
        });

        StepVerifier.create(repository.findByCustomerIdAndKey(42L, "key-1"))
                .assertNext(found -> {
                    assertThat(found.customerId()).isEqualTo(42L);
                    assertThat(found.status()).isEqualTo(IdempotencyStatus.COMPLETED);
                    assertThat(found.requestHash()).isEqualTo("hash");
                    assertThat(found.updatedAt()).isNull();
                })
                .verifyComplete();
    }

    @Test
    @SuppressWarnings("unchecked")
    void tryInsertProcessingReturnsIdWhenInsertWins() {
        when(spec.map(any(BiFunction.class))).thenReturn(rowsFetchSpec);
        when(rowsFetchSpec.one()).thenReturn(Mono.just(11L));

        StepVerifier.create(repository.tryInsertProcessing(1L, "key", "hash", Instant.parse("2026-01-01T00:00:00Z")))
                .expectNext(11L)
                .verifyComplete();

        verify(spec).bind(eq("status"), eq(IdempotencyStatus.PROCESSING.getCode()));
    }

    @Test
    @SuppressWarnings("unchecked")
    void tryInsertProcessingReturnsEmptyOnConflict() {
        when(spec.map(any(BiFunction.class))).thenReturn(rowsFetchSpec);
        when(rowsFetchSpec.one()).thenReturn(Mono.empty());

        StepVerifier.create(repository.tryInsertProcessing(1L, "key", "hash", Instant.parse("2026-01-01T00:00:00Z")))
                .verifyComplete();
    }

    @Test
    void completeUpdatesStatusAndResponse() {
        when(spec.fetch()).thenReturn(fetchSpec);
        when(fetchSpec.rowsUpdated()).thenReturn(Mono.just(1L));

        StepVerifier.create(repository.complete(1L, "key", "{\"ok\":true}", Instant.parse("2026-01-01T00:00:00Z")))
                .verifyComplete();

        verify(spec).bind("status", IdempotencyStatus.COMPLETED.getCode());
        verify(spec).bind("responseBody", "{\"ok\":true}");
    }
}
