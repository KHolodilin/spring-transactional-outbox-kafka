package com.kholodilin.outbox.persistence;

import tools.jackson.databind.json.JsonMapper;
import com.kholodilin.outbox.events.EventEnvelope;
import com.kholodilin.outbox.events.OutboxStatus;
import io.r2dbc.spi.Row;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.r2dbc.core.DatabaseClient;
import org.springframework.r2dbc.core.FetchSpec;
import org.springframework.r2dbc.core.RowsFetchSpec;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.test.StepVerifier;

import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.function.BiFunction;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.lenient;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class OutboxR2dbcRepositoryTest {

    @Mock
    private DatabaseClient databaseClient;

    @Mock
    private DatabaseClient.GenericExecuteSpec spec;

    @Mock
    private RowsFetchSpec<Object> rowsFetchSpec;

    @Mock
    private FetchSpec<Map<String, Object>> fetchSpec;

    private OutboxR2dbcRepository repository;

    @BeforeEach
    void setUp() {
        repository = new OutboxR2dbcRepository(databaseClient, JsonMapper.builder().build());
        lenient().when(databaseClient.sql(anyString())).thenReturn(spec);
        lenient().when(spec.bind(anyString(), any())).thenReturn(spec);
        lenient().when(spec.bindNull(anyString(), any(Class.class))).thenReturn(spec);
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
        StepVerifier.create(repository.claimByIds(List.of(), "pod", Instant.now()))
                .verifyComplete();
        verifyNoInteractions(databaseClient);
    }

    @Test
    void markSentSkipsEmptyIds() {
        StepVerifier.create(repository.markSent(List.of(), Instant.now()))
                .verifyComplete();
        verifyNoInteractions(databaseClient);
    }

    @Test
    void clearLeaseSkipsEmptyIds() {
        StepVerifier.create(repository.clearLease(List.of()))
                .verifyComplete();
        verifyNoInteractions(databaseClient);
    }

    @Test
    void findReenqueueableIdsReturnsEmptyForEmptyInput() {
        StepVerifier.create(repository.findReenqueueableIds(List.of()))
                .verifyComplete();
        verifyNoInteractions(databaseClient);
    }

    @Test
    @SuppressWarnings("unchecked")
    void insertEventBindsTraceParentAndReturnsId() {
        when(spec.map(any(BiFunction.class))).thenReturn(rowsFetchSpec);
        when(rowsFetchSpec.one()).thenReturn(Mono.just(55L));

        StepVerifier.create(repository.insertEvent(
                        1L, 2L, "OrderCreated", "{}", "trace", Instant.parse("2026-01-01T00:00:00Z")))
                .expectNext(55L)
                .verifyComplete();

        verify(spec).bind("traceParent", "trace");
    }

    @Test
    @SuppressWarnings("unchecked")
    void insertEventBindsNullTraceParent() {
        when(spec.map(any(BiFunction.class))).thenReturn(rowsFetchSpec);
        when(rowsFetchSpec.one()).thenReturn(Mono.just(56L));

        StepVerifier.create(repository.insertEvent(
                        1L, 2L, "OrderCreated", "{}", null, Instant.parse("2026-01-01T00:00:00Z")))
                .expectNext(56L)
                .verifyComplete();

        verify(spec).bindNull("traceParent", String.class);
    }

    @Test
    void markFailedUpdatesRow() {
        when(spec.fetch()).thenReturn(fetchSpec);
        when(fetchSpec.rowsUpdated()).thenReturn(Mono.just(1L));

        StepVerifier.create(repository.markFailed(9L, 2, OutboxStatus.FAILED))
                .verifyComplete();

        verify(spec).bind("status", OutboxStatus.FAILED.getCode());
        verify(spec).bind("retryCount", 2);
        verify(spec).bind("id", 9L);
    }

    @Test
    void markSentUpdatesRowsWhenIdsPresent() {
        when(spec.fetch()).thenReturn(fetchSpec);
        when(fetchSpec.rowsUpdated()).thenReturn(Mono.just(2L));

        StepVerifier.create(repository.markSent(List.of(1L, 2L), Instant.parse("2026-01-01T00:00:00Z")))
                .verifyComplete();

        verify(spec).bind("id0", 1L);
        verify(spec).bind("id1", 2L);
    }

    @Test
    void clearLeaseUpdatesRowsWhenIdsPresent() {
        when(spec.fetch()).thenReturn(fetchSpec);
        when(fetchSpec.rowsUpdated()).thenReturn(Mono.just(1L));

        StepVerifier.create(repository.clearLease(List.of(3L)))
                .verifyComplete();

        verify(spec).bind("id0", 3L);
    }

    @Test
    @SuppressWarnings("unchecked")
    void countActivePendingDefaultsToZero() {
        when(spec.map(any(BiFunction.class))).thenReturn(rowsFetchSpec);
        when(rowsFetchSpec.one()).thenReturn(Mono.empty());

        StepVerifier.create(repository.countActivePending())
                .expectNext(0L)
                .verifyComplete();
    }

    @Test
    @SuppressWarnings("unchecked")
    void countActivePendingReturnsCount() {
        when(spec.map(any(BiFunction.class))).thenReturn(rowsFetchSpec);
        when(rowsFetchSpec.one()).thenReturn(Mono.just(7L));

        StepVerifier.create(repository.countActivePending())
                .expectNext(7L)
                .verifyComplete();
    }

    @Test
    @SuppressWarnings("unchecked")
    void claimByIdsMapsRows() {
        when(spec.map(any(BiFunction.class))).thenAnswer(invocation -> {
            BiFunction<Row, Object, OutboxRow> mapper = invocation.getArgument(0);
            Row row = mock(Row.class);
            when(row.get("id", Long.class)).thenReturn(9L);
            when(row.get("order_id", Long.class)).thenReturn(10L);
            when(row.get("customer_id", Long.class)).thenReturn(11L);
            when(row.get("event_type", String.class)).thenReturn("OrderCreated");
            when(row.get("payload", String.class)).thenReturn("{\"orderId\":10}");
            when(row.get("status", Integer.class)).thenReturn(OutboxStatus.PROCESSING.getCode());
            when(row.get("retry_count", Integer.class)).thenReturn(0);
            when(row.get("trace_parent", String.class)).thenReturn("trace");
            OutboxRow mapped = mapper.apply(row, null);
            RowsFetchSpec<OutboxRow> fetch = mock(RowsFetchSpec.class);
            when(fetch.all()).thenReturn(Flux.just(mapped));
            return fetch;
        });

        StepVerifier.create(repository.claimByIds(List.of(9L), "pod", Instant.now()))
                .assertNext(row -> {
                    assertThat(row.id()).isEqualTo(9L);
                    assertThat(row.eventType()).isEqualTo("OrderCreated");
                })
                .verifyComplete();
    }

    @Test
    @SuppressWarnings("unchecked")
    void claimRecoverableIdsReturnsIds() {
        when(spec.map(any(BiFunction.class))).thenReturn(rowsFetchSpec);
        when(rowsFetchSpec.all()).thenReturn(Flux.just(10L, 11L));

        StepVerifier.create(repository.claimRecoverableIds(5, "pod-1", Instant.now()))
                .expectNext(10L, 11L)
                .verifyComplete();
    }

    @Test
    @SuppressWarnings("unchecked")
    void findReenqueueableIdsReturnsIds() {
        when(spec.map(any(BiFunction.class))).thenReturn(rowsFetchSpec);
        when(rowsFetchSpec.all()).thenReturn(Flux.just(4L));

        StepVerifier.create(repository.findReenqueueableIds(List.of(4L)))
                .expectNext(4L)
                .verifyComplete();
    }
}
