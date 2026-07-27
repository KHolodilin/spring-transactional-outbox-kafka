package com.kholodilin.outbox.persistence;

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

import java.math.BigDecimal;
import java.time.Instant;
import java.util.function.BiFunction;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.lenient;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class OrderR2dbcRepositoryTest {

    @Mock
    private DatabaseClient databaseClient;

    @Mock
    private DatabaseClient.GenericExecuteSpec spec;

    @Mock
    private RowsFetchSpec<Long> rowsFetchSpec;

    @Mock
    private FetchSpec<java.util.Map<String, Object>> fetchSpec;

    private OrderR2dbcRepository repository;

    @BeforeEach
    void setUp() {
        repository = new OrderR2dbcRepository(databaseClient);
        lenient().when(databaseClient.sql(anyString())).thenReturn(spec);
        lenient().when(spec.bind(anyString(), any())).thenReturn(spec);
    }

    @Test
    void insertOrderReturnsGeneratedId() {
        when(spec.map(any(BiFunction.class))).thenReturn(rowsFetchSpec);
        when(rowsFetchSpec.one()).thenReturn(Mono.just(101L));

        StepVerifier.create(repository.insertOrder(42L, BigDecimal.TEN, Instant.parse("2026-01-01T00:00:00Z")))
                .expectNext(101L)
                .verifyComplete();

        verify(databaseClient).sql(anyString());
        verify(spec).bind("customerId", 42L);
        verify(spec).bind("status", "CREATED");
    }

    @Test
    void insertOrderItemCompletes() {
        when(spec.fetch()).thenReturn(fetchSpec);
        when(fetchSpec.rowsUpdated()).thenReturn(Mono.just(1L));

        StepVerifier.create(repository.insertOrderItem(
                        101L, 42L, "sku-1", 2, BigDecimal.ONE, Instant.parse("2026-01-01T00:00:00Z")))
                .verifyComplete();

        verify(spec).bind("orderId", 101L);
        verify(spec).bind("productId", "sku-1");
        verify(spec).bind("quantity", 2);
    }
}
