package com.kholodilin.outbox.idempotency;

import tools.jackson.databind.json.JsonMapper;
import com.kholodilin.outbox.events.CreateOrderResponse;
import com.kholodilin.outbox.events.IdempotencyStatus;
import com.kholodilin.outbox.persistence.IdempotencyKeyRow;
import com.kholodilin.outbox.persistence.IdempotencyR2dbcRepository;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.slf4j.MDC;
import reactor.core.publisher.Mono;
import reactor.test.StepVerifier;

import java.time.Instant;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class IdempotencyServiceTest {

    @Mock
    private IdempotencyR2dbcRepository repository;

    private final JsonMapper objectMapper = JsonMapper.builder().build();
    private IdempotencyService service;

    @BeforeEach
    void setUp() {
        service = new IdempotencyService(repository, objectMapper);
    }

    @AfterEach
    void tearDown() {
        MDC.clear();
    }

    @Test
    void returnsCachedCompletedResponse() throws Exception {
        IdempotencyKeyRow row = row(
                "hash",
                IdempotencyStatus.COMPLETED,
                objectMapper.writeValueAsString(new CreateOrderResponse(1L, 2L, "ACCEPTED", Instant.now()))
        );
        when(repository.findByCustomerIdAndKey(1L, "key")).thenReturn(Mono.just(row));

        StepVerifier.create(service.findCachedResponse(1L, "key", "hash"))
                .assertNext(response -> assertThat(response.orderId()).isEqualTo(1L))
                .verifyComplete();
    }

    @Test
    void throwsOnHashConflict() {
        when(repository.findByCustomerIdAndKey(1L, "key"))
                .thenReturn(Mono.just(row("other", IdempotencyStatus.COMPLETED, null)));

        StepVerifier.create(service.findCachedResponse(1L, "key", "hash"))
                .expectError(IdempotencyConflictException.class)
                .verify();
    }

    @Test
    void throwsWhenRequestIsStillProcessing() {
        when(repository.findByCustomerIdAndKey(1L, "key"))
                .thenReturn(Mono.just(row("hash", IdempotencyStatus.PROCESSING, null)));

        StepVerifier.create(service.findCachedResponse(1L, "key", "hash"))
                .expectErrorMatches(ex -> ex instanceof IdempotencyConflictException
                        && ex.getMessage().contains("already being processed"))
                .verify();
    }

    @Test
    void errorsWhenNoExistingRecord() {
        when(repository.findByCustomerIdAndKey(1L, "key")).thenReturn(Mono.empty());

        StepVerifier.create(service.findCachedResponse(1L, "key", "hash"))
                .expectErrorMatches(ex -> ex instanceof IllegalStateException
                        && ex.getMessage().contains("no usable row was found"))
                .verify();
    }

    @Test
    void errorsWhenRecordIsFailedStatus() {
        when(repository.findByCustomerIdAndKey(1L, "key"))
                .thenReturn(Mono.just(row("hash", IdempotencyStatus.FAILED, null)));

        StepVerifier.create(service.findCachedResponse(1L, "key", "hash"))
                .expectError(IllegalStateException.class)
                .verify();
    }

    @Test
    void errorsWhenCompletedWithoutResponseBody() {
        when(repository.findByCustomerIdAndKey(1L, "key"))
                .thenReturn(Mono.just(row("hash", IdempotencyStatus.COMPLETED, null)));

        StepVerifier.create(service.findCachedResponse(1L, "key", "hash"))
                .expectError(IllegalStateException.class)
                .verify();
    }

    @Test
    void throwsWhenStoredResponseCannotBeDeserialized() {
        when(repository.findByCustomerIdAndKey(1L, "key"))
                .thenReturn(Mono.just(row("hash", IdempotencyStatus.COMPLETED, "not-json")));

        StepVerifier.create(service.findCachedResponse(1L, "key", "hash"))
                .expectErrorMatches(ex -> ex instanceof IllegalStateException
                        && ex.getMessage().contains("Failed to deserialize"))
                .verify();
    }

    private static IdempotencyKeyRow row(String requestHash, IdempotencyStatus status, String responseBody) {
        return new IdempotencyKeyRow(1L, 1L, "key", requestHash, status, responseBody, Instant.EPOCH, Instant.EPOCH);
    }
}
