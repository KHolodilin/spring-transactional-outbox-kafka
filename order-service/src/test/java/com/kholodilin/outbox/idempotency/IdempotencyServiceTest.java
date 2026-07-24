package com.kholodilin.outbox.idempotency;

import tools.jackson.databind.json.JsonMapper;
import com.kholodilin.outbox.events.CreateOrderResponse;
import com.kholodilin.outbox.events.IdempotencyStatus;
import com.kholodilin.outbox.persistence.IdempotencyJdbcRepository;
import com.kholodilin.outbox.persistence.IdempotencyKeyRow;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.time.Instant;
import java.util.Optional;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class IdempotencyServiceTest {

    @Mock
    private IdempotencyJdbcRepository repository;

    private final JsonMapper objectMapper = JsonMapper.builder().build();
    private IdempotencyService service;

    @BeforeEach
    void setUp() {
        service = new IdempotencyService(repository, objectMapper);
    }

    @Test
    void returnsCachedCompletedResponse() throws Exception {
        IdempotencyKeyRow row = row(
                "hash",
                IdempotencyStatus.COMPLETED,
                objectMapper.writeValueAsString(new CreateOrderResponse(1L, 2L, "ACCEPTED", Instant.now()))
        );
        when(repository.findByCustomerIdAndKey(1L, "key")).thenReturn(Optional.of(row));

        Optional<CreateOrderResponse> response = service.findCachedResponse(1L, "key", "hash");
        assertThat(response).isPresent();
        assertThat(response.get().orderId()).isEqualTo(1L);
    }

    @Test
    void throwsOnHashConflict() {
        when(repository.findByCustomerIdAndKey(1L, "key"))
                .thenReturn(Optional.of(row("other", IdempotencyStatus.COMPLETED, null)));

        assertThatThrownBy(() -> service.findCachedResponse(1L, "key", "hash"))
                .isInstanceOf(IdempotencyConflictException.class);
    }

    @Test
    void throwsWhenRequestIsStillProcessing() {
        when(repository.findByCustomerIdAndKey(1L, "key"))
                .thenReturn(Optional.of(row("hash", IdempotencyStatus.PROCESSING, null)));

        assertThatThrownBy(() -> service.findCachedResponse(1L, "key", "hash"))
                .isInstanceOf(IdempotencyConflictException.class)
                .hasMessageContaining("already being processed");
    }

    @Test
    void returnsEmptyWhenNoExistingRecord() {
        when(repository.findByCustomerIdAndKey(1L, "key")).thenReturn(Optional.empty());

        assertThat(service.findCachedResponse(1L, "key", "hash")).isEmpty();
    }

    @Test
    void returnsEmptyWhenRecordIsFailedStatus() {
        when(repository.findByCustomerIdAndKey(1L, "key"))
                .thenReturn(Optional.of(row("hash", IdempotencyStatus.FAILED, null)));

        assertThat(service.findCachedResponse(1L, "key", "hash")).isEmpty();
    }

    @Test
    void returnsEmptyWhenCompletedWithoutResponseBody() {
        when(repository.findByCustomerIdAndKey(1L, "key"))
                .thenReturn(Optional.of(row("hash", IdempotencyStatus.COMPLETED, null)));

        assertThat(service.findCachedResponse(1L, "key", "hash")).isEmpty();
    }

    @Test
    void throwsWhenStoredResponseCannotBeDeserialized() {
        when(repository.findByCustomerIdAndKey(1L, "key"))
                .thenReturn(Optional.of(row("hash", IdempotencyStatus.COMPLETED, "not-json")));

        assertThatThrownBy(() -> service.findCachedResponse(1L, "key", "hash"))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("Failed to deserialize");
    }

    private static IdempotencyKeyRow row(String requestHash, IdempotencyStatus status, String responseBody) {
        return new IdempotencyKeyRow(1L, 1L, "key", requestHash, status, responseBody, Instant.EPOCH, Instant.EPOCH);
    }
}
