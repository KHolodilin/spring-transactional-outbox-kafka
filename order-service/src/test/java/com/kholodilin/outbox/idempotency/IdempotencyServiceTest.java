package com.kholodilin.outbox.idempotency;

import tools.jackson.databind.json.JsonMapper;
import com.kholodilin.outbox.events.CreateOrderResponse;
import com.kholodilin.outbox.events.IdempotencyStatus;
import com.kholodilin.outbox.persistence.IdempotencyJdbcRepository;
import com.kholodilin.outbox.persistence.entity.IdempotencyKeyEntity;
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
        IdempotencyKeyEntity entity = IdempotencyKeyEntity.builder()
                .requestHash("hash")
                .status(IdempotencyStatus.COMPLETED)
                .responseBody(objectMapper.writeValueAsString(CreateOrderResponse.builder()
                        .orderId(1L)
                        .eventId(2L)
                        .status("ACCEPTED")
                        .createdAt(Instant.now())
                        .build()))
                .build();
        when(repository.findByCustomerIdAndKey(1L, "key")).thenReturn(Optional.of(entity));

        Optional<CreateOrderResponse> response = service.findCachedResponse(1L, "key", "hash");
        assertThat(response).isPresent();
        assertThat(response.get().getOrderId()).isEqualTo(1L);
    }

    @Test
    void throwsOnHashConflict() {
        IdempotencyKeyEntity entity = IdempotencyKeyEntity.builder()
                .requestHash("other")
                .status(IdempotencyStatus.COMPLETED)
                .build();
        when(repository.findByCustomerIdAndKey(1L, "key")).thenReturn(Optional.of(entity));

        assertThatThrownBy(() -> service.findCachedResponse(1L, "key", "hash"))
                .isInstanceOf(IdempotencyConflictException.class);
    }

    @Test
    void throwsWhenRequestIsStillProcessing() {
        IdempotencyKeyEntity entity = IdempotencyKeyEntity.builder()
                .requestHash("hash")
                .status(IdempotencyStatus.PROCESSING)
                .build();
        when(repository.findByCustomerIdAndKey(1L, "key")).thenReturn(Optional.of(entity));

        assertThatThrownBy(() -> service.findCachedResponse(1L, "key", "hash"))
                .isInstanceOf(IdempotencyConflictException.class)
                .hasMessageContaining("already being processed");
    }
}
