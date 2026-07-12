package com.kholodilin.outbox.persistence;

import tools.jackson.databind.json.JsonMapper;
import com.kholodilin.outbox.events.EventEnvelope;
import com.kholodilin.outbox.events.OutboxStatus;
import org.junit.jupiter.api.Test;
import org.springframework.jdbc.core.JdbcTemplate;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;

class OutboxJdbcRepositoryTest {

    private final OutboxJdbcRepository repository =
            new OutboxJdbcRepository(mock(JdbcTemplate.class), JsonMapper.builder().build());

    @Test
    void toEnvelopeBuildsKafkaMessage() {
        OutboxJdbcRepository.OutboxRow row = new OutboxJdbcRepository.OutboxRow(
                1L,
                99L,
                10L,
                "OrderCreated",
                "{\"orderId\":99,\"customerId\":10}",
                OutboxStatus.NEW,
                0
        );

        EventEnvelope envelope = repository.toEnvelope(row, "corr-1");

        assertThat(envelope.getEventId()).isEqualTo(1L);
        assertThat(envelope.getOrderId()).isEqualTo(99L);
        assertThat(envelope.getCustomerId()).isEqualTo(10L);
        assertThat(envelope.getEventType()).isEqualTo("OrderCreated");
        assertThat(envelope.getCorrelationId()).isEqualTo("corr-1");
        assertThat(envelope.getPayload()).containsEntry("orderId", 99);
    }
}
