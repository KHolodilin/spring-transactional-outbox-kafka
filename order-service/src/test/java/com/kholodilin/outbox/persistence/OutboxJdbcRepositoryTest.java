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
        OutboxRow row = OutboxRow.builder()
                .id(1L)
                .orderId(99L)
                .customerId(10L)
                .eventType("OrderCreated")
                .payload("{\"orderId\":99,\"customerId\":10}")
                .status(OutboxStatus.NEW)
                .retryCount(0)
                .build();

        EventEnvelope envelope = repository.toEnvelope(row, "corr-1");

        assertThat(envelope.getEventId()).isEqualTo(1L);
        assertThat(envelope.getOrderId()).isEqualTo(99L);
        assertThat(envelope.getCustomerId()).isEqualTo(10L);
        assertThat(envelope.getEventType()).isEqualTo("OrderCreated");
        assertThat(envelope.getCorrelationId()).isEqualTo("corr-1");
        assertThat(envelope.getPayload()).containsEntry("orderId", 99);
    }
}
