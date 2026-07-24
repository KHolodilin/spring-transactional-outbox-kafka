package com.kholodilin.outbox.config;

import tools.jackson.databind.json.JsonMapper;
import com.kholodilin.outbox.events.EventEnvelope;
import org.junit.jupiter.api.Test;

import java.time.Instant;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

class KafkaConsumerConfigTest {

    @Test
    void deserializesInstantFromIso8601() throws Exception {
        JsonMapper mapper = KafkaConsumerConfig.kafkaObjectMapper();
        String json = """
                {
                  "eventId": 1,
                  "orderId": 10,
                  "customerId": 20,
                  "eventType": "OrderCreated",
                  "payload": {"orderId": 10},
                  "occurredAt": "2026-07-12T10:15:30Z"
                }
                """;

        EventEnvelope envelope = mapper.readValue(json, EventEnvelope.class);

        assertThat(envelope.eventId()).isEqualTo(1L);
        assertThat(envelope.occurredAt()).isEqualTo(Instant.parse("2026-07-12T10:15:30Z"));
        assertThat(envelope.payload()).containsEntry("orderId", 10);
    }
}
