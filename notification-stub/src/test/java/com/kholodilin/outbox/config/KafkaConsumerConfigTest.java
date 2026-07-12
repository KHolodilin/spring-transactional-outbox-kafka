package com.kholodilin.outbox.config;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.kholodilin.outbox.events.EventEnvelope;
import org.junit.jupiter.api.Test;

import java.time.Instant;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

class KafkaConsumerConfigTest {

    @Test
    void deserializesInstantFromIso8601() throws Exception {
        ObjectMapper mapper = KafkaConsumerConfig.kafkaObjectMapper();
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

        assertThat(envelope.getEventId()).isEqualTo(1L);
        assertThat(envelope.getOccurredAt()).isEqualTo(Instant.parse("2026-07-12T10:15:30Z"));
        assertThat(envelope.getPayload()).containsEntry("orderId", 10);
    }
}
