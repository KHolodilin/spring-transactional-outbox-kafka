package com.kholodilin.outbox.config;

import tools.jackson.databind.json.JsonMapper;
import com.kholodilin.outbox.events.EventEnvelope;
import org.junit.jupiter.api.Test;

import java.time.Instant;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

class KafkaProducerConfigTest {

    @Test
    void serializesInstantAsIso8601() throws Exception {
        JsonMapper mapper = KafkaProducerConfig.kafkaObjectMapper();
        EventEnvelope envelope = EventEnvelope.builder()
                .eventId(1L)
                .orderId(10L)
                .customerId(20L)
                .eventType("OrderCreated")
                .payload(Map.of("orderId", 10))
                .occurredAt(Instant.parse("2026-07-12T10:15:30Z"))
                .build();

        String json = mapper.writeValueAsString(envelope);

        assertThat(json).contains("2026-07-12T10:15:30Z");
        assertThat(json).doesNotContain("1752305730");
    }
}
