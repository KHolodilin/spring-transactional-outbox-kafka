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
        EventEnvelope envelope = new EventEnvelope(
                1L,
                10L,
                20L,
                "OrderCreated",
                Map.of("orderId", 10),
                null,
                Instant.parse("2026-07-12T10:15:30Z"),
                null
        );

        String json = mapper.writeValueAsString(envelope);

        assertThat(json).contains("2026-07-12T10:15:30Z");
        assertThat(json).doesNotContain("1752305730");
    }
}
