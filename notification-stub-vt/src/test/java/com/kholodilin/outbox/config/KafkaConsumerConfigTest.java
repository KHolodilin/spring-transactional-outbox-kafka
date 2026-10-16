package com.kholodilin.outbox.config;

import tools.jackson.databind.json.JsonMapper;
import com.kholodilin.outbox.events.EventEnvelope;
import org.junit.jupiter.api.Test;
import org.springframework.core.task.VirtualThreadTaskExecutor;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.mock.env.MockEnvironment;

import java.time.Instant;
import java.util.Map;
import java.util.Objects;

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
    @Test
    void batchListenerRunsOnVirtualThreads() throws Exception {
        KafkaConsumerConfig config = new KafkaConsumerConfig();
        NotificationStubProperties properties = new NotificationStubProperties();
        MockEnvironment environment = new MockEnvironment()
                .withProperty("spring.kafka.bootstrap-servers", "localhost:9092")
                .withProperty("spring.kafka.consumer.group-id", "notification-stub-vt-test");
        ConsumerFactory<String, EventEnvelope> consumerFactory =
                config.consumerFactory(properties, environment);

        ConcurrentKafkaListenerContainerFactory<String, EventEnvelope> factory =
                config.batchKafkaListenerContainerFactory(consumerFactory, properties);

        assertThat(factory.isBatchListener()).isTrue();
        assertThat(Objects.requireNonNull(factory.getContainerProperties().getListenerTaskExecutor()))
                .isInstanceOf(VirtualThreadTaskExecutor.class);
    }
}
