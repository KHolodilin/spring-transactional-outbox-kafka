package com.kholodilin.outbox.api;

import com.kholodilin.outbox.events.EventConstants;
import com.kholodilin.outbox.events.EventEnvelope;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.resttestclient.TestRestTemplate;
import org.springframework.boot.resttestclient.autoconfigure.AutoConfigureTestRestTemplate;
import org.springframework.http.HttpEntity;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.kafka.support.serializer.JsonDeserializer;
import org.springframework.kafka.test.context.EmbeddedKafka;
import org.springframework.test.context.ActiveProfiles;

import java.time.Duration;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;

@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)
@AutoConfigureTestRestTemplate
@ActiveProfiles("test")
@EmbeddedKafka(partitions = 1, topics = EventConstants.TOPIC_ORDERS)
class OrderApiIT {

    @Autowired
    private TestRestTemplate restTemplate;

    @Autowired
    private org.springframework.core.env.Environment environment;

    @Test
    void createsOrderAndPublishesKafkaMessageWithCustomerPartitionKey() {
        String idempotencyKey = UUID.randomUUID().toString();
        Map<String, Object> body = Map.of(
                "customerId", 42,
                "items", List.of(Map.of("productId", "sku-1", "quantity", 1, "price", 19.99)),
                "correlationId", "corr-it-1"
        );

        HttpHeaders headers = new HttpHeaders();
        headers.setContentType(MediaType.APPLICATION_JSON);
        headers.set(EventConstants.IDEMPOTENCY_KEY_HEADER, idempotencyKey);
        ResponseEntity<Map> response = restTemplate.postForEntity(
                "/api/v1/orders",
                new HttpEntity<>(body, headers),
                Map.class
        );

        assertThat(response.getStatusCode()).isEqualTo(HttpStatus.CREATED);
        assertThat(response.getBody()).containsKeys("orderId", "eventId", "status");
        long eventId = ((Number) response.getBody().get("eventId")).longValue();

        try (KafkaConsumer<String, EventEnvelope> consumer = createConsumer()) {
            consumer.subscribe(List.of(EventConstants.TOPIC_ORDERS));
            ConsumerRecord<String, EventEnvelope> record = awaitRecord(consumer, eventId);
            assertThat(record).isNotNull();
            assertThat(record.key()).isEqualTo("42");
            assertThat(record.value().getCustomerId()).isEqualTo(42L);
            assertThat(record.value().getEventId()).isEqualTo(eventId);
        }
    }

    private ConsumerRecord<String, EventEnvelope> awaitRecord(KafkaConsumer<String, EventEnvelope> consumer, long eventId) {
        long deadline = System.currentTimeMillis() + 10_000;
        while (System.currentTimeMillis() < deadline) {
            ConsumerRecords<String, EventEnvelope> records = consumer.poll(Duration.ofMillis(500));
            for (ConsumerRecord<String, EventEnvelope> record : records) {
                if (record.value() != null && eventId == record.value().getEventId()) {
                    return record;
                }
            }
        }
        return null;
    }

    private KafkaConsumer<String, EventEnvelope> createConsumer() {
        Map<String, Object> props = new HashMap<>();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, environment.getProperty("spring.embedded.kafka.brokers"));
        props.put(ConsumerConfig.GROUP_ID_CONFIG, "order-api-it-" + UUID.randomUUID());
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, JsonDeserializer.class);
        props.put(JsonDeserializer.TRUSTED_PACKAGES, "com.kholodilin.outbox.events");
        props.put(JsonDeserializer.VALUE_DEFAULT_TYPE, EventEnvelope.class.getName());
        return new KafkaConsumer<>(props);
    }
}
