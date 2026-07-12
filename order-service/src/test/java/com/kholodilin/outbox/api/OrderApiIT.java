package com.kholodilin.outbox.api;

import com.kholodilin.outbox.config.KafkaProducerConfig;
import com.kholodilin.outbox.events.EventConstants;
import com.kholodilin.outbox.events.EventEnvelope;
import com.kholodilin.outbox.events.OutboxStatus;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.header.Header;
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
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.kafka.support.serializer.JsonDeserializer;
import org.springframework.kafka.test.EmbeddedKafkaBroker;
import org.springframework.kafka.test.context.EmbeddedKafka;
import org.springframework.kafka.test.utils.KafkaTestUtils;
import org.springframework.test.context.ActiveProfiles;

import java.nio.charset.StandardCharsets;
import java.time.Duration;
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
    private EmbeddedKafkaBroker embeddedKafka;

    @Autowired
    private JdbcTemplate jdbcTemplate;

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

        awaitSentInDatabase(eventId);

        try (KafkaConsumer<String, EventEnvelope> consumer = createConsumer()) {
            TopicPartition partition = new TopicPartition(EventConstants.TOPIC_ORDERS, 0);
            consumer.assign(List.of(partition));
            consumer.seekToBeginning(List.of(partition));

            ConsumerRecord<String, EventEnvelope> record = awaitRecord(consumer, eventId);
            assertThat(record).isNotNull();
            assertThat(record.key()).isEqualTo("42");
            assertThat(record.value()).isNotNull();
            assertThat(record.value().getCustomerId()).isEqualTo(42L);
            assertThat(record.value().getEventId()).isEqualTo(eventId);
        }
    }

    private void awaitSentInDatabase(long eventId) {
        long deadline = System.currentTimeMillis() + 20_000;
        while (System.currentTimeMillis() < deadline) {
            Integer status = jdbcTemplate.queryForObject(
                    "SELECT status FROM outbox_events WHERE id = ?",
                    Integer.class,
                    eventId
            );
            if (status != null && status == OutboxStatus.SENT.getCode()) {
                return;
            }
            try {
                Thread.sleep(200);
            } catch (InterruptedException ex) {
                Thread.currentThread().interrupt();
                throw new IllegalStateException("Interrupted while waiting for outbox SENT status", ex);
            }
        }
        throw new AssertionError("Outbox event " + eventId + " was not marked SENT within 20s");
    }

    private ConsumerRecord<String, EventEnvelope> awaitRecord(KafkaConsumer<String, EventEnvelope> consumer, long eventId) {
        long deadline = System.currentTimeMillis() + 10_000;
        while (System.currentTimeMillis() < deadline) {
            ConsumerRecords<String, EventEnvelope> records = KafkaTestUtils.getRecords(consumer, Duration.ofMillis(500));
            for (ConsumerRecord<String, EventEnvelope> record : records) {
                if (matchesEventId(record, eventId)) {
                    return record;
                }
            }
        }
        return null;
    }

    private boolean matchesEventId(ConsumerRecord<String, EventEnvelope> record, long eventId) {
        Header header = record.headers().lastHeader(EventConstants.HEADER_EVENT_ID);
        if (header != null) {
            return eventId == Long.parseLong(new String(header.value(), StandardCharsets.UTF_8));
        }
        return record.value() != null && eventId == record.value().getEventId();
    }

    private KafkaConsumer<String, EventEnvelope> createConsumer() {
        Map<String, Object> props = KafkaTestUtils.consumerProps(
                "order-api-it-" + UUID.randomUUID(),
                "false",
                embeddedKafka
        );
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        JsonDeserializer<EventEnvelope> deserializer = new JsonDeserializer<>(
                EventEnvelope.class,
                KafkaProducerConfig.kafkaObjectMapper()
        );
        deserializer.addTrustedPackages("com.kholodilin.outbox.events");
        deserializer.setUseTypeHeaders(false);
        return new KafkaConsumer<>(props, new StringDeserializer(), deserializer);
    }
}
