package com.kholodilin.outbox.api;

import com.kholodilin.outbox.events.EventConstants;
import com.kholodilin.outbox.events.EventEnvelope;
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
import org.springframework.kafka.support.serializer.JsonDeserializer;
import org.springframework.kafka.test.EmbeddedKafkaBroker;
import org.springframework.kafka.test.context.EmbeddedKafka;
import org.springframework.kafka.test.utils.KafkaTestUtils;
import org.springframework.test.context.ActiveProfiles;

import java.time.Instant;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;

@SpringBootTest
@ActiveProfiles("test")
@EmbeddedKafka(partitions = 1, topics = EventConstants.TOPIC_ORDERS)
class RecoveryIT {

    @Autowired
    private org.springframework.jdbc.core.JdbcTemplate jdbcTemplate;

    @Autowired
    private com.kholodilin.outbox.recovery.RecoveryWorker recoveryWorker;

    @Autowired
    private com.kholodilin.outbox.config.AppProperties properties;

    @Autowired
    private EmbeddedKafkaBroker embeddedKafka;

    @Test
    void recoveryEnqueuesAndPublishesNewEvent() {
        Instant now = Instant.now();
        Long eventId = jdbcTemplate.queryForObject(
                """
                        INSERT INTO outbox_events (order_id, customer_id, event_type, payload, status, retry_count, created_at)
                        VALUES (?, ?, ?, ?::jsonb, ?, 0, ?)
                        RETURNING id
                        """,
                Long.class,
                100L,
                55L,
                EventConstants.EVENT_TYPE_ORDER_CREATED,
                "{\"orderId\":100,\"customerId\":55}",
                com.kholodilin.outbox.events.OutboxStatus.NEW.getCode(),
                now
        );

        properties.getOutbox().getRecovery().setEnabled(true);
        recoveryWorker.recover();

        try (KafkaConsumer<String, EventEnvelope> consumer = createConsumer()) {
            TopicPartition partition = new TopicPartition(EventConstants.TOPIC_ORDERS, 0);
            consumer.assign(List.of(partition));
            consumer.seekToBeginning(List.of(partition));

            ConsumerRecord<String, EventEnvelope> record = awaitRecord(consumer, eventId);
            assertThat(record).isNotNull();
            assertThat(record.key()).isEqualTo("55");
            assertThat(record.value()).isNotNull();
            assertThat(record.value().getEventId()).isEqualTo(eventId);
        }
    }

    private ConsumerRecord<String, EventEnvelope> awaitRecord(KafkaConsumer<String, EventEnvelope> consumer, long eventId) {
        long deadline = System.currentTimeMillis() + 15_000;
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
                "recovery-it-" + UUID.randomUUID(),
                "false",
                embeddedKafka
        );
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, JsonDeserializer.class);
        props.put(JsonDeserializer.TRUSTED_PACKAGES, "com.kholodilin.outbox.events");
        props.put(JsonDeserializer.VALUE_DEFAULT_TYPE, EventEnvelope.class.getName());
        props.put(JsonDeserializer.USE_TYPE_INFO_HEADERS, false);
        return new KafkaConsumer<>(props);
    }
}
