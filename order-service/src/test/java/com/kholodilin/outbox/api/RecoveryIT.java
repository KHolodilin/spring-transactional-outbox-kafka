package com.kholodilin.outbox.api;

import com.kholodilin.outbox.config.AppProperties;
import com.kholodilin.outbox.events.EventConstants;
import com.kholodilin.outbox.events.OutboxStatus;
import com.kholodilin.outbox.persistence.OutboxJdbcRepository;
import com.kholodilin.outbox.queue.InMemoryEventQueue;
import com.kholodilin.outbox.recovery.RecoveryWorker;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.kafka.support.serializer.JsonDeserializer;
import org.springframework.kafka.test.context.EmbeddedKafka;
import org.springframework.test.context.ActiveProfiles;

import java.time.Duration;
import java.time.Instant;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

@SpringBootTest
@ActiveProfiles("test")
@EmbeddedKafka(partitions = 1, topics = EventConstants.TOPIC_ORDERS)
class RecoveryIT {

    @Autowired
    private JdbcTemplate jdbcTemplate;

    @Autowired
    private RecoveryWorker recoveryWorker;

    @Autowired
    private InMemoryEventQueue eventQueue;

    @Autowired
    private AppProperties properties;

    @Autowired
    private org.springframework.core.env.Environment environment;

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
                OutboxStatus.NEW.getCode(),
                now
        );

        properties.getOutbox().getRecovery().setEnabled(true);
        recoveryWorker.recover();

        try (KafkaConsumer<String, com.kholodilin.outbox.events.EventEnvelope> consumer = createConsumer()) {
            consumer.subscribe(List.of(EventConstants.TOPIC_ORDERS));
            ConsumerRecords<String, com.kholodilin.outbox.events.EventEnvelope> records = consumer.poll(Duration.ofSeconds(15));
            assertThat(records.count()).isPositive();
            ConsumerRecord<String, com.kholodilin.outbox.events.EventEnvelope> record = records.iterator().next();
            assertThat(record.key()).isEqualTo("55");
            assertThat(record.value().getEventId()).isEqualTo(eventId);
        }
    }

    private KafkaConsumer<String, com.kholodilin.outbox.events.EventEnvelope> createConsumer() {
        Map<String, Object> props = new HashMap<>();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, environment.getProperty("spring.embedded.kafka.brokers"));
        props.put(ConsumerConfig.GROUP_ID_CONFIG, "recovery-it");
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, JsonDeserializer.class);
        props.put(JsonDeserializer.TRUSTED_PACKAGES, "com.kholodilin.outbox.events");
        props.put(JsonDeserializer.VALUE_DEFAULT_TYPE, com.kholodilin.outbox.events.EventEnvelope.class.getName());
        return new KafkaConsumer<>(props);
    }
}
