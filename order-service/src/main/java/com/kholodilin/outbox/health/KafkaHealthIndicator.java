package com.kholodilin.outbox.health;

import com.kholodilin.outbox.config.AppProperties;
import org.apache.kafka.clients.producer.Producer;
import org.springframework.boot.actuate.health.Health;
import org.springframework.boot.actuate.health.HealthIndicator;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Component;

/** Verifies that the Kafka cluster is reachable and the outbox topic exists. */
@Component
public class KafkaHealthIndicator implements HealthIndicator {

    private final KafkaTemplate<String, Object> kafkaTemplate;
    private final AppProperties properties;

    public KafkaHealthIndicator(KafkaTemplate<String, Object> kafkaTemplate, AppProperties properties) {
        this.kafkaTemplate = kafkaTemplate;
        this.properties = properties;
    }

    @Override
    public Health health() {
        try (Producer<String, Object> producer = kafkaTemplate.getProducerFactory().createProducer()) {
            producer.partitionsFor(properties.getKafka().getTopic());
            return Health.up().build();
        } catch (Exception ex) {
            return Health.down(ex).build();
        }
    }
}
