package com.kholodilin.outbox.health;

import com.kholodilin.outbox.config.AppProperties;
import lombok.RequiredArgsConstructor;
import org.apache.kafka.clients.producer.Producer;
import org.springframework.boot.health.contributor.Health;
import org.springframework.boot.health.contributor.HealthIndicator;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Component;

/** Verifies that the Kafka cluster is reachable and the outbox topic exists. */
@Component
@RequiredArgsConstructor
public class KafkaHealthIndicator implements HealthIndicator {

    private final KafkaTemplate<String, Object> kafkaTemplate;
    private final AppProperties properties;

    /**
     * Probes Kafka by creating a short-lived producer and listing partitions for the configured topic.
     *
     * @return UP when the topic metadata is reachable; DOWN with the exception otherwise
     */
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
