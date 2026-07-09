package com.kholodilin.outbox.config;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.kafka.support.serializer.JsonDeserializer;

import java.util.HashMap;
import java.util.Map;

/** Kafka batch listener setup for {@link com.kholodilin.outbox.notification.NotificationStubHandler}. */
@Configuration
@EnableConfigurationProperties(NotificationStubProperties.class)
public class KafkaConsumerConfig {

    @Bean
    public ConsumerFactory<String, com.kholodilin.outbox.events.EventEnvelope> consumerFactory(
            NotificationStubProperties properties,
            org.springframework.core.env.Environment environment
    ) {
        Map<String, Object> config = new HashMap<>();
        config.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, environment.getProperty("spring.kafka.bootstrap-servers"));
        config.put(ConsumerConfig.GROUP_ID_CONFIG, environment.getProperty("spring.kafka.consumer.group-id"));
        config.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        config.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, JsonDeserializer.class);
        config.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        config.put(JsonDeserializer.TRUSTED_PACKAGES, "com.kholodilin.outbox.events");
        config.put(JsonDeserializer.VALUE_DEFAULT_TYPE, com.kholodilin.outbox.events.EventEnvelope.class.getName());
        config.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, 100);
        return new DefaultKafkaConsumerFactory<>(config);
    }

    @Bean
    public ConcurrentKafkaListenerContainerFactory<String, com.kholodilin.outbox.events.EventEnvelope> batchKafkaListenerContainerFactory(
            ConsumerFactory<String, com.kholodilin.outbox.events.EventEnvelope> consumerFactory,
            NotificationStubProperties properties
    ) {
        ConcurrentKafkaListenerContainerFactory<String, com.kholodilin.outbox.events.EventEnvelope> factory =
                new ConcurrentKafkaListenerContainerFactory<>();
        factory.setConsumerFactory(consumerFactory);
        // Batch listener matches the publisher's batch-oriented throughput model.
        factory.setBatchListener(properties.getKafka().isBatchListener());
        return factory;
    }
}
