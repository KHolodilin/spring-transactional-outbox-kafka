package com.kholodilin.outbox.config;

import com.kholodilin.outbox.events.EventEnvelope;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.kafka.support.serializer.JacksonJsonDeserializer;
import tools.jackson.databind.DeserializationFeature;
import tools.jackson.databind.json.JsonMapper;

import java.util.HashMap;
import java.util.Map;

/** Kafka batch listener setup for {@link com.kholodilin.outbox.notification.NotificationStubHandler}. */
@Configuration
@EnableConfigurationProperties(NotificationStubProperties.class)
public class KafkaConsumerConfig {

    @Bean
    public ConsumerFactory<String, EventEnvelope> consumerFactory(
            NotificationStubProperties properties,
            org.springframework.core.env.Environment environment
    ) {
        Map<String, Object> config = new HashMap<>();
        config.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, environment.getProperty("spring.kafka.bootstrap-servers"));
        config.put(ConsumerConfig.GROUP_ID_CONFIG, environment.getProperty("spring.kafka.consumer.group-id"));
        config.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        config.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, 100);

        JacksonJsonDeserializer<EventEnvelope> valueDeserializer =
                new JacksonJsonDeserializer<>(EventEnvelope.class, kafkaObjectMapper());
        valueDeserializer.addTrustedPackages("com.kholodilin.outbox.events");
        valueDeserializer.setUseTypeHeaders(false);

        return new DefaultKafkaConsumerFactory<>(config, new StringDeserializer(), valueDeserializer);
    }

    @Bean
    public ConcurrentKafkaListenerContainerFactory<String, EventEnvelope> batchKafkaListenerContainerFactory(
            ConsumerFactory<String, EventEnvelope> consumerFactory,
            NotificationStubProperties properties
    ) {
        ConcurrentKafkaListenerContainerFactory<String, EventEnvelope> factory =
                new ConcurrentKafkaListenerContainerFactory<>();
        factory.setConsumerFactory(consumerFactory);
        // Batch listener matches the publisher's batch-oriented throughput model.
        factory.setBatchListener(properties.getKafka().isBatchListener());
        return factory;
    }

    static JsonMapper kafkaObjectMapper() {
        return JsonMapper.builder()
                .configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false)
                .build();
    }
}
