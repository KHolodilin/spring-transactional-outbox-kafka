package com.kholodilin.outbox.config;

import com.kholodilin.outbox.events.EventEnvelope;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.env.Environment;
import org.springframework.kafka.support.serializer.JacksonJsonDeserializer;
import reactor.kafka.receiver.KafkaReceiver;
import reactor.kafka.receiver.ReceiverOptions;
import tools.jackson.databind.DeserializationFeature;
import tools.jackson.databind.json.JsonMapper;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

@Configuration
@EnableConfigurationProperties(NotificationStubProperties.class)
public class KafkaReceiverConfig {

    @Bean
    public ReceiverOptions<String, EventEnvelope> receiverOptions(
            NotificationStubProperties properties,
            Environment environment
    ) {

        Map<String, Object> consumerProperties = new HashMap<>();

        consumerProperties.put(
                ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG,
                environment.getProperty(
                        "spring.kafka.bootstrap-servers",
                        "localhost:9092"
                )
        );

        consumerProperties.put(
                ConsumerConfig.GROUP_ID_CONFIG,
                environment.getProperty(
                        "spring.kafka.consumer.group-id",
                        "notification-stub-reactive"
                )
        );

        consumerProperties.put(
                ConsumerConfig.AUTO_OFFSET_RESET_CONFIG,
                "earliest"
        );

        consumerProperties.put(
                ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG,
                false
        );

        consumerProperties.put(
                ConsumerConfig.MAX_POLL_RECORDS_CONFIG,
                properties.getKafka().getBatchSize()
        );

        JacksonJsonDeserializer<EventEnvelope> valueDeserializer =
                new JacksonJsonDeserializer<>(
                        EventEnvelope.class,
                        kafkaObjectMapper()
                );

        valueDeserializer.addTrustedPackages(
                "com.kholodilin.outbox.events"
        );

        valueDeserializer.setUseTypeHeaders(false);

        return ReceiverOptions
                .<String, EventEnvelope>create(consumerProperties)
                .withKeyDeserializer(new StringDeserializer())
                .withValueDeserializer(valueDeserializer)
                .subscription(
                        List.of(properties.getKafka().getTopic())
                );
    }

    @Bean
    public KafkaReceiver<String, EventEnvelope> kafkaReceiver(
            ReceiverOptions<String, EventEnvelope> receiverOptions
    ) {
        return KafkaReceiver.create(receiverOptions);
    }

    static JsonMapper kafkaObjectMapper() {
        return JsonMapper.builder()
                .configure(
                        DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES,
                        false
                )
                .build();
    }
}