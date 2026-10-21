package com.kholodilin.outbox.config;

import com.kholodilin.outbox.events.EventEnvelope;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.env.Environment;
import org.springframework.kafka.support.serializer.ErrorHandlingDeserializer;
import org.springframework.kafka.support.serializer.JacksonJsonDeserializer;
import reactor.kafka.receiver.KafkaReceiver;
import reactor.kafka.receiver.ReceiverOptions;
import tools.jackson.databind.DeserializationFeature;
import tools.jackson.databind.json.JsonMapper;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

@Configuration
@EnableConfigurationProperties(NotificationStubProperties.class)
public class KafkaReceiverConfig {

    /** One receiver per {@code orders.events} partition so different customers run in parallel. */
    static final int CONSUMER_CONCURRENCY = 10;

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

        ErrorHandlingDeserializer<EventEnvelope> wrappingDeserializer =
                new ErrorHandlingDeserializer<>(valueDeserializer);

        return ReceiverOptions
                .<String, EventEnvelope>create(consumerProperties)
                .withKeyDeserializer(new StringDeserializer())
                .withValueDeserializer(wrappingDeserializer)
                .subscription(
                        List.of(properties.getKafka().getTopic())
                );
    }

    @Bean
    public List<KafkaReceiver<String, EventEnvelope>> kafkaReceivers(
            ReceiverOptions<String, EventEnvelope> receiverOptions
    ) {
        List<KafkaReceiver<String, EventEnvelope>> receivers = new ArrayList<>();
        for (int index = 0; index < CONSUMER_CONCURRENCY; index++) {
            receivers.add(KafkaReceiver.create(receiverOptions));
        }
        return receivers;
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