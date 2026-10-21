package com.kholodilin.outbox.config;

import com.kholodilin.outbox.events.EventEnvelope;
import com.kholodilin.outbox.inbound.EventEnvelopeDeserializer;
import com.kholodilin.outbox.inbound.InboundMessageParserRegistry;
import com.kholodilin.outbox.inbound.OrderPayloadParser;
import com.kholodilin.outbox.inbound.ServletEnvelopeParser;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import tools.jackson.databind.DeserializationFeature;
import tools.jackson.databind.json.JsonMapper;

import java.util.HashMap;
import java.util.Map;

/** Kafka batch listener setup for {@link com.kholodilin.outbox.notification.NotificationStubHandler}. */
@Configuration
@EnableConfigurationProperties(NotificationStubProperties.class)
public class KafkaConsumerConfig {

    /**
     * Builds a consumer factory whose value deserializer reads both envelope JSON and
     * Debezium {@code order-payload} records into {@link EventEnvelope}.
     *
     * @param properties  stub app config (topic / batch flags used elsewhere)
     * @param environment resolves {@code spring.kafka.bootstrap-servers} and group id
     */
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

        JsonMapper objectMapper = kafkaObjectMapper();
        EventEnvelopeDeserializer valueDeserializer = new EventEnvelopeDeserializer(
                objectMapper,
                new InboundMessageParserRegistry(
                        new ServletEnvelopeParser(objectMapper),
                        new OrderPayloadParser(objectMapper)
                )
        );

        return new DefaultKafkaConsumerFactory<>(config, new StringDeserializer(), valueDeserializer);
    }

    /**
     * Batch-oriented listener container matching the publisher's batch throughput model.
     *
     * @param consumerFactory factory from {@link #consumerFactory}
     * @param properties      controls {@code app.kafka.batch-listener}
     */
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
        // One consumer thread per orders.events partition so different customers run in parallel.
        factory.setConcurrency(10);
        return factory;
    }

    static JsonMapper kafkaObjectMapper() {
        return JsonMapper.builder()
                .configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false)
                .build();
    }
}
