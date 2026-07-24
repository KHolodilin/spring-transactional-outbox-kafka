package com.kholodilin.outbox.config;

import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringSerializer;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.env.Environment;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.core.ProducerFactory;
import org.springframework.kafka.support.serializer.JacksonJsonSerializer;
import tools.jackson.databind.cfg.DateTimeFeature;
import tools.jackson.databind.json.JsonMapper;

import java.util.HashMap;
import java.util.Map;

/** Kafka producer with Jackson 3 {@link JacksonJsonSerializer} ({@code java.time} support is built into Jackson 3). */
@Configuration
public class KafkaProducerConfig {

    /**
     * Builds a producer factory with String keys and Jackson JSON values (no type headers).
     *
     * @param environment resolves {@code spring.kafka.*} bootstrap and ack settings
     */
    @Bean
    public ProducerFactory<String, Object> producerFactory(Environment environment) {
        Map<String, Object> config = new HashMap<>();
        config.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, environment.getProperty("spring.kafka.bootstrap-servers"));
        config.put(ProducerConfig.ACKS_CONFIG, environment.getProperty("spring.kafka.producer.acks", "all"));
        config.put(ProducerConfig.MAX_BLOCK_MS_CONFIG, 10_000);
        config.put(ProducerConfig.REQUEST_TIMEOUT_MS_CONFIG, 30_000);
        config.put(ProducerConfig.DELIVERY_TIMEOUT_MS_CONFIG, 60_000);
        config.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);

        JacksonJsonSerializer<Object> valueSerializer = new JacksonJsonSerializer<>(kafkaObjectMapper());
        valueSerializer.setAddTypeInfo(false);
        return new DefaultKafkaProducerFactory<>(config, new StringSerializer(), valueSerializer);
    }

    /**
     * @param producerFactory factory from {@link #producerFactory(Environment)}
     * @return template used by {@link com.kholodilin.outbox.publisher.KafkaBatchPublisher}
     */
    @Bean
    public KafkaTemplate<String, Object> kafkaTemplate(ProducerFactory<String, Object> producerFactory) {
        return new KafkaTemplate<>(producerFactory);
    }

    /**
     * Jackson mapper for Kafka values: ISO-8601 dates, no timestamps.
     */
    public static JsonMapper kafkaObjectMapper() {
        return JsonMapper.builder()
                .configure(DateTimeFeature.WRITE_DATES_AS_TIMESTAMPS, false)
                .build();
    }
}
