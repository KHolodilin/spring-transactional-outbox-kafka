package com.kholodilin.outbox.config;

import tools.jackson.databind.json.JsonMapper;
import tools.jackson.databind.ObjectMapper;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Primary;

/** Spring beans shared by the order service (JSON mapper, Kafka producer template). */
@Configuration
@EnableConfigurationProperties(AppProperties.class)
public class AppConfig {

    /**
     * Shared Jackson 3 {@link ObjectMapper} for HTTP bodies and outbox payloads.
     * <p>
     * Marked {@code @Primary} so it wins over the starter's {@code outboxJsonMapper}.
     */
    @Bean
    @Primary
    public ObjectMapper objectMapper() {
        return JsonMapper.builder().build();
    }
}
