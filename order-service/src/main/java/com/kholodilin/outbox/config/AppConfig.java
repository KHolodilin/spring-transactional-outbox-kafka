package com.kholodilin.outbox.config;

import tools.jackson.databind.json.JsonMapper;
import tools.jackson.databind.ObjectMapper;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.scheduling.annotation.EnableAsync;

/** Spring beans shared by the order service (JSON mapper, Kafka producer template). */
@Configuration
@EnableConfigurationProperties(AppProperties.class)
@EnableAsync
public class AppConfig {

    /**
     * Shared Jackson 3 {@link ObjectMapper} for HTTP bodies, idempotency JSON, and outbox payloads.
     */
    @Bean
    public ObjectMapper objectMapper() {
        return JsonMapper.builder().build();
    }
}
