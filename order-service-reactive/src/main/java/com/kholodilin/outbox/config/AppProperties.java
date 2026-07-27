package com.kholodilin.outbox.config;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;
import org.springframework.boot.context.properties.ConfigurationProperties;

/** Typed configuration bound from {@code app.*} keys in application.yml. */
@ConfigurationProperties(prefix = "app")
@Getter
@Setter
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class AppProperties {

    @Builder.Default
    private String instanceId = "local";

    @Builder.Default
    private KafkaProperties kafka = KafkaProperties.builder().build();

    @Builder.Default
    private OutboxProperties outbox = OutboxProperties.builder().build();

    @Builder.Default
    private RateLimitProperties rateLimit = RateLimitProperties.builder().build();

    @Builder.Default
    private HealthProperties health = HealthProperties.builder().build();

    @Builder.Default
    private LoggingProperties logging = LoggingProperties.builder().build();
}
