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

    /** Unique pod/instance id used as {@code locked_by} for outbox leases. */
    @Builder.Default
    private String instanceId = "local";

    /** Kafka topic and related producer settings. */
    @Builder.Default
    private KafkaProperties kafka = KafkaProperties.builder().build();

    /** In-memory queue, publisher, and recovery worker settings. */
    @Builder.Default
    private OutboxProperties outbox = OutboxProperties.builder().build();

    /** HTTP rate-limit buckets and adaptive throttling settings. */
    @Builder.Default
    private RateLimitProperties rateLimit = RateLimitProperties.builder().build();

    /** Thresholds used by custom Actuator health indicators. */
    @Builder.Default
    private HealthProperties health = HealthProperties.builder().build();
}
