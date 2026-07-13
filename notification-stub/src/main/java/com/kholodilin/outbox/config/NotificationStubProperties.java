package com.kholodilin.outbox.config;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;
import org.springframework.boot.context.properties.ConfigurationProperties;

/** Typed configuration bound from {@code app.*} keys in notification-stub application.yml. */
@ConfigurationProperties(prefix = "app")
@Getter
@Setter
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class NotificationStubProperties {

    /** Unique pod/instance id for structured logs. */
    @Builder.Default
    private String instanceId = "local";

    /** Kafka consumer settings for the notification stub. */
    @Builder.Default
    private Kafka kafka = Kafka.builder().build();

    /** JSON file logging for centralized log shipping. */
    @Builder.Default
    private LoggingProperties logging = LoggingProperties.builder().build();

    /** Kafka consumer topic and batch-listener toggle. */
    @Getter
    @Setter
    @Builder
    @NoArgsConstructor
    @AllArgsConstructor
    public static class Kafka {

        /** Topic consumed by the notification stub. */
        @Builder.Default
        private String topic = "orders.events";

        /** When {@code true}, the listener receives batches of {@link com.kholodilin.outbox.events.EventEnvelope}. */
        @Builder.Default
        private boolean batchListener = true;
    }
}
