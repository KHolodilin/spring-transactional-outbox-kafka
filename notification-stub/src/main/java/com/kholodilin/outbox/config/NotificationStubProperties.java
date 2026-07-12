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

    /** Kafka consumer settings for the notification stub. */
    @Builder.Default
    private Kafka kafka = Kafka.builder().build();

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
