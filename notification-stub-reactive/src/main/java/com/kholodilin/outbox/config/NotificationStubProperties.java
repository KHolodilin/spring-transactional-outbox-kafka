package com.kholodilin.outbox.config;

import lombok.Getter;
import lombok.Setter;
import org.springframework.boot.context.properties.ConfigurationProperties;

import java.time.Duration;

@Getter
@Setter
@ConfigurationProperties(prefix = "app")
public class NotificationStubProperties {

    private String instanceId = "local";
    private Kafka kafka = new Kafka();
    private LoggingProperties logging = LoggingProperties.builder().build();

    @Getter
    @Setter
    public static class Kafka {

        private String topic = "orders.events";
        private int batchSize = 100;
        private Duration batchWait = Duration.ofMillis(50);
    }
}