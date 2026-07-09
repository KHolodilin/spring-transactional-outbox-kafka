package com.kholodilin.outbox.config;

import org.springframework.boot.context.properties.ConfigurationProperties;

/** Typed configuration bound from {@code app.*} keys in notification-stub application.yml. */
@ConfigurationProperties(prefix = "app")
public class NotificationStubProperties {

    private Kafka kafka = new Kafka();

    public Kafka getKafka() {
        return kafka;
    }

    public void setKafka(Kafka kafka) {
        this.kafka = kafka;
    }

    /** Kafka consumer topic and batch-listener toggle. */
    public static class Kafka {
        private String topic = "orders.events";
        private boolean batchListener = true;

        public String getTopic() {
            return topic;
        }

        public void setTopic(String topic) {
            this.topic = topic;
        }

        public boolean isBatchListener() {
            return batchListener;
        }

        public void setBatchListener(boolean batchListener) {
            this.batchListener = batchListener;
        }
    }
}
