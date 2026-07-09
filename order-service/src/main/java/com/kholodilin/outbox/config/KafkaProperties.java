package com.kholodilin.outbox.config;

/** Kafka topic and publisher settings. */
public class KafkaProperties {

    private String topic = "orders.events";

    public String getTopic() {
        return topic;
    }

    public void setTopic(String topic) {
        this.topic = topic;
    }
}
