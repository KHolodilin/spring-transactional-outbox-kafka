package com.kholodilin.outbox.config;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

class NotificationStubPropertiesTest {

    @Test
    void defaultsIncludeLoggingAndKafkaTopic() {
        NotificationStubProperties properties = new NotificationStubProperties();

        assertThat(properties.getInstanceId()).isEqualTo("local");
        assertThat(properties.getKafka().getTopic()).isEqualTo("orders.events");
        assertThat(properties.getKafka().getBatchSize()).isEqualTo(100);
        assertThat(properties.getLogging()).isNotNull();
        assertThat(properties.getLogging().getJson().isEnabled()).isTrue();
        assertThat(properties.getLogging().getJson().getPath()).isEqualTo("./logs");
    }
}
