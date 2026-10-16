package com.kholodilin.outbox.config;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

class NotificationStubPropertiesTest {

    @Test
    void builderProvidesDefaults() {
        NotificationStubProperties properties = NotificationStubProperties.builder()
                .instanceId("stub-pod")
                .build();

        assertThat(properties.getInstanceId()).isEqualTo("stub-pod");
        assertThat(properties.getKafka().getTopic()).isEqualTo("orders.events");
        assertThat(properties.getKafka().isBatchListener()).isTrue();
        assertThat(properties.getLogging()).isNotNull();
    }
}
