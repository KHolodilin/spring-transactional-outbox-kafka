package com.kholodilin.outbox.config;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

class AppPropertiesTest {

    @Test
    void builderProvidesDefaults() {
        AppProperties properties = AppProperties.builder().instanceId("pod-a").build();

        assertThat(properties.getInstanceId()).isEqualTo("pod-a");
        assertThat(properties.getKafka()).isNotNull();
        assertThat(properties.getOutbox()).isNotNull();
        assertThat(properties.getRateLimit()).isNotNull();
        assertThat(properties.getHealth()).isNotNull();
        assertThat(properties.getLogging()).isNotNull();
    }

    @Test
    void nestedPropertiesAreConfigurable() {
        AppProperties properties = AppProperties.builder()
                .kafka(KafkaProperties.builder().topic("custom-topic").build())
                .health(HealthProperties.builder().outboxPendingCritical(50).build())
                .build();

        assertThat(properties.getKafka().getTopic()).isEqualTo("custom-topic");
        assertThat(properties.getHealth().getOutboxPendingCritical()).isEqualTo(50);
    }
}
