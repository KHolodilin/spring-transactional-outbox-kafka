package com.kholodilin.outbox.config;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

class LoggingPropertiesTest {

    @Test
    void builderProvidesJsonDefaults() {
        LoggingProperties properties = LoggingProperties.builder().build();

        assertThat(properties.getJson().isEnabled()).isTrue();
        assertThat(properties.getJson().getPath()).isEqualTo("./logs");
    }
}
