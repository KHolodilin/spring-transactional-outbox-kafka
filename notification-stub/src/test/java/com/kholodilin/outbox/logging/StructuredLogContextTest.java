package com.kholodilin.outbox.logging;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.slf4j.MDC;

import static org.assertj.core.api.Assertions.assertThat;

class StructuredLogContextTest {

    @AfterEach
    void tearDown() {
        MDC.clear();
    }

    @Test
    void putNotificationFields_populatesMdc() {
        StructuredLogContext.putNotificationFields("log", "sent");

        assertThat(MDC.get("notification.channel")).isEqualTo("log");
        assertThat(MDC.get("notification.status")).isEqualTo("sent");
    }

    @Test
    void clearConsumerContext_removesKafkaFields() {
        StructuredLogContext.putKafkaFields("orders.events", 0, 10L);
        StructuredLogContext.putEventAction("notification.processed");

        StructuredLogContext.clearConsumerContext();

        assertThat(MDC.get("kafka.topic")).isNull();
        assertThat(MDC.get("event.action")).isNull();
    }
}
