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

    @Test
    void putCorrelationAndOrderFieldsPopulateMdc() {
        StructuredLogContext.putCorrelation("corr", 7L);
        StructuredLogContext.putOrderFields(11L, 22L);
        StructuredLogContext.putEventType("OrderCreated");
        StructuredLogContext.putDurationMs(15);
        StructuredLogContext.putBatchSize(3);
        StructuredLogContext.putInstanceFields("pod-1");

        assertThat(MDC.get("correlationId")).isEqualTo("corr");
        assertThat(MDC.get("customerId")).isEqualTo("7");
        assertThat(MDC.get("order.id")).isEqualTo("11");
        assertThat(MDC.get("event.type")).isEqualTo("OrderCreated");
        assertThat(MDC.get("duration.ms")).isEqualTo("15");
        assertThat(MDC.get("outbox.batch_size")).isEqualTo("3");
        assertThat(MDC.get("instance.id")).isEqualTo("pod-1");
    }

    @Test
    void enrichTracingAliasesCopiesTraceFields() {
        MDC.put("traceId", "trace");
        MDC.put("spanId", "span");

        StructuredLogContext.enrichTracingAliases();

        assertThat(MDC.get("trace.id")).isEqualTo("trace");
        assertThat(MDC.get("span.id")).isEqualTo("span");
    }
}
