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
    void putCorrelation_populatesMdcFields() {
        StructuredLogContext.putCorrelation("corr-1", 42L, "idem-key");

        assertThat(MDC.get("correlationId")).isEqualTo("corr-1");
        assertThat(MDC.get("customerId")).isEqualTo("42");
        assertThat(MDC.get("customer.id")).isEqualTo("42");
        assertThat(MDC.get("idempotencyKey")).isEqualTo("idem-key");
    }

    @Test
    void putCorrelation_nullGuardsDoNotPutMdc() {
        StructuredLogContext.putCorrelation(null, null, null);

        assertThat(MDC.get("correlationId")).isNull();
        assertThat(MDC.get("customerId")).isNull();
        assertThat(MDC.get("customer.id")).isNull();
        assertThat(MDC.get("idempotencyKey")).isNull();
    }

    @Test
    void putOrderFields_populatesCanonicalNames() {
        StructuredLogContext.putOrderFields(100L, 200L);

        assertThat(MDC.get("order.id")).isEqualTo("100");
        assertThat(MDC.get("outbox.id")).isEqualTo("200");
        assertThat(MDC.get("orderId")).isEqualTo("100");
        assertThat(MDC.get("eventId")).isEqualTo("200");
    }

    @Test
    void putOrderFields_nullGuardsDoNotPutMdc() {
        StructuredLogContext.putOrderFields(null, null);

        assertThat(MDC.get("order.id")).isNull();
        assertThat(MDC.get("outbox.id")).isNull();
        assertThat(MDC.get("orderId")).isNull();
        assertThat(MDC.get("eventId")).isNull();
    }

    @Test
    void clearRequestContext_removesRequestScopedFields() {
        StructuredLogContext.putCorrelation("corr-1", 42L, "idem-key");
        StructuredLogContext.putEventAction("http.request.accepted");
        StructuredLogContext.putOrderFields(1L, 2L);

        StructuredLogContext.clearRequestContext();

        assertThat(MDC.get("correlationId")).isNull();
        assertThat(MDC.get("event.action")).isNull();
        assertThat(MDC.get("order.id")).isNull();
    }

    @Test
    void putOutboxStatusAndKafkaFieldsPopulateMdc() {
        StructuredLogContext.putOutboxStatus("FAILED", 2, 3);
        StructuredLogContext.putBatchSize(5);
        StructuredLogContext.putDurationMs(99);
        StructuredLogContext.putKafkaFields("orders.events", 1, 42L);
        StructuredLogContext.putEventType("OrderCreated");
        StructuredLogContext.putInstanceFields("pod-1");

        assertThat(MDC.get("outbox.status")).isEqualTo("FAILED");
        assertThat(MDC.get("outbox.status_code")).isEqualTo("2");
        assertThat(MDC.get("outbox.retry_count")).isEqualTo("3");
        assertThat(MDC.get("outbox.batch_size")).isEqualTo("5");
        assertThat(MDC.get("duration.ms")).isEqualTo("99");
        assertThat(MDC.get("kafka.topic")).isEqualTo("orders.events");
        assertThat(MDC.get("kafka.partition")).isEqualTo("1");
        assertThat(MDC.get("kafka.offset")).isEqualTo("42");
        assertThat(MDC.get("event.type")).isEqualTo("OrderCreated");
        assertThat(MDC.get("instance.id")).isEqualTo("pod-1");
        assertThat(MDC.get("locked_by")).isEqualTo("pod-1");
    }

    @Test
    void putOutboxStatusAndKafkaFields_nullGuardsDoNotPutMdc() {
        StructuredLogContext.putOutboxStatus(null, null, null);
        StructuredLogContext.putKafkaFields(null, null, null);
        StructuredLogContext.putEventType(null);
        StructuredLogContext.putInstanceFields(null);

        assertThat(MDC.get("outbox.status")).isNull();
        assertThat(MDC.get("outbox.status_code")).isNull();
        assertThat(MDC.get("outbox.retry_count")).isNull();
        assertThat(MDC.get("kafka.topic")).isNull();
        assertThat(MDC.get("kafka.partition")).isNull();
        assertThat(MDC.get("kafka.offset")).isNull();
        assertThat(MDC.get("event.type")).isNull();
        assertThat(MDC.get("instance.id")).isNull();
        assertThat(MDC.get("locked_by")).isNull();
    }
}
