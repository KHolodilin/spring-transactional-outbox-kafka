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
    void putOrderFields_populatesCanonicalNames() {
        StructuredLogContext.putOrderFields(100L, 200L);

        assertThat(MDC.get("order.id")).isEqualTo("100");
        assertThat(MDC.get("outbox.id")).isEqualTo("200");
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
    void enrichTracingAliases_copiesTraceFields() {
        MDC.put("traceId", "abc123");
        MDC.put("spanId", "def456");

        StructuredLogContext.enrichTracingAliases();

        assertThat(MDC.get("trace.id")).isEqualTo("abc123");
        assertThat(MDC.get("span.id")).isEqualTo("def456");
    }
}
