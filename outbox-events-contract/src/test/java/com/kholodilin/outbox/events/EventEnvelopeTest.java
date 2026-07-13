package com.kholodilin.outbox.events;

import org.junit.jupiter.api.Test;

import java.time.Instant;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

class EventEnvelopeTest {

    @Test
    void builderPopulatesFields() {
        Instant now = Instant.parse("2026-01-01T00:00:00Z");
        EventEnvelope envelope = EventEnvelope.builder()
                .eventId(1L)
                .orderId(2L)
                .customerId(3L)
                .eventType(EventConstants.EVENT_TYPE_ORDER_CREATED)
                .payload(Map.of("orderId", 2))
                .correlationId("corr")
                .occurredAt(now)
                .traceParent("00-trace")
                .build();

        assertThat(envelope.getEventId()).isEqualTo(1L);
        assertThat(envelope.getEventType()).isEqualTo(EventConstants.EVENT_TYPE_ORDER_CREATED);
        assertThat(envelope.getTraceParent()).isEqualTo("00-trace");
        assertThat(envelope.getOccurredAt()).isEqualTo(now);
    }
}
