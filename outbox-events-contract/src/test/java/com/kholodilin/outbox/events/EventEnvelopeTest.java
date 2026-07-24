package com.kholodilin.outbox.events;

import org.junit.jupiter.api.Test;

import java.time.Instant;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

class EventEnvelopeTest {

    @Test
    void builderPopulatesFields() {
        Instant now = Instant.parse("2026-01-01T00:00:00Z");
        EventEnvelope envelope = new EventEnvelope(
                1L,
                2L,
                3L,
                EventConstants.EVENT_TYPE_ORDER_CREATED,
                Map.of("orderId", 2),
                "corr",
                now,
                "00-trace",
                null
        );

        assertThat(envelope.eventId()).isEqualTo(1L);
        assertThat(envelope.eventType()).isEqualTo(EventConstants.EVENT_TYPE_ORDER_CREATED);
        assertThat(envelope.traceParent()).isEqualTo("00-trace");
        assertThat(envelope.occurredAt()).isEqualTo(now);
    }
}
