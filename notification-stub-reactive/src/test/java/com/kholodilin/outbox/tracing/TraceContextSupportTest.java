package com.kholodilin.outbox.tracing;

import io.micrometer.observation.ObservationRegistry;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.junit.jupiter.api.Test;
import reactor.core.publisher.Mono;
import reactor.test.StepVerifier;

import java.nio.charset.StandardCharsets;

class TraceContextSupportTest {

    private static final String TRACE_PARENT =
            "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01";

    private final TraceContextSupport support =
            new TraceContextSupport(ObservationRegistry.create());

    @Test
    void withRecordTraceCompletesAction() {
        ConsumerRecord<String, String> record =
                new ConsumerRecord<>("orders.events", 0, 0L, "1", "body");

        StepVerifier.create(
                        support.withRecordTrace(
                                record,
                                "notification.consume",
                                () -> Mono.just("ok")
                        )
                )
                .expectNext("ok")
                .verifyComplete();
    }

    @Test
    void withRecordTracePropagatesError() {
        ConsumerRecord<String, String> record =
                new ConsumerRecord<>("orders.events", 0, 1L, "1", "body");

        StepVerifier.create(
                        support.withRecordTrace(
                                record,
                                "notification.consume",
                                () -> Mono.error(new IllegalStateException("boom"))
                        )
                )
                .expectError(IllegalStateException.class)
                .verify();
    }

    @Test
    void withRecordTraceReadsTraceparentHeader() {
        ConsumerRecord<String, String> record =
                new ConsumerRecord<>("orders.events", 0, 2L, "1", "body");
        record.headers().add(
                "traceparent",
                TRACE_PARENT.getBytes(StandardCharsets.UTF_8)
        );

        StepVerifier.create(
                        support.withRecordTrace(
                                record,
                                "notification.batch.receive",
                                Mono::empty
                        )
                )
                .verifyComplete();
    }
}
