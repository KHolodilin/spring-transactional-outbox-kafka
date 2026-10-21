package com.kholodilin.outbox.connect;

import io.opentelemetry.api.trace.StatusCode;
import io.opentelemetry.context.propagation.ContextPropagators;
import io.opentelemetry.sdk.OpenTelemetrySdk;
import io.opentelemetry.sdk.testing.exporter.InMemorySpanExporter;
import io.opentelemetry.sdk.trace.SdkTracerProvider;
import io.opentelemetry.sdk.trace.data.SpanData;
import io.opentelemetry.sdk.trace.export.SimpleSpanProcessor;
import io.opentelemetry.api.trace.propagation.W3CTraceContextPropagator;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.header.Header;
import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

class ConnectTraceProducerInterceptorTest {

    private static final String TRACEPARENT = "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01";

    @Test
    void continuesTheOrderTraceAndBecomesTheStubParent() {
        InMemorySpanExporter exporter = InMemorySpanExporter.create();
        ConnectTraceProducerInterceptor interceptor = interceptor(exporter);
        ProducerRecord<byte[], byte[]> record = record(TRACEPARENT);

        interceptor.onSend(record);
        interceptor.onAcknowledgement(null, null);

        String updated = header(record, ConnectTraceProducerInterceptor.TRACEPARENT);
        assertThat(updated).startsWith("00-4bf92f3577b34da6a3ce929d0e0e4736-");
        assertThat(updated).isNotEqualTo(TRACEPARENT);
        SpanData span = exporter.getFinishedSpanItems().get(0);
        assertThat(span.getName()).isEqualTo(ConnectTraceProducerInterceptor.SPAN_NAME);
        assertThat(span.getParentSpanId()).isEqualTo("00f067aa0ba902b7");
        assertThat(updated).contains(span.getSpanId());
        assertThat(span.getAttributes().get(io.opentelemetry.api.common.AttributeKey.stringKey("messaging.destination.name")))
                .isEqualTo("orders.events");
    }

    @Test
    void sampledFlag03StillContinuesTheTrace() {
        InMemorySpanExporter exporter = InMemorySpanExporter.create();
        ConnectTraceProducerInterceptor interceptor = interceptor(exporter);
        String traceParent = "00-d291382826ed9265a1e94fbb05713686-fbe8a29fa7e7ebaf-03";
        ProducerRecord<byte[], byte[]> record = record(traceParent);

        interceptor.onSend(record);
        interceptor.onAcknowledgement(null, null);

        String updated = header(record, ConnectTraceProducerInterceptor.TRACEPARENT);
        assertThat(updated).startsWith("00-d291382826ed9265a1e94fbb05713686-");
        assertThat(updated).isNotEqualTo(traceParent);
        assertThat(exporter.getFinishedSpanItems().get(0).getParentSpanId()).isEqualTo("fbe8a29fa7e7ebaf");
    }

    @Test
    void configureReadsTraceParentWithTheW3cPropagator() {
        System.setProperty("otel.traces.endpoint", "http://127.0.0.1:9/v1/traces");
        ConnectTraceProducerInterceptor interceptor = new ConnectTraceProducerInterceptor();
        try {
            interceptor.configure(Map.of());
            String traceParent = "00-d291382826ed9265a1e94fbb05713686-fbe8a29fa7e7ebaf-03";
            ProducerRecord<byte[], byte[]> record = record(traceParent);

            interceptor.onSend(record);
            interceptor.onAcknowledgement(null, null);

            assertThat(header(record, ConnectTraceProducerInterceptor.TRACEPARENT))
                    .startsWith("00-d291382826ed9265a1e94fbb05713686-")
                    .isNotEqualTo(traceParent);
        } finally {
            interceptor.close();
            System.clearProperty("otel.traces.endpoint");
        }
    }

    @Test
    void acknowledgementErrorMarksTheSpan() {
        InMemorySpanExporter exporter = InMemorySpanExporter.create();
        ConnectTraceProducerInterceptor interceptor = interceptor(exporter);

        interceptor.onSend(record(TRACEPARENT));
        interceptor.onAcknowledgement(null, new IllegalStateException("broker down"));

        SpanData span = exporter.getFinishedSpanItems().get(0);
        assertThat(span.getStatus().getStatusCode()).isEqualTo(StatusCode.ERROR);
        assertThat(span.getEvents()).isNotEmpty();
    }

    @Test
    void recordWithoutTraceParentIsUnchanged() {
        InMemorySpanExporter exporter = InMemorySpanExporter.create();
        ConnectTraceProducerInterceptor interceptor = interceptor(exporter);
        ProducerRecord<byte[], byte[]> plain = new ProducerRecord<>("orders.events", "k".getBytes(StandardCharsets.UTF_8), "{}".getBytes(StandardCharsets.UTF_8));
        ProducerRecord<byte[], byte[]> traced = record(TRACEPARENT);

        interceptor.onSend(traced);
        interceptor.onSend(plain);
        interceptor.onAcknowledgement(null, null);
        interceptor.onAcknowledgement(null, null);

        assertThat(plain.headers().toArray()).isEmpty();
        assertThat(exporter.getFinishedSpanItems()).hasSize(1);
    }

    @Test
    void invalidTraceParentDoesNotRewriteTheHeader() {
        InMemorySpanExporter exporter = InMemorySpanExporter.create();
        ConnectTraceProducerInterceptor interceptor = interceptor(exporter);
        ProducerRecord<byte[], byte[]> record = record("not-a-traceparent");

        interceptor.onSend(record);
        interceptor.onAcknowledgement(null, null);

        assertThat(header(record, ConnectTraceProducerInterceptor.TRACEPARENT)).isEqualTo("not-a-traceparent");
        assertThat(exporter.getFinishedSpanItems()).isEmpty();
    }

    private static ConnectTraceProducerInterceptor interceptor(InMemorySpanExporter exporter) {
        SdkTracerProvider provider = SdkTracerProvider.builder()
                .addSpanProcessor(SimpleSpanProcessor.create(exporter))
                .build();
        OpenTelemetrySdk sdk = OpenTelemetrySdk.builder()
                .setTracerProvider(provider)
                .setPropagators(ContextPropagators.create(W3CTraceContextPropagator.getInstance()))
                .build();
        return new ConnectTraceProducerInterceptor(sdk);
    }

    private static ProducerRecord<byte[], byte[]> record(String traceParent) {
        ProducerRecord<byte[], byte[]> record = new ProducerRecord<>(
                "orders.events",
                "42".getBytes(StandardCharsets.UTF_8),
                "{}".getBytes(StandardCharsets.UTF_8)
        );
        record.headers().add(ConnectTraceProducerInterceptor.TRACEPARENT, traceParent.getBytes(StandardCharsets.UTF_8));
        return record;
    }

    private static String header(ProducerRecord<byte[], byte[]> record, String key) {
        Header header = record.headers().lastHeader(key);
        return new String(header.value(), StandardCharsets.UTF_8);
    }
}
