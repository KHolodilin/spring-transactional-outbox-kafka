package com.kholodilin.outbox.connect;

import io.opentelemetry.api.OpenTelemetry;
import io.opentelemetry.api.common.AttributeKey;
import io.opentelemetry.api.common.Attributes;
import io.opentelemetry.api.trace.Span;
import io.opentelemetry.api.trace.SpanKind;
import io.opentelemetry.api.trace.StatusCode;
import io.opentelemetry.api.trace.Tracer;
import io.opentelemetry.api.trace.propagation.W3CTraceContextPropagator;
import io.opentelemetry.context.Context;
import io.opentelemetry.context.propagation.ContextPropagators;
import io.opentelemetry.context.propagation.TextMapGetter;
import io.opentelemetry.context.propagation.TextMapPropagator;
import io.opentelemetry.context.propagation.TextMapSetter;
import io.opentelemetry.exporter.otlp.http.trace.OtlpHttpSpanExporter;
import io.opentelemetry.sdk.OpenTelemetrySdk;
import io.opentelemetry.sdk.resources.Resource;
import io.opentelemetry.sdk.trace.SdkTracerProvider;
import io.opentelemetry.sdk.trace.export.BatchSpanProcessor;
import io.opentelemetry.sdk.trace.export.SpanExporter;
import org.apache.kafka.clients.producer.ProducerInterceptor;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.header.Headers;

import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Opens a {@code connect.publish} span as a child of the outbox {@code traceparent} header.
 * <p>
 * The header keeps the same trace id and adopts this span id, so the notification stub
 * continues the chain under Connect instead of directly under the HTTP span.
 */
public class ConnectTraceProducerInterceptor implements ProducerInterceptor<byte[], byte[]> {

    static final String TRACEPARENT = "traceparent";
    static final String TRACESTATE = "tracestate";
    static final String SPAN_NAME = "connect.publish";

    private static final Logger LOG = Logger.getLogger(ConnectTraceProducerInterceptor.class.getName());
    private static final AttributeKey<String> SERVICE_NAME = AttributeKey.stringKey("service.name");

    private final ConcurrentLinkedQueue<Span> pending = new ConcurrentLinkedQueue<>();

    private Tracer tracer;
    private TextMapPropagator propagator;
    private SdkTracerProvider tracerProvider;

    public ConnectTraceProducerInterceptor() {
    }

    ConnectTraceProducerInterceptor(OpenTelemetry openTelemetry) {
        this.tracer = openTelemetry.getTracer("debezium-connect");
        this.propagator = openTelemetry.getPropagators().getTextMapPropagator();
    }

    @Override
    public void configure(Map<String, ?> configs) {
        if (tracer != null) {
            return;
        }
        try {
            String endpoint = endpoint();
            String serviceName = serviceName();
            SpanExporter exporter = OtlpHttpSpanExporter.builder().setEndpoint(endpoint).build();
            tracerProvider = SdkTracerProvider.builder()
                    .setResource(Resource.getDefault().merge(Resource.create(Attributes.of(SERVICE_NAME, serviceName))))
                    .addSpanProcessor(BatchSpanProcessor.builder(exporter)
                            .setScheduleDelay(Duration.ofSeconds(1))
                            .build())
                    .build();
            OpenTelemetrySdk sdk = OpenTelemetrySdk.builder()
                    .setTracerProvider(tracerProvider)
                    .setPropagators(ContextPropagators.create(W3CTraceContextPropagator.getInstance()))
                    .build();
            tracer = sdk.getTracer("debezium-connect");
            propagator = sdk.getPropagators().getTextMapPropagator();
            LOG.info("Connect trace interceptor exports to " + endpoint);
        } catch (RuntimeException ex) {
            LOG.log(Level.WARNING, "Connect trace interceptor is disabled", ex);
            tracer = null;
        }
    }

    @Override
    public ProducerRecord<byte[], byte[]> onSend(ProducerRecord<byte[], byte[]> record) {
        Span span = startSpan(record);
        pending.add(span);
        return record;
    }

    @Override
    public void onAcknowledgement(RecordMetadata metadata, Exception exception) {
        Span span = pending.poll();
        if (span == null || !span.isRecording()) {
            return;
        }
        if (exception != null) {
            span.recordException(exception);
            span.setStatus(StatusCode.ERROR);
        }
        span.end();
    }

    @Override
    public void close() {
        if (tracerProvider != null) {
            tracerProvider.close();
        }
    }

    private Span startSpan(ProducerRecord<byte[], byte[]> record) {
        if (tracer == null || propagator == null) {
            return Span.getInvalid();
        }
        String traceParent = header(record.headers(), TRACEPARENT);
        if (traceParent == null || traceParent.isBlank()) {
            return Span.getInvalid();
        }
        Map<String, String> incoming = new HashMap<>();
        incoming.put(TRACEPARENT, traceParent);
        String traceState = header(record.headers(), TRACESTATE);
        if (traceState != null) {
            incoming.put(TRACESTATE, traceState);
        }
        Context parent = propagator.extract(Context.root(), incoming, GETTER);
        if (!Span.fromContext(parent).getSpanContext().isValid()) {
            return Span.getInvalid();
        }
        Span span = tracer.spanBuilder(SPAN_NAME)
                .setParent(parent)
                .setSpanKind(SpanKind.PRODUCER)
                .startSpan();
        if (record.topic() != null) {
            span.setAttribute("messaging.destination.name", record.topic());
        }
        span.setAttribute("messaging.system", "kafka");
        Map<String, String> outgoing = new HashMap<>();
        propagator.inject(parent.with(span), outgoing, SETTER);
        replaceHeader(record.headers(), TRACEPARENT, outgoing.get(TRACEPARENT));
        replaceHeader(record.headers(), TRACESTATE, outgoing.get(TRACESTATE));
        return span;
    }

    private static String endpoint() {
        String configured = System.getenv("OTEL_EXPORTER_OTLP_TRACES_ENDPOINT");
        if (configured == null || configured.isBlank()) {
            configured = System.getProperty("otel.traces.endpoint");
        }
        if (configured == null || configured.isBlank()) {
            return "http://tempo:4318/v1/traces";
        }
        return configured;
    }

    private static String serviceName() {
        String configured = System.getenv("OTEL_SERVICE_NAME");
        if (configured == null || configured.isBlank()) {
            return "debezium-connect";
        }
        return configured;
    }

    private static String header(Headers headers, String key) {
        Header header = headers.lastHeader(key);
        if (header == null || header.value() == null) {
            return null;
        }
        return new String(header.value(), StandardCharsets.UTF_8);
    }

    private static void replaceHeader(Headers headers, String key, String value) {
        if (value == null) {
            return;
        }
        headers.remove(key);
        headers.add(key, value.getBytes(StandardCharsets.UTF_8));
    }

    private static final TextMapGetter<Map<String, String>> GETTER = new TextMapGetter<>() {
        @Override
        public Iterable<String> keys(Map<String, String> carrier) {
            return carrier == null ? Collections.emptyList() : carrier.keySet();
        }

        @Override
        public String get(Map<String, String> carrier, String key) {
            if (carrier == null) {
                return null;
            }
            return carrier.get(key);
        }
    };

    private static final TextMapSetter<Map<String, String>> SETTER = Map::put;
}
