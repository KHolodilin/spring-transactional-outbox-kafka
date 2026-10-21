package com.kholodilin.outbox.tracing;

import io.micrometer.tracing.Span;
import io.micrometer.tracing.Tracer;
import io.micrometer.tracing.propagation.Propagator;
import org.springframework.beans.factory.ObjectProvider;
import org.springframework.stereotype.Component;

import java.util.HashMap;
import java.util.Map;
import java.util.function.Supplier;

/**
 * Captures the active W3C {@code traceparent} so a later CDC publisher can continue this HTTP trace.
 * <p>
 * There is no in-app Kafka publish, so this service only stores the header on the outbox row.
 * When tracing beans are absent, {@link #captureTraceParent()} returns {@code null} and the insert still proceeds.
 */
@Component
public class TraceContextSupport {

    private static final String TRACE_PARENT_KEY = "traceparent";

    private final ObjectProvider<Tracer> tracerProvider;
    private final ObjectProvider<Propagator> propagatorProvider;

    public TraceContextSupport(ObjectProvider<Tracer> tracerProvider, ObjectProvider<Propagator> propagatorProvider) {
        this.tracerProvider = tracerProvider;
        this.propagatorProvider = propagatorProvider;
    }

    /**
     * Serializes the current span as a W3C {@code traceparent}.
     *
     * @return header value, or {@code null} when tracing is off or no span is active
     */
    public String captureTraceParent() {
        Tracer tracer = tracerProvider.getIfAvailable();
        Propagator propagator = propagatorProvider.getIfAvailable();
        if (tracer == null || propagator == null) {
            return null;
        }
        Span current = tracer.currentSpan();
        if (current == null) {
            return null;
        }
        Map<String, String> carrier = new HashMap<>();
        propagator.inject(current.context(), carrier, Map::put);
        return carrier.get(TRACE_PARENT_KEY);
    }

    /**
     * Runs {@code action} under a child span and always closes that span.
     * <p>
     * A blank {@code traceParent} continues the span already on this thread, which is the HTTP
     * request when {@code db.persist} starts. When tracing beans are absent, {@code action} runs
     * with no span.
     *
     * @param traceParent W3C header, or {@code null} to continue the current trace
     * @param spanName    span name
     * @param action      work to measure; exceptions propagate after the span ends
     * @param <T>         result type
     * @return value produced by {@code action}
     */
    public <T> T runWithTraceParent(String traceParent, String spanName, Supplier<T> action) {
        Tracer tracer = tracerProvider.getIfAvailable();
        Propagator propagator = propagatorProvider.getIfAvailable();
        if (tracer == null || propagator == null) {
            return action.get();
        }
        Span span = startSpan(tracer, propagator, traceParent, spanName);
        try (Tracer.SpanInScope scope = tracer.withSpan(span)) {
            return action.get();
        } finally {
            span.end();
        }
    }

    private Span startSpan(Tracer tracer, Propagator propagator, String traceParent, String spanName) {
        if (traceParent == null || traceParent.isBlank()) {
            return tracer.nextSpan().name(spanName).start();
        }
        Map<String, String> carrier = Map.of(TRACE_PARENT_KEY, traceParent);
        return propagator.extract(carrier, Map::get).name(spanName).start();
    }
}
