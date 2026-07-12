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
 * Restores W3C trace context from Kafka headers for downstream consumption.
 * No-ops when Micrometer tracing is disabled.
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

    public void runWithTraceParent(String traceParent, String spanName, Runnable action) {
        runWithTraceParent(traceParent, spanName, () -> {
            action.run();
            return null;
        });
    }

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
