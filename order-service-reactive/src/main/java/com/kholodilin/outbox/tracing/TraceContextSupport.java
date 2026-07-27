package com.kholodilin.outbox.tracing;

import io.micrometer.tracing.Span;
import io.micrometer.tracing.Tracer;
import io.micrometer.tracing.propagation.Propagator;
import org.springframework.beans.factory.ObjectProvider;
import org.springframework.stereotype.Component;

import java.util.HashMap;
import java.util.Map;
import java.util.function.Supplier;

/** Captures and restores W3C Trace Context for async Kafka publishing. */
@Component
public class TraceContextSupport {

    private static final String TRACE_PARENT_KEY = "traceparent";

    private final ObjectProvider<Tracer> tracerProvider;
    private final ObjectProvider<Propagator> propagatorProvider;

    public TraceContextSupport(ObjectProvider<Tracer> tracerProvider, ObjectProvider<Propagator> propagatorProvider) {
        this.tracerProvider = tracerProvider;
        this.propagatorProvider = propagatorProvider;
    }

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
