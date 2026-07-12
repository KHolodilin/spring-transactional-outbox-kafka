package com.kholodilin.outbox.tracing;

import io.micrometer.tracing.Span;
import io.micrometer.tracing.Tracer;
import org.springframework.beans.factory.ObjectProvider;
import org.springframework.stereotype.Component;

import java.util.function.Supplier;

/**
 * Dedicated spans for the outbox publishing pipeline (separate from Micrometer metrics).
 * No-ops when Micrometer tracing is disabled.
 */
@Component
public class OutboxTracing {

    private final ObjectProvider<Tracer> tracerProvider;
    private final TraceContextSupport traceContextSupport;

    public OutboxTracing(ObjectProvider<Tracer> tracerProvider, TraceContextSupport traceContextSupport) {
        this.tracerProvider = tracerProvider;
        this.traceContextSupport = traceContextSupport;
    }

    public void observe(String spanName, Runnable action) {
        observe(spanName, () -> {
            action.run();
            return null;
        });
    }

    public <T> T observe(String spanName, Supplier<T> action) {
        Tracer tracer = tracerProvider.getIfAvailable();
        if (tracer == null) {
            return action.get();
        }
        Span span = tracer.nextSpan().name(spanName).start();
        try (Tracer.SpanInScope scope = tracer.withSpan(span)) {
            return action.get();
        } finally {
            span.end();
        }
    }

    public void observeWithTraceParent(String traceParent, String spanName, Runnable action) {
        traceContextSupport.runWithTraceParent(traceParent, spanName, action);
    }

    public <T> T observeWithTraceParent(String traceParent, String spanName, Supplier<T> action) {
        return traceContextSupport.runWithTraceParent(traceParent, spanName, action);
    }
}
