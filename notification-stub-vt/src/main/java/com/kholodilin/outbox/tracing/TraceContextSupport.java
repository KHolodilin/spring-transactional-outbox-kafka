package com.kholodilin.outbox.tracing;

import io.micrometer.tracing.Span;
import io.micrometer.tracing.Tracer;
import io.micrometer.tracing.propagation.Propagator;
import org.springframework.beans.factory.ObjectProvider;
import org.springframework.stereotype.Component;

import java.util.Map;
import java.util.function.Supplier;

/**
 * Restores W3C Trace Context from Kafka headers so consumer-side work continues the same
 * distributed trace that started in order-service (HTTP → outbox → Kafka publish).
 * <p>
 * <b>Why this class exists.</b>
 * Spring Kafka / Micrometer may propagate context automatically when the consumer runs under
 * Observation. This helper is used when the notification handler needs an explicit named span
 * under a {@code traceparent} taken from the message (or from headers already extracted by
 * the listener). It keeps Tempo's tree connected: publish span → consume / business span
 * share one {@code traceId}.
 * <p>
 * Unlike order-service's {@code TraceContextSupport}, this class only restores context
 * ({@link #runWithTraceParent}); capture/persist of {@code trace_parent} happens upstream
 * when the outbox row is written.
 * <p>
 * <b>Exceptions.</b>
 * {@code action} exceptions are not caught — they propagate to the Kafka listener error
 * handling / retry path. {@link Span#end()} always runs in {@code finally} so failed
 * processing still closes the span in Tempo.
 * <p>
 * <b>Disabled tracing.</b>
 * If {@link Tracer} or {@link Propagator} is missing ({@code management.tracing.enabled=false}),
 * {@code action} runs with no span — safe for tests and CI without Tempo.
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
     * Opens a named span under the restored {@code traceParent} and runs {@code action}.
     *
     * @param traceParent W3C header from Kafka / upstream, may be blank/{@code null}
     * @param spanName    child span name for consumer-side work
     * @param action      handler logic; exceptions propagate after the span ends
     */
    public void runWithTraceParent(String traceParent, String spanName, Runnable action) {
        runWithTraceParent(traceParent, spanName, () -> {
            action.run();
            return null;
        });
    }

    /**
     * Same as {@link #runWithTraceParent(String, String, Runnable)} with a return value.
     * <p>
     * Blank/null {@code traceParent} still creates a local span via {@code tracer.nextSpan()}
     * so processing remains visible when a message arrived without context.
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

    /**
     * Extracts parent context from {@code traceparent} or starts a standalone span when absent.
     */
    private Span startSpan(Tracer tracer, Propagator propagator, String traceParent, String spanName) {
        if (traceParent == null || traceParent.isBlank()) {
            return tracer.nextSpan().name(spanName).start();
        }
        Map<String, String> carrier = Map.of(TRACE_PARENT_KEY, traceParent);
        return propagator.extract(carrier, Map::get).name(spanName).start();
    }
}
