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
 * Captures and restores W3C Trace Context so asynchronous outbox publishing stays on the
 * same distributed trace as the original HTTP request.
 * <p>
 * <b>Problem this solves.</b>
 * In the transactional outbox pattern the HTTP request commits the business row + outbox row
 * and returns. Kafka publish happens later on a background thread (or after recovery). By then
 * the HTTP span is already finished and its thread-local context is gone. If we started a new
 * root span for publish, Tempo would show two unrelated traces for one order.
 * <p>
 * <b>Approach.</b>
 * At outbox insert time {@link #captureTraceParent()} serializes the active span into a W3C
 * {@code traceparent} string and stores it in {@code outbox_events.trace_parent}. When the
 * publisher (or consumer path) runs, {@link #runWithTraceParent} extracts that value via
 * Micrometer {@link Propagator} and starts a child span under the original parent —
 * continuing the same {@code traceId}.
 * <p>
 * Optional {@link #captureTraceState()} covers the W3C {@code tracestate} vendor bag; it is
 * available for Kafka header propagation even when the DB column stores only {@code traceparent}.
 * <p>
 * <b>Exceptions.</b>
 * No try/catch around {@code action}. Failures bubble to the publisher/recovery caller so
 * retry / FAILED / DEAD status handling stays unchanged. {@link Span#end()} always runs in
 * {@code finally}, so aborted spans still close cleanly in Tempo.
 * <p>
 * <b>Disabled tracing.</b>
 * Both {@link Tracer} and {@link Propagator} are optional ({@link ObjectProvider}). When either
 * is missing (tests, tracing disabled), capture methods return {@code null} and
 * {@code runWithTraceParent} executes {@code action} without opening a span.
 *
 * @see OutboxTracing
 */
@Component
public class TraceContextSupport {

    private static final String TRACE_PARENT_KEY = "traceparent";
    private static final String TRACE_STATE_KEY = "tracestate";

    private final ObjectProvider<Tracer> tracerProvider;
    private final ObjectProvider<Propagator> propagatorProvider;

    public TraceContextSupport(ObjectProvider<Tracer> tracerProvider, ObjectProvider<Propagator> propagatorProvider) {
        this.tracerProvider = tracerProvider;
        this.propagatorProvider = propagatorProvider;
    }

    /**
     * Serializes the current span context as a W3C {@code traceparent} value.
     * <p>
     * Called when persisting an outbox row so the later publisher can continue this trace.
     * Returns {@code null} when tracing is disabled, no span is active, or injection yields
     * no {@code traceparent} entry — callers must tolerate null and still insert the row.
     *
     * @return W3C traceparent string, or {@code null} when absent
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
     * Serializes optional W3C {@code tracestate} from the active span.
     * <p>
     * Used when propagating context into Kafka headers alongside {@code traceparent}.
     * Not required for the DB column; returns {@code null} under the same conditions as
     * {@link #captureTraceParent()}.
     *
     * @return W3C tracestate string, or {@code null} when absent
     */
    public String captureTraceState() {
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
        return carrier.get(TRACE_STATE_KEY);
    }

    /**
     * Restores context from {@code traceParent} and runs {@code action} under a named child span.
     *
     * @param traceParent persisted W3C header, may be blank/{@code null} (then a local root span is used)
     * @param spanName    name of the child span opened for this work
     * @param action      work to execute; exceptions propagate after {@link Span#end()}
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
     * When Tracer/Propagator are unavailable, {@code action} runs immediately with no span.
     * Otherwise: extract parent (or {@code nextSpan} if blank) → start → scope → action → end.
     *
     * @param traceParent persisted W3C header, may be blank/{@code null}
     * @param spanName    child span name
     * @param action      work to measure
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

    /**
     * Builds the span that will wrap the action.
     * <ul>
     *   <li>Blank/null {@code traceParent} → {@code tracer.nextSpan()} (new local root / child of
     *       whatever is currently in scope — keeps publishing observable for rows without context).</li>
     *   <li>Otherwise → {@link Propagator#extract} from a map carrier containing {@code traceparent},
     *       then name and start the extracted span builder so the new span continues that trace.</li>
     * </ul>
     */
    private Span startSpan(Tracer tracer, Propagator propagator, String traceParent, String spanName) {
        if (traceParent == null || traceParent.isBlank()) {
            return tracer.nextSpan().name(spanName).start();
        }
        Map<String, String> carrier = Map.of(TRACE_PARENT_KEY, traceParent);
        return propagator.extract(carrier, Map::get).name(spanName).start();
    }
}
