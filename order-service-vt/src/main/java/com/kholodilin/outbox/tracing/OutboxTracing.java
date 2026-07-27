package com.kholodilin.outbox.tracing;

import io.micrometer.tracing.Span;
import io.micrometer.tracing.Tracer;
import org.springframework.beans.factory.ObjectProvider;
import org.springframework.stereotype.Component;

import java.util.function.Supplier;

/**
 * Creates dedicated Micrometer Tracing spans for the asynchronous outbox publishing pipeline.
 * <p>
 * <b>Why this class exists.</b>
 * Spring Boot already instruments HTTP, JDBC and Kafka automatically. Those spans do not cover
 * our custom outbox stages (recovery claim, batch fetch, Kafka publish after a DB commit).
 * Without explicit spans, Grafana Tempo would show a broken chain: the HTTP request ends when
 * the response is returned, and the later publisher/recovery work would appear as unrelated
 * root traces — or not at all.
 * <p>
 * This component fills that gap by wrapping pipeline steps in named spans such as
 * {@code outbox.recovery}, {@code outbox.batch.fetch} and {@code outbox.batch.publish}.
 * <p>
 * <b>Two restore modes.</b>
 * <ul>
 *   <li>{@link #observe} — starts a <em>child</em> of the currently active span (or a new root
 *       when nothing is in scope). Used for recovery and for stages that already run under a
 *       restored HTTP/outbox context.</li>
 *   <li>{@link #observeWithTraceParent} — restores the W3C {@code traceparent} that was
 *       persisted on the outbox row at insert time, then starts a child under that parent.
 *       This is what reconnects the delayed Kafka publish to the original HTTP request trace
 *       after the transactional outbox delay.</li>
 * </ul>
 * <p>
 * <b>Exceptions.</b>
 * Actions are not wrapped in a catch block. Any exception thrown by {@code action} propagates
 * to the caller unchanged. The span is always closed in {@code finally} ({@link Span#end()}),
 * so a failed publish still appears in Tempo with a proper end timestamp. Status / error tags
 * are left to Micrometer Observation / the OTel bridge where configured; this class only
 * guarantees span lifecycle.
 * <p>
 * <b>Disabled tracing.</b>
 * {@link Tracer} is resolved via {@link ObjectProvider#getIfAvailable()}. When tracing is off
 * (for example {@code management.tracing.enabled=false} in the {@code test} profile), methods
 * simply execute {@code action} with no span overhead — no NPE, no Tempo dependency in CI.
 *
 * @see TraceContextSupport
 * @see RecoveryTracingConfig
 */
@Component
public class OutboxTracing {

    private final ObjectProvider<Tracer> tracerProvider;
    private final TraceContextSupport traceContextSupport;

    public OutboxTracing(ObjectProvider<Tracer> tracerProvider, TraceContextSupport traceContextSupport) {
        this.tracerProvider = tracerProvider;
        this.traceContextSupport = traceContextSupport;
    }

    /**
     * Runs {@code action} inside a named span that continues the current trace context.
     * <p>
     * Prefer this when the caller already has an active span (HTTP request, restored
     * {@code traceparent}, etc.). Equivalent to {@link #observe(String, Supplier)} with a
     * {@code void} result.
     *
     * @param spanName logical stage name shown in Tempo (e.g. {@code outbox.recovery})
     * @param action   work to measure; exceptions propagate after the span is ended
     */
    public void observe(String spanName, Runnable action) {
        observe(spanName, () -> {
            action.run();
            return null;
        });
    }

    /**
     * Same as {@link #observe(String, Runnable)} but returns the supplier result.
     * <p>
     * Span lifecycle: {@code nextSpan → start → withSpan(scope) → action → finally end}.
     * If {@link Tracer} is unavailable, {@code action} runs without creating a span.
     *
     * @param spanName logical stage name shown in Tempo
     * @param action   work to measure; return value is passed through; exceptions propagate
     * @param <T>      result type
     * @return value produced by {@code action}
     */
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

    /**
     * Continues the original request trace using a persisted W3C {@code traceparent}, then
     * runs {@code action} under a new named child span.
     * <p>
     * Used by the batch publisher after loading outbox rows that stored {@code trace_parent}
     * at insert time. Without this call, publish spans would not share the HTTP request's
     * {@code traceId} and Tempo would show two disconnected trees.
     * <p>
     * Delegates to {@link TraceContextSupport#runWithTraceParent(String, String, Runnable)}.
     * A blank/null {@code traceParent} still creates a local span (new root) so publishing
     * remains observable even for legacy rows without context.
     *
     * @param traceParent W3C Trace Context header value from {@code outbox_events.trace_parent}
     * @param spanName    child span name (e.g. {@code outbox.batch.publish})
     * @param action      publish / DB work; exceptions propagate after the span is ended
     */
    public void observeWithTraceParent(String traceParent, String spanName, Runnable action) {
        traceContextSupport.runWithTraceParent(traceParent, spanName, action);
    }

    /**
     * Same as {@link #observeWithTraceParent(String, String, Runnable)} with a return value.
     *
     * @param traceParent W3C Trace Context header value, may be {@code null}
     * @param spanName    child span name
     * @param action      work to measure
     * @param <T>         result type
     * @return value produced by {@code action}
     */
    public <T> T observeWithTraceParent(String traceParent, String spanName, Supplier<T> action) {
        return traceContextSupport.runWithTraceParent(traceParent, spanName, action);
    }
}
