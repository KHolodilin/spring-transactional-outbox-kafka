package com.kholodilin.outbox.tracing;

import io.micrometer.tracing.Span;
import io.micrometer.tracing.TraceContext;
import io.micrometer.tracing.Tracer;
import io.micrometer.tracing.propagation.Propagator;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.ObjectProvider;

import java.util.Map;
import java.util.concurrent.atomic.AtomicReference;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

class TraceContextSupportTest {

    private static final String TRACE_PARENT = "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01";

    @Test
    void captureTraceParentReturnsInjectedValue() {
        Tracer tracer = mock(Tracer.class);
        Propagator propagator = mock(Propagator.class);
        Span currentSpan = mock(Span.class);
        TraceContext traceContext = mock(TraceContext.class);
        when(tracer.currentSpan()).thenReturn(currentSpan);
        when(currentSpan.context()).thenReturn(traceContext);
        doAnswer(invocation -> {
            Map<String, String> carrier = invocation.getArgument(1);
            carrier.put("traceparent", TRACE_PARENT);
            return null;
        }).when(propagator).inject(eq(traceContext), any(), any());

        TraceContextSupport support = support(tracer, propagator);

        assertThat(support.captureTraceParent()).isEqualTo(TRACE_PARENT);
    }

    @Test
    void captureTraceParentReturnsNullWhenInjectionOmitsHeader() {
        Tracer tracer = mock(Tracer.class);
        Propagator propagator = mock(Propagator.class);
        Span currentSpan = mock(Span.class);
        when(tracer.currentSpan()).thenReturn(currentSpan);
        when(currentSpan.context()).thenReturn(mock(TraceContext.class));

        assertThat(support(tracer, propagator).captureTraceParent()).isNull();
    }

    @Test
    void captureTraceParentReturnsNullWhenNoActiveSpan() {
        Tracer tracer = mock(Tracer.class);
        when(tracer.currentSpan()).thenReturn(null);

        assertThat(support(tracer, mock(Propagator.class)).captureTraceParent()).isNull();
    }

    @Test
    void captureTraceParentReturnsNullWhenTracingDisabled() {
        assertThat(support(null, null).captureTraceParent()).isNull();
        assertThat(support(mock(Tracer.class), null).captureTraceParent()).isNull();
    }

    @Test
    void runWithBlankTraceParentContinuesCurrentTrace() {
        Tracer tracer = mock(Tracer.class);
        Span child = mock(Span.class);
        when(tracer.nextSpan()).thenReturn(child);
        when(child.name("db.persist")).thenReturn(child);
        when(child.start()).thenReturn(child);
        when(tracer.withSpan(child)).thenReturn(mock(Tracer.SpanInScope.class));

        String result = support(tracer, mock(Propagator.class)).runWithTraceParent(null, "db.persist", () -> "saved");

        assertThat(result).isEqualTo("saved");
        verify(child).end();
    }

    @Test
    void runWithTraceParentRestoresStoredContext() {
        Tracer tracer = mock(Tracer.class);
        Propagator propagator = mock(Propagator.class);
        Span.Builder builder = mock(Span.Builder.class);
        Span child = mock(Span.class);
        when(propagator.extract(any(), any())).thenReturn(builder);
        when(builder.name("outbox.save")).thenReturn(builder);
        when(builder.start()).thenReturn(child);
        when(tracer.withSpan(child)).thenReturn(mock(Tracer.SpanInScope.class));

        AtomicReference<String> seen = new AtomicReference<>();
        support(tracer, propagator).runWithTraceParent(TRACE_PARENT, "outbox.save", () -> {
            seen.set("ran");
            return null;
        });

        assertThat(seen).hasValue("ran");
        verify(child).end();
    }

    @Test
    void runWithTraceParentSkipsSpanWhenTracingDisabled() {
        AtomicReference<String> seen = new AtomicReference<>();
        support(null, null).runWithTraceParent(TRACE_PARENT, "db.persist", () -> {
            seen.set("ran");
            return null;
        });
        assertThat(seen).hasValue("ran");
    }

    @Test
    void runWithTraceParentSkipsSpanWhenPropagatorMissing() {
        AtomicReference<String> seen = new AtomicReference<>();
        support(mock(Tracer.class), null).runWithTraceParent(TRACE_PARENT, "db.persist", () -> {
            seen.set("ran");
            return null;
        });
        assertThat(seen).hasValue("ran");
    }

    @Test
    void runWithBlankStringContinuesCurrentTrace() {
        Tracer tracer = mock(Tracer.class);
        Span child = mock(Span.class);
        when(tracer.nextSpan()).thenReturn(child);
        when(child.name("db.persist")).thenReturn(child);
        when(child.start()).thenReturn(child);
        when(tracer.withSpan(child)).thenReturn(mock(Tracer.SpanInScope.class));

        String result = support(tracer, mock(Propagator.class)).runWithTraceParent("  ", "db.persist", () -> "saved");

        assertThat(result).isEqualTo("saved");
        verify(child).end();
    }

    private static TraceContextSupport support(Tracer tracer, Propagator propagator) {
        @SuppressWarnings("unchecked")
        ObjectProvider<Tracer> tracerProvider = mock(ObjectProvider.class);
        @SuppressWarnings("unchecked")
        ObjectProvider<Propagator> propagatorProvider = mock(ObjectProvider.class);
        when(tracerProvider.getIfAvailable()).thenReturn(tracer);
        when(propagatorProvider.getIfAvailable()).thenReturn(propagator);
        return new TraceContextSupport(tracerProvider, propagatorProvider);
    }
}
