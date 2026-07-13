package com.kholodilin.outbox.tracing;

import io.micrometer.tracing.Span;
import io.micrometer.tracing.Tracer;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.beans.factory.ObjectProvider;

import java.util.concurrent.atomic.AtomicBoolean;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class OutboxTracingTest {

    @Mock
    private ObjectProvider<Tracer> tracerProvider;

    @Mock
    private TraceContextSupport traceContextSupport;

    @Test
    void observeRunsActionWhenTracerUnavailable() {
        when(tracerProvider.getIfAvailable()).thenReturn(null);
        OutboxTracing tracing = new OutboxTracing(tracerProvider, traceContextSupport);
        AtomicBoolean ran = new AtomicBoolean();

        tracing.observe("test-span", () -> ran.set(true));

        assertThat(ran).isTrue();
    }

    @Test
    void observeWithSupplierWhenTracerUnavailable() {
        when(tracerProvider.getIfAvailable()).thenReturn(null);
        OutboxTracing tracing = new OutboxTracing(tracerProvider, traceContextSupport);

        String result = tracing.observe("test-span", () -> "ok");

        assertThat(result).isEqualTo("ok");
    }

    @Test
    void observeWithTraceParentDelegatesToTraceContextSupport() {
        OutboxTracing tracing = new OutboxTracing(tracerProvider, traceContextSupport);
        AtomicBoolean ran = new AtomicBoolean();
        org.mockito.Mockito.doAnswer(invocation -> {
            Runnable runnable = invocation.getArgument(2);
            runnable.run();
            return null;
        }).when(traceContextSupport).runWithTraceParent(org.mockito.ArgumentMatchers.any(), org.mockito.ArgumentMatchers.eq("span"), org.mockito.ArgumentMatchers.any(Runnable.class));

        tracing.observeWithTraceParent("trace", "span", () -> ran.set(true));

        assertThat(ran).isTrue();
    }

    @Test
    void observeCreatesSpanWhenTracerAvailable() {
        Tracer tracer = mock(Tracer.class);
        Span span = mock(Span.class);
        Tracer.SpanInScope scope = mock(Tracer.SpanInScope.class);
        when(tracerProvider.getIfAvailable()).thenReturn(tracer);
        when(tracer.nextSpan()).thenReturn(span);
        when(span.name("test-span")).thenReturn(span);
        when(span.start()).thenReturn(span);
        when(tracer.withSpan(span)).thenReturn(scope);
        OutboxTracing tracing = new OutboxTracing(tracerProvider, traceContextSupport);

        tracing.observe("test-span", () -> {
        });

        org.mockito.Mockito.verify(span).end();
    }
}
