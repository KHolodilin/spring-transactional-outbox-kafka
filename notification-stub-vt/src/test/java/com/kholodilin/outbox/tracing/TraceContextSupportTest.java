package com.kholodilin.outbox.tracing;

import io.micrometer.tracing.Span;
import io.micrometer.tracing.Tracer;
import io.micrometer.tracing.propagation.Propagator;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.mockito.junit.jupiter.MockitoSettings;
import org.mockito.quality.Strictness;
import org.springframework.beans.factory.ObjectProvider;

import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
@MockitoSettings(strictness = Strictness.LENIENT)
class TraceContextSupportTest {

    private static final String TRACE_PARENT = "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01";

    @Mock
    private Tracer tracer;

    @Mock
    private Propagator propagator;

    @Mock
    private ObjectProvider<Tracer> tracerProvider;

    @Mock
    private ObjectProvider<Propagator> propagatorProvider;

    @Mock
    private Span childSpan;

    @Mock
    private Span.Builder spanBuilder;

    @Mock
    private Tracer.SpanInScope spanInScope;

    private TraceContextSupport traceContextSupport;

    @BeforeEach
    void setUp() {
        when(tracerProvider.getIfAvailable()).thenReturn(tracer);
        when(propagatorProvider.getIfAvailable()).thenReturn(propagator);
        traceContextSupport = new TraceContextSupport(tracerProvider, propagatorProvider);
    }

    @Test
    void runsActionWhenTracingDisabled() {
        when(tracerProvider.getIfAvailable()).thenReturn(null);
        AtomicBoolean ran = new AtomicBoolean();

        traceContextSupport.runWithTraceParent(null, "span", () -> ran.set(true));

        assertThat(ran).isTrue();
    }

    @Test
    void runWithTraceParentRestoresContext() {
        when(propagator.extract(any(), any())).thenReturn(spanBuilder);
        when(spanBuilder.name("notification.consume")).thenReturn(spanBuilder);
        when(spanBuilder.start()).thenReturn(childSpan);
        when(tracer.withSpan(childSpan)).thenReturn(spanInScope);

        AtomicReference<String> seen = new AtomicReference<>();
        traceContextSupport.runWithTraceParent(TRACE_PARENT, "notification.consume", () -> seen.set("ran"));

        assertThat(seen).hasValue("ran");
        ArgumentCaptor<Map<String, String>> carrierCaptor = ArgumentCaptor.forClass(Map.class);
        verify(propagator).extract(carrierCaptor.capture(), any());
        assertThat(carrierCaptor.getValue()).containsEntry("traceparent", TRACE_PARENT);
        verify(childSpan).end();
    }

    @Test
    void startsNewSpanWhenTraceParentMissing() {
        when(tracer.nextSpan()).thenReturn(childSpan);
        when(childSpan.name("notification.batch.receive")).thenReturn(childSpan);
        when(childSpan.start()).thenReturn(childSpan);
        when(tracer.withSpan(childSpan)).thenReturn(spanInScope);

        traceContextSupport.runWithTraceParent("  ", "notification.batch.receive", () -> {
        });

        verify(childSpan).end();
    }
}
