package com.kholodilin.outbox.tracing;

import io.micrometer.tracing.Span;
import io.micrometer.tracing.TraceContext;
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
import java.util.concurrent.atomic.AtomicReference;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doAnswer;
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
    private Span currentSpan;

    @Mock
    private Span childSpan;

    @Mock
    private TraceContext traceContext;

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
    void captureTraceParentReturnsInjectedValue() {
        when(tracer.currentSpan()).thenReturn(currentSpan);
        when(currentSpan.context()).thenReturn(traceContext);
        doAnswer(invocation -> {
            Map<String, String> carrier = invocation.getArgument(1);
            carrier.put("traceparent", TRACE_PARENT);
            return null;
        }).when(propagator).inject(eq(traceContext), any(), any());

        assertThat(traceContextSupport.captureTraceParent()).isEqualTo(TRACE_PARENT);
    }

    @Test
    void captureTraceParentReturnsNullWhenNoActiveSpan() {
        when(tracer.currentSpan()).thenReturn(null);

        assertThat(traceContextSupport.captureTraceParent()).isNull();
    }

    @Test
    void runWithTraceParentRestoresContext() {
        when(propagator.extract(any(), any())).thenReturn(spanBuilder);
        when(spanBuilder.name("outbox.publish")).thenReturn(spanBuilder);
        when(spanBuilder.start()).thenReturn(childSpan);
        when(tracer.withSpan(childSpan)).thenReturn(spanInScope);

        AtomicReference<String> traceParentSeen = new AtomicReference<>();
        traceContextSupport.runWithTraceParent(TRACE_PARENT, "outbox.publish", () -> traceParentSeen.set("ran"));

        assertThat(traceParentSeen).hasValue("ran");
        ArgumentCaptor<Map<String, String>> carrierCaptor = ArgumentCaptor.forClass(Map.class);
        verify(propagator).extract(carrierCaptor.capture(), any());
        assertThat(carrierCaptor.getValue()).containsEntry("traceparent", TRACE_PARENT);
        verify(childSpan).end();
    }

    @Test
    void runWithTraceParentNoOpsWhenTracingDisabled() {
        ObjectProvider<Tracer> emptyTracer = org.mockito.Mockito.mock(ObjectProvider.class);
        ObjectProvider<Propagator> emptyPropagator = org.mockito.Mockito.mock(ObjectProvider.class);
        when(emptyTracer.getIfAvailable()).thenReturn(null);
        when(emptyPropagator.getIfAvailable()).thenReturn(null);
        TraceContextSupport disabled = new TraceContextSupport(emptyTracer, emptyPropagator);
        AtomicReference<String> seen = new AtomicReference<>();
        disabled.runWithTraceParent(TRACE_PARENT, "outbox.publish", () -> seen.set("ran"));
        assertThat(seen).hasValue("ran");
    }
}
