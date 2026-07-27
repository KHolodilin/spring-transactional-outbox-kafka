package com.kholodilin.outbox.tracing;

import io.micrometer.observation.Observation;
import io.micrometer.observation.ObservationPredicate;
import io.micrometer.observation.ObservationView;
import io.micrometer.tracing.exporter.FinishedSpan;
import io.micrometer.tracing.exporter.SpanExportingPredicate;
import jakarta.servlet.http.HttpServletRequest;
import org.junit.jupiter.api.Test;
import org.springframework.http.server.observation.ServerRequestObservationContext;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

class ActuatorTracingExclusionConfigTest {

    @Test
    void observationPredicateSkipsByNameContainingActuator() {
        ActuatorTracingExclusionConfig config = new ActuatorTracingExclusionConfig("/actuator");
        ObservationPredicate predicate = config.noActuatorObservationPredicate();

        assertThat(predicate.test("http get /actuator/health", new Observation.Context())).isFalse();
        assertThat(predicate.test("http post /api/v1/orders", new Observation.Context())).isTrue();
    }

    @Test
    void observationPredicateSkipsByServletUri() {
        ActuatorTracingExclusionConfig config = new ActuatorTracingExclusionConfig("/actuator/");
        ObservationPredicate predicate = config.noActuatorObservationPredicate();

        HttpServletRequest request = mock(HttpServletRequest.class);
        when(request.getRequestURI()).thenReturn("/actuator/prometheus");
        ServerRequestObservationContext serverContext = mock(ServerRequestObservationContext.class);
        when(serverContext.getCarrier()).thenReturn(request);
        when(serverContext.getParentObservation()).thenReturn(null);

        assertThat(predicate.test("http get", serverContext)).isFalse();
    }

    @Test
    void observationPredicateWalksParentChain() {
        ActuatorTracingExclusionConfig config = new ActuatorTracingExclusionConfig("/actuator");
        ObservationPredicate predicate = config.noActuatorObservationPredicate();

        HttpServletRequest request = mock(HttpServletRequest.class);
        when(request.getRequestURI()).thenReturn("/actuator/info");
        ServerRequestObservationContext parentContext = mock(ServerRequestObservationContext.class);
        when(parentContext.getCarrier()).thenReturn(request);
        when(parentContext.getParentObservation()).thenReturn(null);

        ObservationView parentView = mock(ObservationView.class);
        when(parentView.getContextView()).thenReturn(parentContext);

        Observation.Context child = mock(Observation.Context.class);
        when(child.getParentObservation()).thenReturn(parentView);

        assertThat(predicate.test("child", child)).isFalse();
    }

    @Test
    void spanExportingPredicateBlocksActuatorNames() {
        ActuatorTracingExclusionConfig config = new ActuatorTracingExclusionConfig("/actuator");
        SpanExportingPredicate predicate = config.noActuatorSpanExportingPredicate();

        FinishedSpan actuator = mock(FinishedSpan.class);
        when(actuator.getName()).thenReturn("GET /actuator/health");
        FinishedSpan business = mock(FinishedSpan.class);
        when(business.getName()).thenReturn("http post /api/v1/orders");
        FinishedSpan unnamed = mock(FinishedSpan.class);
        when(unnamed.getName()).thenReturn(null);

        assertThat(predicate.isExportable(actuator)).isFalse();
        assertThat(predicate.isExportable(business)).isTrue();
        assertThat(predicate.isExportable(unnamed)).isTrue();
    }
}
