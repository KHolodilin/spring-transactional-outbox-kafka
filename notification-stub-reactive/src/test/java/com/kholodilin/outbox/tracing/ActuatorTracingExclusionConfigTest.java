package com.kholodilin.outbox.tracing;

import io.micrometer.observation.Observation;
import io.micrometer.observation.ObservationPredicate;
import io.micrometer.observation.ObservationView;
import io.micrometer.tracing.exporter.FinishedSpan;
import io.micrometer.tracing.exporter.SpanExportingPredicate;
import org.junit.jupiter.api.Test;
import org.springframework.http.server.RequestPath;
import org.springframework.http.server.reactive.ServerHttpRequest;
import org.springframework.http.server.reactive.observation.ServerRequestObservationContext;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

class ActuatorTracingExclusionConfigTest {

    @Test
    void observationPredicateSkipsByNameContainingActuator() {
        ActuatorTracingExclusionConfig config = new ActuatorTracingExclusionConfig("/actuator");
        ObservationPredicate predicate = config.noActuatorObservationPredicate();

        assertThat(predicate.test("http get /actuator/health", new Observation.Context())).isFalse();
        assertThat(predicate.test("http post /api", new Observation.Context())).isTrue();
        assertThat(predicate.test(null, new Observation.Context())).isTrue();
    }

    @Test
    void observationPredicateSkipsByReactiveUri() {
        ActuatorTracingExclusionConfig config = new ActuatorTracingExclusionConfig("/actuator/");
        ObservationPredicate predicate = config.noActuatorObservationPredicate();

        assertThat(predicate.test("http get", serverContext("/actuator/prometheus"))).isFalse();
        assertThat(predicate.test("http get", serverContext("/api/orders"))).isTrue();
        assertThat(predicate.test("http get", serverContext(null))).isTrue();
        assertThat(predicate.test("http get", serverContextWithCarrier(null))).isTrue();
    }

    @Test
    void observationPredicateWalksParentChain() {
        ActuatorTracingExclusionConfig config = new ActuatorTracingExclusionConfig("/actuator");
        ObservationPredicate predicate = config.noActuatorObservationPredicate();

        ServerRequestObservationContext parentContext = serverContext("/actuator/info");

        ObservationView parentView = mock(ObservationView.class);
        when(parentView.getContextView()).thenReturn(parentContext);

        Observation.Context child = mock(Observation.Context.class);
        when(child.getParentObservation()).thenReturn(parentView);

        assertThat(predicate.test("child", child)).isFalse();
    }

    @Test
    void spanExportingPredicateBlocksActuatorNames() {
        ActuatorTracingExclusionConfig config = new ActuatorTracingExclusionConfig("/manage/");
        SpanExportingPredicate predicate = config.noActuatorSpanExportingPredicate();

        FinishedSpan actuator = mock(FinishedSpan.class);
        when(actuator.getName()).thenReturn("GET /actuator/health");
        FinishedSpan business = mock(FinishedSpan.class);
        when(business.getName()).thenReturn("notification.consume");
        FinishedSpan unnamed = mock(FinishedSpan.class);
        when(unnamed.getName()).thenReturn(null);

        assertThat(predicate.isExportable(actuator)).isFalse();
        assertThat(predicate.isExportable(business)).isTrue();
        assertThat(predicate.isExportable(unnamed)).isTrue();
    }

    private static ServerRequestObservationContext serverContext(String uri) {
        ServerHttpRequest request = mock(ServerHttpRequest.class);
        RequestPath path = mock(RequestPath.class);
        when(path.value()).thenReturn(uri);
        when(request.getPath()).thenReturn(path);
        return serverContextWithCarrier(request);
    }

    private static ServerRequestObservationContext serverContextWithCarrier(ServerHttpRequest carrier) {
        ServerRequestObservationContext serverContext = mock(ServerRequestObservationContext.class);
        when(serverContext.getCarrier()).thenReturn(carrier);
        when(serverContext.getParentObservation()).thenReturn(null);
        return serverContext;
    }
}
