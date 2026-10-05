package com.kholodilin.outbox.tracing;

import io.micrometer.observation.Observation;
import io.micrometer.observation.ObservationPredicate;
import io.micrometer.tracing.exporter.FinishedSpan;
import io.micrometer.tracing.exporter.SpanExportingPredicate;
import org.junit.jupiter.api.Test;
import org.springframework.http.server.reactive.ServerHttpRequest;
import org.springframework.http.server.reactive.observation.ServerRequestObservationContext;
import org.springframework.http.server.RequestPath;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

class ActuatorTracingExclusionConfigTest {

    @Test
    void observationPredicateSkipsActuatorTraffic() {
        ActuatorTracingExclusionConfig config = new ActuatorTracingExclusionConfig("/actuator");
        ObservationPredicate predicate = config.noActuatorObservationPredicate();

        assertThat(predicate.test("http get /actuator/health", new Observation.Context())).isFalse();

        ServerHttpRequest request = mock(ServerHttpRequest.class);
        RequestPath path = mock(RequestPath.class);
        when(path.value()).thenReturn("/actuator/prometheus");
        when(request.getPath()).thenReturn(path);
        ServerRequestObservationContext serverContext = mock(ServerRequestObservationContext.class);
        when(serverContext.getCarrier()).thenReturn(request);
        when(serverContext.getParentObservation()).thenReturn(null);

        assertThat(predicate.test("http get", serverContext)).isFalse();
        assertThat(predicate.test("http post /api", new Observation.Context())).isTrue();
    }

    @Test
    void spanExportingPredicateBlocksActuatorNames() {
        ActuatorTracingExclusionConfig config = new ActuatorTracingExclusionConfig("/manage/");
        SpanExportingPredicate predicate = config.noActuatorSpanExportingPredicate();

        FinishedSpan actuator = mock(FinishedSpan.class);
        when(actuator.getName()).thenReturn("GET /actuator/health");
        FinishedSpan business = mock(FinishedSpan.class);
        when(business.getName()).thenReturn("notification.consume");

        assertThat(predicate.isExportable(actuator)).isFalse();
        assertThat(predicate.isExportable(business)).isTrue();
    }
}
