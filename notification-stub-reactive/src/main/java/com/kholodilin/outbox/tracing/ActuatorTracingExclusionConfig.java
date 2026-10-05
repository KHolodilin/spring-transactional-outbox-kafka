package com.kholodilin.outbox.tracing;

import io.micrometer.observation.Observation;
import io.micrometer.observation.ObservationPredicate;
import io.micrometer.observation.ObservationView;
import io.micrometer.tracing.exporter.SpanExportingPredicate;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.http.server.reactive.ServerHttpRequest;
import org.springframework.http.server.reactive.observation.ServerRequestObservationContext;
import org.springframework.util.AntPathMatcher;

/**
 * Suppresses Micrometer Observation / tracing export for Spring Boot Actuator endpoints.
 * <p>
 * WebFlux variant of the servlet stub filter: Prometheus scrapes {@code /actuator/prometheus}
 * and probes hit health on a short interval. Without filtering, each scrape becomes a tiny
 * HTTP span in Grafana Tempo and drowns Kafka consumer / business traces.
 */
@Configuration
public class ActuatorTracingExclusionConfig {

    private final AntPathMatcher pathMatcher = new AntPathMatcher();
    private final String actuatorPattern;

    public ActuatorTracingExclusionConfig(
            @Value("${management.endpoints.web.base-path:/actuator}") String actuatorBasePath) {
        String normalized = actuatorBasePath.endsWith("/")
                ? actuatorBasePath.substring(0, actuatorBasePath.length() - 1)
                : actuatorBasePath;
        this.actuatorPattern = normalized + "/**";
    }

    @Bean
    ObservationPredicate noActuatorObservationPredicate() {
        return (name, context) -> !matchesActuator(name, context);
    }

    @Bean
    SpanExportingPredicate noActuatorSpanExportingPredicate() {
        return span -> {
            String name = span.getName();
            return name == null || !name.contains("/actuator");
        };
    }

    private boolean matchesActuator(String name, Observation.Context context) {
        if (name != null && name.contains("/actuator")) {
            return true;
        }

        Observation.Context current = context;
        while (current != null) {
            if (current instanceof ServerRequestObservationContext serverContext) {
                Object carrier = serverContext.getCarrier();
                if (carrier instanceof ServerHttpRequest request) {
                    String uri = request.getPath().value();
                    if (uri != null && pathMatcher.match(actuatorPattern, uri)) {
                        return true;
                    }
                }
            }

            ObservationView parentObservation = current.getParentObservation();
            if (parentObservation == null) {
                break;
            }
            current = (Observation.Context) parentObservation.getContextView();
        }

        return false;
    }
}
