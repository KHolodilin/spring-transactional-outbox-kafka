package com.kholodilin.outbox.tracing;

import io.micrometer.observation.Observation;
import io.micrometer.observation.ObservationPredicate;
import io.micrometer.observation.ObservationView;
import io.micrometer.tracing.exporter.SpanExportingPredicate;
import jakarta.servlet.http.HttpServletRequest;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.http.server.observation.ServerRequestObservationContext;
import org.springframework.util.AntPathMatcher;

/**
 * Suppresses Micrometer Observation / tracing export for Spring Boot Actuator endpoints.
 * <p>
 * <b>Why this config exists.</b>
 * Prometheus scrapes {@code /actuator/prometheus} and probes hit health/info on a short
 * interval. Without filtering, each scrape becomes a tiny HTTP span in Grafana Tempo and
 * drowns Kafka consumer / business traces that matter for this stub.
 * <p>
 * <b>How exclusion works (two layers).</b>
 * <ol>
 *   <li>{@link ObservationPredicate} — do not start an Observation for actuator URIs
 *       (cheapest: no span created).</li>
 *   <li>{@link SpanExportingPredicate} — do not export spans whose name still contains
 *       {@code /actuator} (safety net).</li>
 * </ol>
 * Base path comes from {@code management.endpoints.web.base-path} (default {@code /actuator}).
 * Parent {@link ObservationView}s are walked because nested observations may not hold the
 * servlet request themselves.
 * <p>
 * <b>Exceptions.</b>
 * Predicates are side-effect free and never affect actuator response status. A miss only
 * means extra scrape spans in Tempo, not a failed health check.
 *
 * @see TraceContextSupport
 */
@Configuration
public class ActuatorTracingExclusionConfig {

    private final AntPathMatcher pathMatcher = new AntPathMatcher();
    private final String actuatorPattern;

    /**
     * Builds pattern {@code <base-path>/**} from the configured actuator base path.
     */
    public ActuatorTracingExclusionConfig(
            @Value("${management.endpoints.web.base-path:/actuator}") String actuatorBasePath) {
        String normalized = actuatorBasePath.endsWith("/")
                ? actuatorBasePath.substring(0, actuatorBasePath.length() - 1)
                : actuatorBasePath;
        this.actuatorPattern = normalized + "/**";
    }

    /**
     * Skips Observations for actuator HTTP traffic (and nested observations under them).
     */
    @Bean
    ObservationPredicate noActuatorObservationPredicate() {
        return (name, context) -> !matchesActuator(name, context);
    }

    /**
     * Blocks export of residual spans whose name mentions {@code /actuator}.
     */
    @Bean
    SpanExportingPredicate noActuatorSpanExportingPredicate() {
        return span -> {
            String name = span.getName();
            return name == null || !name.contains("/actuator");
        };
    }

    /**
     * @return {@code true} when this observation should be suppressed
     */
    private boolean matchesActuator(String name, Observation.Context context) {
        if (name != null && name.contains("/actuator")) {
            return true;
        }

        Observation.Context current = context;
        while (current != null) {
            if (current instanceof ServerRequestObservationContext serverContext) {
                Object carrier = serverContext.getCarrier();
                if (carrier instanceof HttpServletRequest request) {
                    String uri = request.getRequestURI();
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
