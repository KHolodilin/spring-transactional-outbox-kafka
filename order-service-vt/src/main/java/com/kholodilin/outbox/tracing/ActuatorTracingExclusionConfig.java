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
 * Prometheus scrapes {@code /actuator/prometheus} (and health/info probes hit actuator) on a
 * short interval. Spring Boot's HTTP instrumentation would otherwise create a high-volume
 * stream of tiny traces for every scrape. Those spans:
 * <ul>
 *   <li>drown business traces in Grafana Tempo / Explore,</li>
 *   <li>inflate OTLP export traffic and Tempo storage,</li>
 *   <li>add noise when correlating logs by {@code traceId}.</li>
 * </ul>
 * Actuator traffic is operational, not part of the order → outbox → Kafka story we want to debug.
 * <p>
 * <b>How exclusion works (two layers).</b>
 * <ol>
 *   <li>{@link ObservationPredicate} — returns {@code false} for actuator requests so an
 *       Observation (and therefore a span) is never started. Preferred path: cheapest and
 *       avoids work entirely.</li>
 *   <li>{@link SpanExportingPredicate} — safety net if a span was still created (name contains
 *       {@code /actuator}): do not export it to Tempo. Covers edge cases where the observation
 *       name or parent chain does not match the HTTP URI check.</li>
 * </ol>
 * Matching uses {@code management.endpoints.web.base-path} (default {@code /actuator}) so a
 * custom base path still excludes the right URLs. The predicate walks parent
 * {@link ObservationView}s because nested observations may not carry the servlet request
 * themselves.
 * <p>
 * <b>What is not excluded.</b>
 * Business HTTP APIs, JDBC, Kafka, and explicit {@link OutboxTracing} spans continue to be
 * observed and exported as usual.
 * <p>
 * <b>Exceptions / failures.</b>
 * These beans never throw into request handling. A mis-matched path simply means a scrape
 * might still produce a span (too much data), never that actuator health fails. Predicates
 * must stay side-effect free.
 *
 * @see RecoveryTracingConfig
 */
@Configuration
public class ActuatorTracingExclusionConfig {

    private final AntPathMatcher pathMatcher = new AntPathMatcher();
    private final String actuatorPattern;

    /**
     * Builds an Ant-style pattern {@code <base-path>/**} from the configured actuator base path.
     *
     * @param actuatorBasePath value of {@code management.endpoints.web.base-path}
     */
    public ActuatorTracingExclusionConfig(
            @Value("${management.endpoints.web.base-path:/actuator}") String actuatorBasePath) {
        String normalized = actuatorBasePath.endsWith("/")
                ? actuatorBasePath.substring(0, actuatorBasePath.length() - 1)
                : actuatorBasePath;
        this.actuatorPattern = normalized + "/**";
    }

    /**
     * Skips creating Observations for actuator HTTP traffic (and nested observations whose
     * ancestry is an actuator server request).
     * <p>
     * Returning {@code false} means “do not observe”. Returning {@code true} leaves the
     * default instrumentation in place.
     */
    @Bean
    ObservationPredicate noActuatorObservationPredicate() {
        return (name, context) -> !matchesActuator(name, context);
    }

    /**
     * Blocks export of any span whose name still mentions {@code /actuator}.
     * <p>
     * Complements {@link #noActuatorObservationPredicate()}: if an observation slipped through,
     * the span must not reach Tempo.
     */
    @Bean
    SpanExportingPredicate noActuatorSpanExportingPredicate() {
        return span -> {
            String name = span.getName();
            return name == null || !name.contains("/actuator");
        };
    }

    /**
     * Detects actuator traffic by observation name and/or servlet URI on the observation
     * ancestry chain.
     *
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
