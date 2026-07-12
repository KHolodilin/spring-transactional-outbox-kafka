package com.kholodilin.outbox.tracing;

import io.micrometer.observation.ObservationPredicate;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.http.server.observation.ServerRequestObservationContext;

@Configuration
public class ActuatorTracingExclusionConfig {

    private final String actuatorBasePath;

    public ActuatorTracingExclusionConfig(
            @Value("${management.endpoints.web.base-path:/actuator}") String actuatorBasePath) {
        this.actuatorBasePath = actuatorBasePath.endsWith("/")
                ? actuatorBasePath.substring(0, actuatorBasePath.length() - 1)
                : actuatorBasePath;
    }

    @Bean
    ObservationPredicate noActuatorObservationPredicate() {
        return (name, context) -> {
            if (context instanceof ServerRequestObservationContext serverContext) {
                String uri = serverContext.getCarrier().getRequestURI();
                return uri == null || !uri.startsWith(actuatorBasePath);
            }
            return true;
        };
    }
}
