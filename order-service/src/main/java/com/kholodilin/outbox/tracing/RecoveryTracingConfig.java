package com.kholodilin.outbox.tracing;

import com.kholodilin.outbox.recovery.RecoveryWorker;
import io.micrometer.observation.ObservationPredicate;
import io.micrometer.tracing.exporter.SpanExportingPredicate;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.scheduling.support.ScheduledTaskObservationContext;

@Configuration
public class RecoveryTracingConfig {

    private static final String RECOVERY_SCHEDULER_SPAN_PREFIX = "task recoveryWorker.";

    @Bean
    ObservationPredicate skipRecoverySchedulerObservation() {
        return (name, context) -> !isRecoveryScheduler(name, context);
    }

    @Bean
    SpanExportingPredicate noRecoverySchedulerSpanExportingPredicate() {
        return span -> {
            String name = span.getName();
            return name == null || !name.startsWith(RECOVERY_SCHEDULER_SPAN_PREFIX);
        };
    }

    private static boolean isRecoveryScheduler(String name, io.micrometer.observation.Observation.Context context) {
        if (name != null && name.startsWith(RECOVERY_SCHEDULER_SPAN_PREFIX)) {
            return true;
        }
        if (context instanceof ScheduledTaskObservationContext scheduledContext) {
            return RecoveryWorker.class.isAssignableFrom(scheduledContext.getTargetClass());
        }
        return false;
    }
}
