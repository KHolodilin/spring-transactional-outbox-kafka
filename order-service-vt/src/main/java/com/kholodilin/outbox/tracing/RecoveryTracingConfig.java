package com.kholodilin.outbox.tracing;

import com.kholodilin.outbox.recovery.RecoveryWorker;
import io.micrometer.observation.ObservationPredicate;
import io.micrometer.tracing.exporter.SpanExportingPredicate;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.scheduling.support.ScheduledTaskObservationContext;

/**
 * Suppresses Spring's automatic {@code @Scheduled} observations for {@link RecoveryWorker}.
 * <p>
 * <b>Why this config exists.</b>
 * {@link RecoveryWorker#recover()} runs on a fixed delay (default every 10s). Spring Boot
 * schedules instrumentation would create a root span named like
 * {@code task recoveryWorker.recover} on <em>every</em> tick — including empty runs that only
 * query the DB and find nothing to re-enqueue.
 * <p>
 * That noise is harmful in Tempo:
 * <ul>
 *   <li>thousands of empty recovery traces drown real order/outbox traces,</li>
 *   <li>empty ticks look like “failed” or “orphan” work when they are healthy no-ops,</li>
 *   <li>we already create a meaningful span via {@link OutboxTracing#observe} named
 *       {@code outbox.recovery} <em>only when</em> recoverable ids were claimed.</li>
 * </ul>
 * So the automatic scheduler span is dropped; the intentional outbox span stays.
 * <p>
 * <b>How exclusion works (two layers).</b>
 * <ol>
 *   <li>{@link ObservationPredicate} — do not start an Observation when the name starts with
 *       {@code task recoveryWorker.} or the {@link ScheduledTaskObservationContext} target is
 *       {@link RecoveryWorker}.</li>
 *   <li>{@link SpanExportingPredicate} — do not export spans whose name still starts with that
 *       prefix (belt-and-suspenders if an observation was created anyway).</li>
 * </ol>
 * <p>
 * <b>What is not excluded.</b>
 * Other {@code @Scheduled} tasks (if added later) keep default tracing unless they match the
 * recovery prefix / target class. Explicit {@code outbox.recovery} spans from
 * {@link OutboxTracing} are unrelated and remain visible.
 * <p>
 * <b>Exceptions.</b>
 * Predicates are pure filters: they never swallow recovery errors. If {@code recover()} throws,
 * that still surfaces in application logs / metrics; only the noisy empty scheduler span is
 * removed from the export path.
 *
 * @see ActuatorTracingExclusionConfig
 * @see OutboxTracing
 */
@Configuration
public class RecoveryTracingConfig {

    /** Micrometer / Spring naming prefix for {@link RecoveryWorker} scheduled methods. */
    private static final String RECOVERY_SCHEDULER_SPAN_PREFIX = "task recoveryWorker.";

    /**
     * Prevents Observations for RecoveryWorker scheduled ticks.
     * <p>
     * Returning {@code false} = skip observation. Empty ticks therefore never become traces.
     */
    @Bean
    ObservationPredicate skipRecoverySchedulerObservation() {
        return (name, context) -> !isRecoveryScheduler(name, context);
    }

    /**
     * Blocks OTLP export of recovery scheduler spans if one was created despite the
     * {@link ObservationPredicate}.
     */
    @Bean
    SpanExportingPredicate noRecoverySchedulerSpanExportingPredicate() {
        return span -> {
            String name = span.getName();
            return name == null || !name.startsWith(RECOVERY_SCHEDULER_SPAN_PREFIX);
        };
    }

    /**
     * @return {@code true} when this observation belongs to the RecoveryWorker schedule
     */
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
