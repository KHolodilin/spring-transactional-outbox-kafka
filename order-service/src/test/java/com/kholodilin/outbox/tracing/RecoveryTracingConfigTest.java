package com.kholodilin.outbox.tracing;

import io.micrometer.observation.Observation;
import io.micrometer.observation.ObservationPredicate;
import io.micrometer.tracing.exporter.FinishedSpan;
import io.micrometer.tracing.exporter.SpanExportingPredicate;
import org.junit.jupiter.api.Test;
import org.springframework.scheduling.support.ScheduledTaskObservationContext;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

class RecoveryTracingConfigTest {

    @Test
    void observationPredicateSkipsRecoverySchedulerByName() {
        RecoveryTracingConfig config = new RecoveryTracingConfig();
        ObservationPredicate predicate = config.skipRecoverySchedulerObservation();

        assertThat(predicate.test("task recoveryWorker.recover", new Observation.Context())).isFalse();
        assertThat(predicate.test("outbox.recovery", new Observation.Context())).isTrue();
    }

    @Test
    void observationPredicateSkipsRecoverySchedulerByTargetClass() {
        RecoveryTracingConfig config = new RecoveryTracingConfig();
        ObservationPredicate predicate = config.skipRecoverySchedulerObservation();

        ScheduledTaskObservationContext scheduled = mock(ScheduledTaskObservationContext.class);
        org.mockito.Mockito.doReturn(com.kholodilin.outbox.recovery.RecoveryWorker.class)
                .when(scheduled).getTargetClass();

        assertThat(predicate.test("task other", scheduled)).isFalse();
    }

    @Test
    void spanExportingPredicateBlocksRecoverySchedulerPrefix() {
        RecoveryTracingConfig config = new RecoveryTracingConfig();
        SpanExportingPredicate predicate = config.noRecoverySchedulerSpanExportingPredicate();

        FinishedSpan recovery = mock(FinishedSpan.class);
        when(recovery.getName()).thenReturn("task recoveryWorker.recover");
        FinishedSpan other = mock(FinishedSpan.class);
        when(other.getName()).thenReturn("outbox.recovery");
        FinishedSpan unnamed = mock(FinishedSpan.class);
        when(unnamed.getName()).thenReturn(null);

        assertThat(predicate.isExportable(recovery)).isFalse();
        assertThat(predicate.isExportable(other)).isTrue();
        assertThat(predicate.isExportable(unnamed)).isTrue();
    }
}
