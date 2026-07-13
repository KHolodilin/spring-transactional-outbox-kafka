package com.kholodilin.outbox.health;

import com.kholodilin.outbox.config.AppProperties;
import com.kholodilin.outbox.config.HealthProperties;
import com.kholodilin.outbox.persistence.OutboxJdbcRepository;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.boot.health.contributor.Health;
import org.springframework.boot.health.contributor.Status;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class OutboxHealthIndicatorTest {

    @Mock
    private OutboxJdbcRepository outboxJdbcRepository;

    @Test
    void reportsUpWhenPendingBelowCritical() {
        when(outboxJdbcRepository.countActivePending()).thenReturn(5L);
        OutboxHealthIndicator indicator = new OutboxHealthIndicator(outboxJdbcRepository, properties(100L));

        Health health = indicator.health();

        assertThat(health.getStatus()).isEqualTo(Status.UP);
        assertThat(health.getDetails()).containsEntry("activePending", 5L);
    }

    @Test
    void reportsDownWhenPendingAtCriticalThreshold() {
        when(outboxJdbcRepository.countActivePending()).thenReturn(100L);
        OutboxHealthIndicator indicator = new OutboxHealthIndicator(outboxJdbcRepository, properties(100L));

        Health health = indicator.health();

        assertThat(health.getStatus()).isEqualTo(Status.DOWN);
    }

    private static AppProperties properties(long critical) {
        return AppProperties.builder()
                .health(HealthProperties.builder().outboxPendingCritical(critical).build())
                .build();
    }
}
