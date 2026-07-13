package com.kholodilin.outbox.health;

import com.kholodilin.outbox.config.AppProperties;
import com.kholodilin.outbox.config.HealthProperties;
import com.kholodilin.outbox.queue.InMemoryEventQueue;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.boot.health.contributor.Health;
import org.springframework.boot.health.contributor.Status;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class QueueHealthIndicatorTest {

    @Mock
    private InMemoryEventQueue eventQueue;

    @Test
    void reportsUpWhenPressureBelowCritical() {
        when(eventQueue.pressure()).thenReturn(0.5);
        when(eventQueue.size()).thenReturn(10);
        QueueHealthIndicator indicator = new QueueHealthIndicator(eventQueue, properties(0.95));

        Health health = indicator.health();

        assertThat(health.getStatus()).isEqualTo(Status.UP);
        assertThat(health.getDetails()).containsEntry("queuePressure", 0.5);
    }

    @Test
    void reportsDownWhenPressureAtCriticalThreshold() {
        when(eventQueue.pressure()).thenReturn(0.95);
        when(eventQueue.size()).thenReturn(9500);
        QueueHealthIndicator indicator = new QueueHealthIndicator(eventQueue, properties(0.95));

        Health health = indicator.health();

        assertThat(health.getStatus()).isEqualTo(Status.DOWN);
        assertThat(health.getDetails()).containsEntry("queueSize", 9500);
    }

    private static AppProperties properties(double critical) {
        return AppProperties.builder()
                .health(HealthProperties.builder().queuePressureCritical(critical).build())
                .build();
    }
}
