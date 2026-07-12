package com.kholodilin.outbox.metrics;

import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

class NotificationStubMetricsTest {

    @Test
    void recordsBatchMetrics() {
        SimpleMeterRegistry registry = new SimpleMeterRegistry();
        NotificationStubMetrics metrics = new NotificationStubMetrics(registry);

        metrics.recordBatch(3, () -> {
        });

        assertThat(registry.find("notification.batches.received").counter().count()).isEqualTo(1.0);
        assertThat(registry.find("notification.events.received").counter().count()).isEqualTo(3.0);
        assertThat(registry.find("notification.batch.processing").timer().count()).isEqualTo(1);
    }
}
