package com.kholodilin.outbox.metrics;

import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.test.util.ReflectionTestUtils;

import static org.assertj.core.api.Assertions.assertThat;

class OutboxMetricsTest {

    private OutboxMetrics metrics;
    private SimpleMeterRegistry registry;

    @BeforeEach
    void setUp() {
        registry = new SimpleMeterRegistry();
        metrics = new OutboxMetrics(registry);
        ReflectionTestUtils.invokeMethod(metrics, "registerMeters");
    }

    @Test
    void updatesQueueGaugesAndCounters() {
        metrics.updateQueue(5, 0.5);
        metrics.incrementEnqueue();
        metrics.incrementDequeue(2);
        metrics.incrementPublishFailures();
        metrics.incrementRetryCount();
        metrics.incrementRecoveryCount(3);
        metrics.incrementRateLimitRejects();
        metrics.recordPublishedBatch(4, 1_000_000L);

        assertThat(registry.get("outbox.queue.size").gauge().value()).isEqualTo(5.0);
        assertThat(registry.get("outbox.queue.pressure").gauge().value()).isEqualTo(0.5);
        assertThat(registry.get("outbox.queue.enqueue").counter().count()).isEqualTo(1.0);
        assertThat(registry.get("outbox.publish.failures").counter().count()).isEqualTo(1.0);
        assertThat(registry.get("outbox.recovery.count").counter().count()).isEqualTo(3.0);
    }
}
