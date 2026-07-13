package com.kholodilin.outbox.metrics;

import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import org.junit.jupiter.api.Test;
import org.springframework.test.util.ReflectionTestUtils;

import static org.assertj.core.api.Assertions.assertThat;

class OutboxMetricsTest {

    @Test
    void registersAndUpdatesMeters() {
        SimpleMeterRegistry registry = new SimpleMeterRegistry();
        OutboxMetrics metrics = new OutboxMetrics(registry);
        ReflectionTestUtils.invokeMethod(metrics, "registerMeters");

        metrics.updateQueue(5, 0.25);
        metrics.incrementPublishFailures();
        metrics.incrementRetryCount();
        metrics.incrementRecoveryCount(2);
        metrics.incrementRateLimitRejects();
        metrics.publishLatency().record(java.time.Duration.ofMillis(10));

        assertThat(registry.find("outbox.queue.size").gauge().value()).isEqualTo(5.0);
        assertThat(registry.find("outbox.queue.pressure").gauge().value()).isEqualTo(0.25);
        assertThat(registry.find("outbox.publish.failures").counter().count()).isEqualTo(1.0);
        assertThat(registry.find("outbox.retry.count").counter().count()).isEqualTo(1.0);
        assertThat(registry.find("outbox.recovery.count").counter().count()).isEqualTo(2.0);
        assertThat(registry.find("outbox.rate_limit.rejects").counter().count()).isEqualTo(1.0);
        assertThat(registry.find("outbox.publish.latency").timer().count()).isEqualTo(1);
    }
}
