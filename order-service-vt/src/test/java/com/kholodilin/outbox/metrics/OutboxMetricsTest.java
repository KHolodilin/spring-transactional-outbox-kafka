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
        metrics.incrementEnqueue();
        metrics.incrementDequeue(3);
        metrics.recordPublishedBatch(7, java.util.concurrent.TimeUnit.MILLISECONDS.toNanos(10));
        metrics.recordPublishedBatch(0, 1L);
        metrics.recordPublishedBatch(-1, 1L);
        metrics.incrementDequeue(0);
        metrics.publishLatency().record(java.time.Duration.ofMillis(5));
        metrics.orderTransaction().record(java.time.Duration.ofMillis(20));

        assertThat(registry.find("outbox.queue.size").gauge().value()).isEqualTo(5.0);
        assertThat(registry.find("outbox.queue.pressure").gauge().value()).isEqualTo(0.25);
        assertThat(registry.find("outbox.publish.failures").counter().count()).isEqualTo(1.0);
        assertThat(registry.find("outbox.retry.count").counter().count()).isEqualTo(1.0);
        assertThat(registry.find("outbox.recovery.count").counter().count()).isEqualTo(2.0);
        assertThat(registry.find("outbox.rate_limit.rejects").counter().count()).isEqualTo(1.0);
        assertThat(registry.find("outbox.queue.enqueue").counter().count()).isEqualTo(1.0);
        assertThat(registry.find("outbox.queue.dequeue").counter().count()).isEqualTo(3.0);
        assertThat(registry.find("outbox.publish.latency").timer().count()).isEqualTo(2);
        assertThat(registry.find("outbox.publish.batch.size").summary().count()).isEqualTo(1);
        assertThat(registry.find("outbox.publish.batch.size").summary().totalAmount()).isEqualTo(7.0);
        assertThat(registry.find("outbox.publish.events").counter().count()).isEqualTo(7.0);
        assertThat(registry.find("order.transaction").timer().count()).isEqualTo(1);
    }
}
