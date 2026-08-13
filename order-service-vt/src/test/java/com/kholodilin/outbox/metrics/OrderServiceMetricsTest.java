package com.kholodilin.outbox.metrics;

import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import org.junit.jupiter.api.Test;
import org.springframework.test.util.ReflectionTestUtils;

import static org.assertj.core.api.Assertions.assertThat;

class OrderServiceMetricsTest {

    @Test
    void registersAndUpdatesMeters() {
        SimpleMeterRegistry registry = new SimpleMeterRegistry();
        OrderServiceMetrics metrics = new OrderServiceMetrics(registry);
        ReflectionTestUtils.invokeMethod(metrics, "registerMeters");

        metrics.incrementRateLimitRejects();
        metrics.incrementBackpressureRejects();
        metrics.incrementPoolExhaustedRejects();
        metrics.updateBackpressureInFlight(12);
        metrics.orderTransaction().record(java.time.Duration.ofMillis(20));

        assertThat(registry.find("outbox.rate_limit.rejects").counter().count()).isEqualTo(1.0);
        assertThat(registry.find("outbox.backpressure.rejects").counter().count()).isEqualTo(1.0);
        assertThat(registry.find("outbox.pool_exhausted.rejects").counter().count()).isEqualTo(1.0);
        assertThat(registry.find("outbox.backpressure.in_flight").gauge().value()).isEqualTo(12.0);
        assertThat(registry.find("order.transaction").timer().count()).isEqualTo(1);
    }
}
