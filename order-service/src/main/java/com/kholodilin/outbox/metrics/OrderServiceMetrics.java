package com.kholodilin.outbox.metrics;

import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.Gauge;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Timer;
import jakarta.annotation.PostConstruct;
import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Component;

import java.util.concurrent.atomic.AtomicInteger;

/**
 * App-owned Micrometer meters for order-service (rate limit, backpressure, pool, order TX).
 * Outbox pipeline meters are provided by {@code spring-boot-outbox-starter}.
 */
@Component
@RequiredArgsConstructor
public class OrderServiceMetrics {

    private final MeterRegistry registry;

    private Timer orderTransaction;
    private Counter rateLimitRejects;
    private Counter backpressureRejects;
    private Counter poolExhaustedRejects;
    private final AtomicInteger backpressureInFlight = new AtomicInteger();

    @PostConstruct
    void registerMeters() {
        Gauge.builder("outbox.backpressure.in_flight", backpressureInFlight, AtomicInteger::get).register(registry);
        orderTransaction = Timer.builder("order.transaction")
                .publishPercentileHistogram()
                .publishPercentiles(0.5, 0.95, 0.99)
                .register(registry);
        rateLimitRejects = Counter.builder("outbox.rate_limit.rejects").register(registry);
        backpressureRejects = Counter.builder("outbox.backpressure.rejects").register(registry);
        poolExhaustedRejects = Counter.builder("outbox.pool_exhausted.rejects").register(registry);
    }

    /**
     * @return timer for the create-order DB transaction ({@code order + outbox + idempotency})
     */
    public Timer orderTransaction() {
        return orderTransaction;
    }

    /** Increments {@code outbox.rate_limit.rejects} when the rate-limit filter returns 429. */
    public void incrementRateLimitRejects() {
        rateLimitRejects.increment();
    }

    /** Increments {@code outbox.backpressure.rejects} when the create bulkhead returns 429. */
    public void incrementBackpressureRejects() {
        backpressureRejects.increment();
    }

    /** Increments {@code outbox.pool_exhausted.rejects} when Hikari cannot hand out a connection. */
    public void incrementPoolExhaustedRejects() {
        poolExhaustedRejects.increment();
    }

    /**
     * Sets the gauge for in-flight create-order requests holding a bulkhead permit.
     *
     * @param inFlight {@code maxPermits - availablePermits}
     */
    public void updateBackpressureInFlight(int inFlight) {
        backpressureInFlight.set(Math.max(0, inFlight));
    }
}
