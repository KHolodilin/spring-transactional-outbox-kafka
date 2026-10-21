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
 * Micrometer meters for the create-order rate limit, bulkhead, pool exhaustion, and order transaction.
 * Names match the servlet service so the same dashboards apply.
 */
@Component
@RequiredArgsConstructor
public class OrderServiceMetrics {

    private final MeterRegistry registry;

    private Timer orderTransaction;
    private Counter rateLimitRejects;
    private Counter bulkheadRejects;
    private Counter poolExhaustedRejects;
    private final AtomicInteger bulkheadInFlight = new AtomicInteger();

    @PostConstruct
    void registerMeters() {
        Gauge.builder("order.bulkhead.in_flight", bulkheadInFlight, AtomicInteger::get).register(registry);
        orderTransaction = Timer.builder("order.transaction")
                .publishPercentileHistogram()
                .publishPercentiles(0.5, 0.95, 0.99)
                .register(registry);
        rateLimitRejects = Counter.builder("outbox.rate_limit.rejects").register(registry);
        bulkheadRejects = Counter.builder("order.bulkhead.rejects").register(registry);
        poolExhaustedRejects = Counter.builder("outbox.pool_exhausted.rejects").register(registry);
    }

    /**
     * @return timer for the create-order DB transaction (order, outbox, and idempotency)
     */
    public Timer orderTransaction() {
        return orderTransaction;
    }

    /** Increments {@code outbox.rate_limit.rejects} when the rate-limit filter returns 429. */
    public void incrementRateLimitRejects() {
        rateLimitRejects.increment();
    }

    /** Increments {@code order.bulkhead.rejects} when the create bulkhead returns 429. */
    public void incrementBulkheadRejects() {
        bulkheadRejects.increment();
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
    public void updateBulkheadInFlight(int inFlight) {
        bulkheadInFlight.set(Math.max(0, inFlight));
    }
}
