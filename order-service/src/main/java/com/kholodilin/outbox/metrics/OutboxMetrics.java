package com.kholodilin.outbox.metrics;

import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.Gauge;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Timer;
import org.springframework.stereotype.Component;

import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

/** Micrometer gauges and counters for the outbox publishing pipeline. */
@Component
public class OutboxMetrics {

    private final AtomicInteger queueSize = new AtomicInteger();
    private final AtomicReference<Double> queuePressure = new AtomicReference<>(0.0);
    private final Timer publishLatency;
    private final Counter publishFailures;
    private final Counter retryCount;
    private final Counter recoveryCount;
    private final Counter rateLimitRejects;

    public OutboxMetrics(MeterRegistry registry) {
        Gauge.builder("outbox.queue.size", queueSize, AtomicInteger::get).register(registry);
        Gauge.builder("outbox.queue.pressure", queuePressure, AtomicReference::get).register(registry);
        this.publishLatency = Timer.builder("outbox.publish.latency").register(registry);
        this.publishFailures = Counter.builder("outbox.publish.failures").register(registry);
        this.retryCount = Counter.builder("outbox.retry.count").register(registry);
        this.recoveryCount = Counter.builder("outbox.recovery.count").register(registry);
        this.rateLimitRejects = Counter.builder("outbox.rate_limit.rejects").register(registry);
    }

    public void updateQueue(int size, double pressure) {
        queueSize.set(size);
        queuePressure.set(pressure);
    }

    public Timer publishLatency() {
        return publishLatency;
    }

    public void incrementPublishFailures() {
        publishFailures.increment();
    }

    public void incrementRetryCount() {
        retryCount.increment();
    }

    public void incrementRecoveryCount(int count) {
        recoveryCount.increment(count);
    }

    public void incrementRateLimitRejects() {
        rateLimitRejects.increment();
    }
}
