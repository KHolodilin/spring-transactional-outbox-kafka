package com.kholodilin.outbox.metrics;

import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.Gauge;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Timer;
import jakarta.annotation.PostConstruct;
import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Component;

import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

/** Micrometer gauges and counters for the outbox publishing pipeline. */
@Component
@RequiredArgsConstructor
public class OutboxMetrics {

    private final MeterRegistry registry;

    private final AtomicInteger queueSize = new AtomicInteger();
    private final AtomicReference<Double> queuePressure = new AtomicReference<>(0.0);
    private Timer publishLatency;
    private Counter publishFailures;
    private Counter retryCount;
    private Counter recoveryCount;
    private Counter rateLimitRejects;

    @PostConstruct
    void registerMeters() {
        Gauge.builder("outbox.queue.size", queueSize, AtomicInteger::get).register(registry);
        Gauge.builder("outbox.queue.pressure", queuePressure, AtomicReference::get).register(registry);
        publishLatency = Timer.builder("outbox.publish.latency").register(registry);
        publishFailures = Counter.builder("outbox.publish.failures").register(registry);
        retryCount = Counter.builder("outbox.retry.count").register(registry);
        recoveryCount = Counter.builder("outbox.recovery.count").register(registry);
        rateLimitRejects = Counter.builder("outbox.rate_limit.rejects").register(registry);
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
