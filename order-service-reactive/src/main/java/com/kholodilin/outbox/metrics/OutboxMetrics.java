package com.kholodilin.outbox.metrics;

import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.DistributionSummary;
import io.micrometer.core.instrument.Gauge;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Timer;
import jakarta.annotation.PostConstruct;
import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Component;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

/** Micrometer gauges and counters for the outbox publishing pipeline (same metric names as order-service). */
@Component
@RequiredArgsConstructor
public class OutboxMetrics {

    private final MeterRegistry registry;

    private final AtomicInteger queueSize = new AtomicInteger();
    private final AtomicReference<Double> queuePressure = new AtomicReference<>(0.0);
    private Timer publishLatency;
    private Timer orderTransaction;
    private DistributionSummary publishBatchSize;
    private Counter publishEvents;
    private Counter publishFailures;
    private Counter retryCount;
    private Counter recoveryCount;
    private Counter rateLimitRejects;
    private Counter enqueueCount;
    private Counter dequeueCount;

    @PostConstruct
    void registerMeters() {
        Gauge.builder("outbox.queue.size", queueSize, AtomicInteger::get).register(registry);
        Gauge.builder("outbox.queue.pressure", queuePressure, AtomicReference::get).register(registry);
        publishLatency = Timer.builder("outbox.publish.latency")
                .publishPercentileHistogram()
                .publishPercentiles(0.5, 0.95, 0.99)
                .register(registry);
        orderTransaction = Timer.builder("order.transaction")
                .publishPercentileHistogram()
                .publishPercentiles(0.5, 0.95, 0.99)
                .register(registry);
        publishBatchSize = DistributionSummary.builder("outbox.publish.batch.size")
                .publishPercentileHistogram()
                .publishPercentiles(0.5, 0.95, 0.99)
                .register(registry);
        publishEvents = Counter.builder("outbox.publish.events").register(registry);
        publishFailures = Counter.builder("outbox.publish.failures").register(registry);
        retryCount = Counter.builder("outbox.retry.count").register(registry);
        recoveryCount = Counter.builder("outbox.recovery.count").register(registry);
        rateLimitRejects = Counter.builder("outbox.rate_limit.rejects").register(registry);
        enqueueCount = Counter.builder("outbox.queue.enqueue").register(registry);
        dequeueCount = Counter.builder("outbox.queue.dequeue").register(registry);
    }

    public void updateQueue(int size, double pressure) {
        queueSize.set(size);
        queuePressure.set(pressure);
    }

    public Timer publishLatency() {
        return publishLatency;
    }

    public Timer orderTransaction() {
        return orderTransaction;
    }

    public void recordPublishedBatch(int eventCount, long durationNs) {
        if (eventCount <= 0) {
            return;
        }
        publishLatency.record(durationNs, TimeUnit.NANOSECONDS);
        publishBatchSize.record(eventCount);
        publishEvents.increment(eventCount);
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

    public void incrementEnqueue() {
        enqueueCount.increment();
    }

    public void incrementDequeue(int count) {
        if (count > 0) {
            dequeueCount.increment(count);
        }
    }
}
