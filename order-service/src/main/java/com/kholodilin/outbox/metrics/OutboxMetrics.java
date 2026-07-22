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
    private Timer orderTransaction;
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
        publishFailures = Counter.builder("outbox.publish.failures").register(registry);
        retryCount = Counter.builder("outbox.retry.count").register(registry);
        recoveryCount = Counter.builder("outbox.recovery.count").register(registry);
        rateLimitRejects = Counter.builder("outbox.rate_limit.rejects").register(registry);
        enqueueCount = Counter.builder("outbox.queue.enqueue").register(registry);
        dequeueCount = Counter.builder("outbox.queue.dequeue").register(registry);
    }

    /**
     * Updates gauges reflecting the current in-memory queue occupancy.
     *
     * @param size     number of ids waiting in the queue
     * @param pressure {@code size / capacity} in {@code [0, 1]}
     */
    public void updateQueue(int size, double pressure) {
        queueSize.set(size);
        queuePressure.set(pressure);
    }

    /**
     * @return timer used to record Kafka publish batch latency
     */
    public Timer publishLatency() {
        return publishLatency;
    }

    /**
     * @return timer for the create-order DB transaction ({@code order + outbox + idempotency})
     */
    public Timer orderTransaction() {
        return orderTransaction;
    }

    /** Increments {@code outbox.publish.failures} after a Kafka batch send error. */
    public void incrementPublishFailures() {
        publishFailures.increment();
    }

    /** Increments {@code outbox.retry.count} when a row is marked FAILED / DEAD. */
    public void incrementRetryCount() {
        retryCount.increment();
    }

    /**
     * Adds {@code count} to {@code outbox.recovery.count} for successfully re-enqueued ids.
     *
     * @param count number of ids enqueued by recovery in one tick
     */
    public void incrementRecoveryCount(int count) {
        recoveryCount.increment(count);
    }

    /** Increments {@code outbox.rate_limit.rejects} when the rate-limit filter returns 429. */
    public void incrementRateLimitRejects() {
        rateLimitRejects.increment();
    }

    /** Increments {@code outbox.queue.enqueue} after a successful memory-queue offer. */
    public void incrementEnqueue() {
        enqueueCount.increment();
    }

    /**
     * Adds {@code count} to {@code outbox.queue.dequeue} when ids leave the memory queue
     * for publishing.
     *
     * @param count number of dequeued ids
     */
    public void incrementDequeue(int count) {
        if (count > 0) {
            dequeueCount.increment(count);
        }
    }
}
