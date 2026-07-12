package com.kholodilin.outbox.queue;

import com.kholodilin.outbox.config.AppProperties;
import com.kholodilin.outbox.metrics.OutboxMetrics;
import jakarta.annotation.PostConstruct;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;

/**
 * Per-pod hot queue for outbox event IDs.
 * <p>
 * This is the only entry point for the Kafka publisher (fast path after commit and recovery path).
 * IDs are deduplicated while waiting in the queue; if the queue is full the event stays NEW in
 * PostgreSQL and recovery will pick it up later.
 */
@Slf4j
@Component
@RequiredArgsConstructor
public class InMemoryEventQueue {

    private final AppProperties properties;
    private final OutboxMetrics metrics;

    private BlockingQueue<Long> queue;
    private Set<Long> dedup;
    private int capacity;

    @PostConstruct
    void init() {
        capacity = properties.getOutbox().getMemoryQueue().getCapacity();
        queue = new ArrayBlockingQueue<>(capacity);
        dedup = ConcurrentHashMap.newKeySet();
        updateMetrics();
    }

    /**
     * Adds an event id to the queue. Returns false when the id is already queued (dedup)
     * or when the bounded queue is full.
     */
    public boolean enqueue(long eventId) {
        if (!dedup.add(eventId)) {
            log.debug("Duplicate enqueue ignored eventId={}", eventId);
            return false;
        }
        boolean offered = queue.offer(eventId);
        if (!offered) {
            // Roll back dedup so recovery can enqueue the same id again later.
            dedup.remove(eventId);
            log.warn("Memory queue full, rejected eventId={}", eventId);
            updateMetrics();
            return false;
        }
        log.debug("Enqueued eventId={} queueSize={}", eventId, queue.size());
        updateMetrics();
        return true;
    }

    /** Waits up to {@code timeoutMs} for the next event id; removes it from the dedup set. */
    public Long poll(long timeoutMs) throws InterruptedException {
        Long eventId = queue.poll(timeoutMs, TimeUnit.MILLISECONDS);
        if (eventId != null) {
            dedup.remove(eventId);
            updateMetrics();
        }
        return eventId;
    }

    /** Non-blocking drain of up to {@code batchSize} ids for the publisher worker. */
    public List<Long> drainBatch(int batchSize) {
        List<Long> batch = new ArrayList<>(batchSize);
        queue.drainTo(batch, batchSize);
        for (Long eventId : batch) {
            dedup.remove(eventId);
        }
        updateMetrics();
        log.debug("Drained batch size={} remaining={}", batch.size(), queue.size());
        return batch;
    }

    public int size() {
        return queue.size();
    }

    /** Queue fill ratio in [0..1], used for adaptive rate limiting and health checks. */
    public double pressure() {
        return capacity == 0 ? 0.0 : (double) queue.size() / capacity;
    }

    public int capacity() {
        return capacity;
    }

    private void updateMetrics() {
        metrics.updateQueue(queue.size(), pressure());
    }
}
