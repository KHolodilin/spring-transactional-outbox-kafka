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
 * IDs already waiting in the queue or currently in-flight (being published) are rejected;
 * if the queue is full the event stays NEW in PostgreSQL and recovery will pick it up later.
 * <p>
 * <b>Remark on concurrency / atomicity.</b>
 * {@code queue}, {@code dedup}, and {@code inFlight} are not updated under one critical section,
 * so narrow TOCTOU windows remain (e.g. between {@code inFlight.contains} and {@code dedup.add},
 * or between {@code dedup.remove} and {@code inFlight.add} on poll). That is a known trade-off,
 * not an oversight.
 * <p>
 * We deliberately keep this design: the in-memory layer is coalescing and backpressure only.
 * Correctness of publish / retry lives in PostgreSQL ({@code claimByIds}, lease, status).
 * A duplicate id in the queue at worst causes an extra claim attempt that the DB rejects.
 * A global {@code ReentrantLock} or atomic {@code ABSENT → QUEUED → IN_FLIGHT} map would close
 * the windows but add complexity without changing the source of truth — so it was not adopted.
 */
@Slf4j
@Component
@RequiredArgsConstructor
public class InMemoryEventQueue {

    private final AppProperties properties;
    private final OutboxMetrics metrics;

    private BlockingQueue<Long> queue;
    private Set<Long> dedup;
    private Set<Long> inFlight;
    private int capacity;

    @PostConstruct
    void init() {
        capacity = properties.getOutbox().getMemoryQueue().getCapacity();
        queue = new ArrayBlockingQueue<>(capacity);
        dedup = ConcurrentHashMap.newKeySet();
        inFlight = ConcurrentHashMap.newKeySet();
        updateMetrics();
    }

    /**
     * Adds an event id to the queue.
     *
     * @return {@code false} when the id is already queued, currently in-flight (being published),
     *         or the bounded queue is full
     */
    public boolean enqueue(long eventId) {
        if (inFlight.contains(eventId)) {
            log.debug("In-flight enqueue ignored eventId={}", eventId);
            return false;
        }
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
        metrics.incrementEnqueue();
        log.debug("Enqueued eventId={} queueSize={}", eventId, queue.size());
        updateMetrics();
        return true;
    }

    /** Waits up to {@code timeoutMs} for the next event id; removes it from the dedup set. */
    public Long poll(long timeoutMs) throws InterruptedException {
        Long eventId = queue.poll(timeoutMs, TimeUnit.MILLISECONDS);
        if (eventId != null) {
            dedup.remove(eventId);
            inFlight.add(eventId);
            metrics.incrementDequeue(1);
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
            inFlight.add(eventId);
        }
        metrics.incrementDequeue(batch.size());
        updateMetrics();
        log.debug("Drained batch size={} remaining={}", batch.size(), queue.size());
        return batch;
    }

    /** Called after publish completes (success or failure) so recovery can re-enqueue if needed. */
    public void acknowledge(java.util.Collection<Long> eventIds) {
        for (Long eventId : eventIds) {
            inFlight.remove(eventId);
        }
    }

    /** @return number of event ids currently waiting in the bounded queue (excludes in-flight) */
    public int size() {
        return queue.size();
    }

    /** Queue fill ratio in [0..1], used for adaptive rate limiting and health checks. */
    public double pressure() {
        return capacity == 0 ? 0.0 : (double) queue.size() / capacity;
    }

    /** @return configured maximum queue capacity */
    public int capacity() {
        return capacity;
    }

    private void updateMetrics() {
        metrics.updateQueue(queue.size(), pressure());
    }
}
