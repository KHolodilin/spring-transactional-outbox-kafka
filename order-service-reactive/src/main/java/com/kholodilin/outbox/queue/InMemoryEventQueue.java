package com.kholodilin.outbox.queue;

import com.kholodilin.outbox.config.AppProperties;
import com.kholodilin.outbox.metrics.OutboxMetrics;
import jakarta.annotation.PostConstruct;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;
import reactor.core.publisher.Mono;
import reactor.core.scheduler.Schedulers;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;

/**
 * Per-pod hot queue for outbox event IDs (mirrors servlet {@code InMemoryEventQueue} semantics).
 * <p>
 * Dedup + inFlight prevent duplicate work; a full queue leaves the event NEW in PostgreSQL for recovery.
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

    /** Blocking poll wrapped for the reactive publisher loop (runs on boundedElastic). */
    public Mono<Long> pollMono(Duration timeout) {
        return Mono.<Long>create(sink -> {
                    try {
                        Long eventId = poll(timeout.toMillis());
                        if (eventId == null) {
                            sink.success();
                        } else {
                            sink.success(eventId);
                        }
                    } catch (InterruptedException ex) {
                        Thread.currentThread().interrupt();
                        sink.error(ex);
                    } catch (Exception ex) {
                        sink.error(ex);
                    }
                })
                .subscribeOn(Schedulers.boundedElastic());
    }

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

    public void acknowledge(Collection<Long> eventIds) {
        for (Long eventId : eventIds) {
            inFlight.remove(eventId);
        }
    }

    public int size() {
        return queue.size();
    }

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
