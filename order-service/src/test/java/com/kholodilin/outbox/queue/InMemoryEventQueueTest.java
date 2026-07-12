package com.kholodilin.outbox.queue;

import com.kholodilin.outbox.config.AppProperties;
import com.kholodilin.outbox.metrics.OutboxMetrics;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

class InMemoryEventQueueTest {

    private InMemoryEventQueue queue;

    @BeforeEach
    void setUp() {
        AppProperties properties = new AppProperties();
        properties.getOutbox().getMemoryQueue().setCapacity(2);
        properties.getOutbox().getMemoryQueue().setBatchSize(10);
        properties.getOutbox().getMemoryQueue().setBatchWait(Duration.ofMillis(10));
        queue = new InMemoryEventQueue(properties, new OutboxMetrics(new SimpleMeterRegistry()));
    }

    @Test
    void deduplicatesEnqueue() {
        assertThat(queue.enqueue(1L)).isTrue();
        assertThat(queue.enqueue(1L)).isFalse();
        assertThat(queue.size()).isOne();
    }

    @Test
    void rejectsWhenFull() {
        assertThat(queue.enqueue(1L)).isTrue();
        assertThat(queue.enqueue(2L)).isTrue();
        assertThat(queue.enqueue(3L)).isFalse();
    }

    @Test
    void drainsBatch() throws Exception {
        queue.enqueue(1L);
        queue.enqueue(2L);
        Long first = queue.poll(100);
        List<Long> drained = queue.drainBatch(10);
        assertThat(first).isEqualTo(1L);
        assertThat(drained).containsExactly(2L);
    }

    @Test
    void reportsPressure() {
        queue.enqueue(1L);
        assertThat(queue.pressure()).isEqualTo(0.5);
        assertThat(queue.capacity()).isEqualTo(2);
    }

    @Test
    void allowsEnqueueAfterQueueSlotFreed() throws Exception {
        assertThat(queue.enqueue(1L)).isTrue();
        assertThat(queue.enqueue(2L)).isTrue();
        assertThat(queue.enqueue(3L)).isFalse();
        queue.poll(100);
        assertThat(queue.enqueue(3L)).isTrue();
    }
}
