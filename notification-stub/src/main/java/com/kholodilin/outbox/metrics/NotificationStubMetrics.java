package com.kholodilin.outbox.metrics;

import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Timer;
import jakarta.annotation.PostConstruct;
import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Component;

/** Micrometer metrics for the notification stub Kafka consumer. */
@Component
@RequiredArgsConstructor
public class NotificationStubMetrics {

    private final MeterRegistry registry;

    private Counter eventsReceived;
    private Counter batchesReceived;
    private Timer batchProcessing;

    @PostConstruct
    void registerMeters() {
        eventsReceived = Counter.builder("notification.events.received").register(registry);
        batchesReceived = Counter.builder("notification.batches.received").register(registry);
        batchProcessing = Timer.builder("notification.batch.processing").register(registry);
    }

    public void recordBatch(int batchSize, Runnable processing) {
        batchesReceived.increment();
        eventsReceived.increment(batchSize);
        batchProcessing.record(processing);
    }
}
