package com.kholodilin.outbox.metrics;

import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Timer;
import org.springframework.stereotype.Component;

/** Micrometer metrics for the notification stub Kafka consumer. */
@Component
public class NotificationStubMetrics {

    private final Counter eventsReceived;
    private final Counter batchesReceived;
    private final Timer batchProcessing;

    public NotificationStubMetrics(MeterRegistry registry) {
        this.eventsReceived = Counter.builder("notification.events.received").register(registry);
        this.batchesReceived = Counter.builder("notification.batches.received").register(registry);
        this.batchProcessing = Timer.builder("notification.batch.processing").register(registry);
    }

    public void recordBatch(int batchSize, Runnable processing) {
        batchesReceived.increment();
        eventsReceived.increment(batchSize);
        batchProcessing.record(processing);
    }
}
