package com.kholodilin.outbox.metrics;

import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Timer;
import jakarta.annotation.PostConstruct;
import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Component;
import reactor.core.publisher.Mono;

import java.util.function.Supplier;

@Component
@RequiredArgsConstructor
public class NotificationStubMetrics {

    private final MeterRegistry registry;

    private Counter eventsReceived;
    private Counter batchesReceived;
    private Timer batchProcessing;

    @PostConstruct
    void registerMeters() {
        eventsReceived = Counter.builder("notification.events.received")
                .register(registry);

        batchesReceived = Counter.builder("notification.batches.received")
                .register(registry);

        batchProcessing = Timer.builder("notification.batch.processing")
                .register(registry);
    }

    public Mono<Void> recordBatch(
            int batchSize,
            Supplier<Mono<Void>> processing
    ) {
        return Mono.defer(() -> {

            batchesReceived.increment();
            eventsReceived.increment(batchSize);

            Timer.Sample sample = Timer.start(registry);

            return processing.get()
                    .doFinally(signalType ->
                                       sample.stop(batchProcessing)
                    );
        });
    }
}