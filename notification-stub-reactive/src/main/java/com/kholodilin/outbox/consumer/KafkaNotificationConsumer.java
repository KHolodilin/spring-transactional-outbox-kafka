package com.kholodilin.outbox.consumer;

import com.kholodilin.outbox.config.NotificationStubProperties;
import com.kholodilin.outbox.events.EventEnvelope;
import com.kholodilin.outbox.notification.NotificationStubHandler;
import jakarta.annotation.PreDestroy;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.common.TopicPartition;
import org.springframework.boot.context.event.ApplicationReadyEvent;
import org.springframework.boot.sql.init.dependency.DependsOnDatabaseInitialization;
import org.springframework.context.event.EventListener;
import org.springframework.stereotype.Component;
import reactor.core.Disposable;
import reactor.core.publisher.Mono;
import reactor.kafka.receiver.KafkaReceiver;
import reactor.kafka.receiver.ReceiverRecord;
import reactor.util.retry.Retry;

import java.time.Duration;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;

@Slf4j
@Component
@RequiredArgsConstructor
@DependsOnDatabaseInitialization
public class KafkaNotificationConsumer {

    private final KafkaReceiver<String, EventEnvelope> kafkaReceiver;
    private final NotificationStubHandler notificationStubHandler;
    private final NotificationStubProperties properties;

    private final AtomicBoolean started = new AtomicBoolean(false);
    private Disposable subscription;

    @EventListener(ApplicationReadyEvent.class)
    void start() {
        if (!started.compareAndSet(false, true)) {
            return;
        }

        log.info("Notification Kafka consumer starting after database initialization");

        subscription = kafkaReceiver
                .receive()
                .doOnNext(this::acknowledgeMalformedRecord)
                .filter(record -> record.value() != null)
                .bufferTimeout(
                        properties.getKafka().getBatchSize(),
                        properties.getKafka().getBatchWait()
                )
                .filter(batch -> !batch.isEmpty())
                .concatMap(this::processBatch)
                /*
                 * Technical error terminates the current receive pipeline.
                 * The failing batch was never acknowledged,
                 * so Kafka can redeliver it after the receiver retries.
                 */
                .retryWhen(
                        Retry.backoff(
                                        Long.MAX_VALUE,
                                        Duration.ofSeconds(1)
                                )
                                .maxBackoff(Duration.ofSeconds(30))
                                .doBeforeRetry(signal ->
                                        log.warn(
                                                "Notification Kafka consumer retrying after failure attempt={} reason={}",
                                                signal.totalRetries() + 1,
                                                signal.failure().toString()
                                        )
                                )
                )
                .subscribe(
                        ignored -> {
                        },
                        exception ->
                                log.error(
                                        "Notification Kafka consumer terminated",
                                        exception
                                )
                );
    }

    /**
     * Poison JSON becomes a null value via {@code ErrorHandlingDeserializer}.
     * Acknowledge and drop it so the rest of the partitions keep consuming.
     */
    void acknowledgeMalformedRecord(
            ReceiverRecord<String, EventEnvelope> record
    ) {
        if (record.value() != null) {
            return;
        }

        log.warn(
                "Notification Kafka consumer skipped malformed record partition={} offset={}",
                record.partition(),
                record.offset()
        );
        record.receiverOffset().acknowledge();
    }

    Mono<Void> processBatch(
            List<ReceiverRecord<String, EventEnvelope>> batch
    ) {
        return notificationStubHandler
                .handleBatch(batch)
                .then(
                        Mono.fromRunnable(
                                () -> acknowledgeLastOffsets(batch)
                        )
                );
    }

    /**
     * Acknowledge only the greatest successfully processed offset
     * from each Kafka partition.
     */
    void acknowledgeLastOffsets(
            List<ReceiverRecord<String, EventEnvelope>> batch
    ) {

        Map<TopicPartition, ReceiverRecord<String, EventEnvelope>>
                lastRecordByPartition = new HashMap<>();

        for (ReceiverRecord<String, EventEnvelope> record : batch) {

            TopicPartition partition = new TopicPartition(
                    record.topic(),
                    record.partition()
            );

            lastRecordByPartition.merge(
                    partition,
                    record,
                    (current, candidate) ->
                            candidate.offset() > current.offset()
                                    ? candidate
                                    : current
            );
        }

        lastRecordByPartition
                .values()
                .forEach(record -> {

                    log.debug(
                            "Acknowledging Kafka partition={} offset={}",
                            record.partition(),
                            record.offset()
                    );

                    record.receiverOffset().acknowledge();
                });
    }

    @PreDestroy
    void stop() {
        if (subscription != null) {
            subscription.dispose();
        }
    }
}
