package com.kholodilin.outbox.notification;

import com.kholodilin.idempotency.exception.IdempotencyConflictException;
import com.kholodilin.outbox.events.EventEnvelope;
import com.kholodilin.outbox.events.ObservabilityVocabulary;
import com.kholodilin.outbox.logging.InstanceMdcInitializer;
import com.kholodilin.outbox.logging.StructuredLogContext;
import com.kholodilin.outbox.metrics.NotificationStubMetrics;
import com.kholodilin.outbox.tracing.TraceContextSupport;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.stereotype.Component;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.util.List;

@Slf4j
@Component
@RequiredArgsConstructor
public class NotificationStubHandler {

    private final NotificationStubMetrics metrics;
    private final TraceContextSupport traceContextSupport;
    private final InstanceMdcInitializer instanceMdcInitializer;
    private final NotificationTransactionService notificationTransactionService;

    public Mono<Void> handleBatch(
            List<? extends ConsumerRecord<String, EventEnvelope>> records
    ) {

        if (records.isEmpty()) {
            return Mono.empty();
        }

        ConsumerRecord<String, EventEnvelope> firstRecord =
                records.get(0);

        return traceContextSupport.withRecordTrace(
                firstRecord,
                ObservabilityVocabulary.SPAN_NOTIFICATION_BATCH_RECEIVE,
                () -> Mono.defer(() -> {

                    instanceMdcInitializer.enrich();

                    StructuredLogContext.putEventAction(
                            ObservabilityVocabulary.NOTIFICATION_BATCH_RECEIVED
                    );

                    StructuredLogContext.putBatchSize(
                            records.size()
                    );

                    long startNs = System.nanoTime();

                    return metrics.recordBatch(
                                    records.size(),
                                    () -> {

                                        log.info(
                                                "Notification stub batch received size={}",
                                                records.size()
                                        );

                                        StructuredLogContext.putEventAction(
                                                ObservabilityVocabulary.NOTIFICATION_PROCESSING_STARTED
                                        );

                                        return Flux.fromIterable(records)
                                                .concatMap(this::processRecord)
                                                .then();
                                    }
                            )
                            .doOnSuccess(ignored -> {

                                instanceMdcInitializer.enrich();

                                StructuredLogContext.putDurationMs(
                                        (System.nanoTime() - startNs)
                                                / 1_000_000
                                );

                                StructuredLogContext.putEventAction(
                                        ObservabilityVocabulary.NOTIFICATION_PROCESSED
                                );

                                log.info(
                                        "Notification stub batch processed size={}",
                                        records.size()
                                );
                            })
                            .doFinally(signalType ->
                                               instanceMdcInitializer
                                                       .clearConsumerContext()
                            );
                })
        );
    }

    private Mono<Void> processRecord(
            ConsumerRecord<String, EventEnvelope> record
    ) {

        EventEnvelope event = record.value();

        return traceContextSupport.withRecordTrace(
                record,
                ObservabilityVocabulary.SPAN_NOTIFICATION_CONSUME,
                () -> Mono.defer(() -> {

                    enrichRecordContext(record, event);

                    return notificationTransactionService
                            .process(event)

                            .doOnNext(sent -> {

                                enrichRecordContext(
                                        record,
                                        event
                                );

                                if (!sent) {

                                    StructuredLogContext
                                            .putNotificationFields(
                                                    "log",
                                                    "skipped"
                                            );

                                    StructuredLogContext
                                            .putEventAction(
                                                    ObservabilityVocabulary.NOTIFICATION_DUPLICATE_SKIPPED
                                            );

                                    log.info(
                                            "Notification stub skipped duplicate eventId={}",
                                            event.eventId()
                                    );
                                }
                            })

                            .onErrorResume(
                                    IdempotencyConflictException.class,
                                    exception -> {

                                        enrichRecordContext(
                                                record,
                                                event
                                        );

                                        StructuredLogContext
                                                .putNotificationFields(
                                                        "log",
                                                        "skipped"
                                                );

                                        StructuredLogContext
                                                .putEventAction(
                                                        ObservabilityVocabulary.NOTIFICATION_CONFLICT_SKIPPED
                                                );

                                        log.warn(
                                                "Notification stub skipped conflicting eventId={} reason={}",
                                                event.eventId(),
                                                exception.getMessage()
                                        );

                                        return Mono.empty();
                                    }
                            )

                            .doOnError(exception -> {

                                enrichRecordContext(
                                        record,
                                        event
                                );

                                StructuredLogContext
                                        .putNotificationFields(
                                                "log",
                                                "failed"
                                        );

                                StructuredLogContext
                                        .putEventAction(
                                                ObservabilityVocabulary.NOTIFICATION_PROCESSING_FAILED
                                        );

                                log.error(
                                        "Notification stub failed eventId={}",
                                        event.eventId(),
                                        exception
                                );
                            })

                            .then()

                            .doFinally(signalType ->
                                               instanceMdcInitializer
                                                       .clearConsumerContext()
                            );
                })
        );
    }

    private void enrichRecordContext(
            ConsumerRecord<String, EventEnvelope> record,
            EventEnvelope event
    ) {

        instanceMdcInitializer.enrich();

        StructuredLogContext.putCorrelation(
                event.correlationId(),
                event.customerId()
        );

        StructuredLogContext.putOrderFields(
                event.orderId(),
                event.eventId()
        );

        StructuredLogContext.putEventType(
                event.eventType()
        );

        StructuredLogContext.putKafkaFields(
                record.topic(),
                record.partition(),
                record.offset()
        );
    }
}