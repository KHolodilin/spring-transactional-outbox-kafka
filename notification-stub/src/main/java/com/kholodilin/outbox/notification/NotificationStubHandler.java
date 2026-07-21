package com.kholodilin.outbox.notification;

import com.kholodilin.outbox.events.EventConstants;
import com.kholodilin.outbox.events.EventEnvelope;
import com.kholodilin.outbox.logging.InstanceMdcInitializer;
import com.kholodilin.outbox.logging.StructuredLogContext;
import com.kholodilin.outbox.metrics.NotificationStubMetrics;
import com.kholodilin.outbox.tracing.TraceContextSupport;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.header.Header;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

import java.nio.charset.StandardCharsets;
import java.util.List;

/**
 * Demo downstream consumer that pretends to send customer notifications.
 * <p>
 * No real email/SMS integration — logs only, to show the end-to-end outbox pipeline.
 * Restores W3C trace context from Kafka {@code traceparent} headers.
 */
@Slf4j
@Component
@RequiredArgsConstructor
public class NotificationStubHandler {

    private final NotificationStubMetrics metrics;
    private final TraceContextSupport traceContextSupport;
    private final InstanceMdcInitializer instanceMdcInitializer;

    /**
     * Consumes a Kafka batch of order events and logs a mock notification per record.
     * <p>
     * Restores W3C {@code traceparent} for the batch span and again per record so Tempo
     * continues the publisher's trace. Metrics record batch size and processing time.
     *
     * @param records polled consumer records (may be empty)
     */
    @KafkaListener(
            topics = "${app.kafka.topic}",
            containerFactory = "batchKafkaListenerContainerFactory"
    )
    public void handleBatch(List<ConsumerRecord<String, EventEnvelope>> records) {
        if (records.isEmpty()) {
            return;
        }
        String batchTraceParent = extractTraceParent(records.get(0));
        traceContextSupport.runWithTraceParent(batchTraceParent, "notification.batch.receive", () -> {
            instanceMdcInitializer.enrich();
            StructuredLogContext.putEventAction("notification.batch.received");
            StructuredLogContext.putBatchSize(records.size());
            long start = System.nanoTime();
            metrics.recordBatch(records.size(), () -> {
                log.info("Notification stub batch received size={}", records.size());
                StructuredLogContext.putEventAction("notification.processing.started");
                for (ConsumerRecord<String, EventEnvelope> record : records) {
                    traceContextSupport.runWithTraceParent(
                            extractTraceParent(record),
                            "notification.consume",
                            () -> logEvent(record)
                    );
                }
                StructuredLogContext.putDurationMs((System.nanoTime() - start) / 1_000_000);
                StructuredLogContext.putEventAction("notification.processed");
                log.info("Notification stub batch processed size={}", records.size());
            });
            instanceMdcInitializer.clearConsumerContext();
        });
    }

    private void logEvent(ConsumerRecord<String, EventEnvelope> record) {
        EventEnvelope event = record.value();
        instanceMdcInitializer.enrich();
        StructuredLogContext.putCorrelation(event.correlationId(), event.customerId());
        StructuredLogContext.putOrderFields(event.orderId(), event.eventId());
        StructuredLogContext.putEventType(event.eventType());
        StructuredLogContext.putKafkaFields(record.topic(), record.partition(), record.offset());
        StructuredLogContext.putNotificationFields("log", "sent");
        StructuredLogContext.putEventAction("notification.processed");
        log.info("Notification stub sent orderId={} customerId={} eventId={}",
                event.orderId(), event.customerId(), event.eventId());
        log.debug("Notification stub event details eventType={} correlationId={} payload={}",
                event.eventType(), event.correlationId(), event.payload());
        instanceMdcInitializer.clearConsumerContext();
    }

    private String extractTraceParent(ConsumerRecord<String, EventEnvelope> record) {
        Header header = record.headers().lastHeader(EventConstants.HEADER_TRACEPARENT);
        if (header == null || header.value() == null) {
            return null;
        }
        return new String(header.value(), StandardCharsets.UTF_8);
    }
}
