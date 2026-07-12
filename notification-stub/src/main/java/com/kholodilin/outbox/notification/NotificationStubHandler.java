package com.kholodilin.outbox.notification;

import com.kholodilin.outbox.events.EventConstants;
import com.kholodilin.outbox.events.EventEnvelope;
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
            metrics.recordBatch(records.size(), () -> {
                log.info("Notification stub batch received size={}", records.size());
                for (ConsumerRecord<String, EventEnvelope> record : records) {
                    traceContextSupport.runWithTraceParent(
                            extractTraceParent(record),
                            "notification.consume",
                            () -> logEvent(record.value())
                    );
                }
                log.info("Notification stub batch processed size={}", records.size());
            });
        });
    }

    private void logEvent(EventEnvelope event) {
        log.info("Notification stub sent orderId={} customerId={} eventId={}",
                event.getOrderId(), event.getCustomerId(), event.getEventId());
        log.debug("Notification stub event details eventType={} correlationId={} payload={}",
                event.getEventType(), event.getCorrelationId(), event.getPayload());
    }

    private String extractTraceParent(ConsumerRecord<String, EventEnvelope> record) {
        Header header = record.headers().lastHeader(EventConstants.HEADER_TRACEPARENT);
        if (header == null || header.value() == null) {
            return null;
        }
        return new String(header.value(), StandardCharsets.UTF_8);
    }
}
