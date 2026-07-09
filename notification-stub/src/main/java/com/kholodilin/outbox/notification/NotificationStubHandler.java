package com.kholodilin.outbox.notification;

import com.kholodilin.outbox.events.EventEnvelope;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

import java.util.List;

/**
 * Demo downstream consumer that pretends to send customer notifications.
 * <p>
 * No real email/SMS integration — logs only, to show the end-to-end outbox pipeline.
 */
@Slf4j
@Component
public class NotificationStubHandler {

    @KafkaListener(
            topics = "${app.kafka.topic}",
            containerFactory = "batchKafkaListenerContainerFactory"
    )
    public void handleBatch(List<EventEnvelope> events) {
        long start = System.nanoTime();
        log.info("Notification stub batch received size={}", events.size());
        for (EventEnvelope event : events) {
            log.info("Notification stub sent orderId={} customerId={} eventId={}",
                    event.getOrderId(), event.getCustomerId(), event.getEventId());
            log.debug("Notification stub event details eventType={} correlationId={} payload={}",
                    event.getEventType(), event.getCorrelationId(), event.getPayload());
        }
        long durationMs = (System.nanoTime() - start) / 1_000_000;
        log.info("Notification stub batch processed size={} durationMs={}", events.size(), durationMs);
    }
}
