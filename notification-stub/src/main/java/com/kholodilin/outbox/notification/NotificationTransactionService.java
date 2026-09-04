package com.kholodilin.outbox.notification;

import com.kholodilin.idempotency.ExecutionResult;
import com.kholodilin.idempotency.IdempotencyService;
import com.kholodilin.outbox.events.EventEnvelope;
import com.kholodilin.outbox.logging.StructuredLogContext;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.util.concurrent.atomic.AtomicBoolean;

/** Processes one notification event in its own idempotent database transaction. */
@Slf4j
@Service
@RequiredArgsConstructor
public class NotificationTransactionService {

    private static final String OPERATION = "NOTIFY_ORDER_CREATED";

    private final IdempotencyService idempotencyService;

    /**
     * Sends the mock notification only when the event has not already been processed.
     *
     * @param event consumed Kafka event
     * @return {@code true} when the notification action ran; {@code false} for a replay
     */
    @Transactional
    public boolean process(EventEnvelope event) {
        AtomicBoolean executed = new AtomicBoolean(false);

        ExecutionResult<Void> result = idempotencyService
                .operation(OPERATION)
                .key(String.valueOf(event.eventId()))
                .request(event)
                .execute(Void.class, () -> {
                    executed.set(true);
                    logNotification(event);
                    return ExecutionResult.success(null);
                });

        result.valueOrThrow();
        return executed.get();
    }

    private void logNotification(EventEnvelope event) {
        StructuredLogContext.putNotificationFields("log", "sent");
        StructuredLogContext.putEventAction("notification.processed");
        log.info("Notification stub sent orderId={} customerId={} eventId={}",
                event.orderId(), event.customerId(), event.eventId());
        log.debug("Notification stub event details eventType={} correlationId={} payload={}",
                event.eventType(), event.correlationId(), event.payload());
    }
}
