package com.kholodilin.outbox.notification;

import com.kholodilin.idempotency.ExecutionResult;
import com.kholodilin.idempotency.reactive.ReactiveIdempotencyService;
import com.kholodilin.outbox.events.EventEnvelope;
import com.kholodilin.outbox.events.ObservabilityVocabulary;
import com.kholodilin.outbox.logging.StructuredLogContext;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.boot.sql.init.dependency.DependsOnDatabaseInitialization;
import org.springframework.stereotype.Service;
import org.springframework.transaction.reactive.TransactionalOperator;
import reactor.core.publisher.Mono;

import java.util.concurrent.atomic.AtomicBoolean;

@Slf4j
@Service
@RequiredArgsConstructor
@DependsOnDatabaseInitialization
public class NotificationTransactionService {

    private static final String OPERATION = "NOTIFY_ORDER_CREATED";

    private final ReactiveIdempotencyService idempotencyService;
    private final TransactionalOperator transactionalOperator;

    public Mono<Boolean> process(EventEnvelope event) {

        AtomicBoolean executed = new AtomicBoolean(false);

        return transactionalOperator.transactional(
                        idempotencyService
                                .operation(OPERATION)
                                .key(String.valueOf(event.eventId()))
                                .request(event)
                                .execute(Void.class, () -> {
                                    executed.set(true);

                                    logNotification(event);

                                    return Mono.just(
                                            ExecutionResult.success(null)
                                    );
                                })
                )
                .map(result -> {
                    result.valueOrThrow();
                    return executed.get();
                });
    }

    private void logNotification(EventEnvelope event) {

        StructuredLogContext.putNotificationFields(
                "log",
                "sent"
        );

        StructuredLogContext.putEventAction(
                ObservabilityVocabulary.NOTIFICATION_PROCESSED
        );

        log.info(
                "Notification stub sent orderId={} customerId={} eventId={}",
                event.orderId(),
                event.customerId(),
                event.eventId()
        );

        log.debug(
                "Notification stub event details eventType={} correlationId={} payload={}",
                event.eventType(),
                event.correlationId(),
                event.payload()
        );
    }
}