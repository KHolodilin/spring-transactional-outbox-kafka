package com.kholodilin.outbox.notification;

import com.kholodilin.idempotency.ExecutionResult;
import com.kholodilin.idempotency.exception.IdempotencyConflictException;
import com.kholodilin.idempotency.model.IdempotencyKey;
import com.kholodilin.idempotency.reactive.ReactiveIdempotencyCall;
import com.kholodilin.idempotency.reactive.ReactiveIdempotencyService;
import com.kholodilin.outbox.events.EventEnvelope;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.transaction.reactive.TransactionalOperator;
import reactor.core.publisher.Mono;
import reactor.test.StepVerifier;

import java.util.Map;
import java.util.function.Supplier;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class NotificationTransactionServiceTest {

    @Mock
    private ReactiveIdempotencyService idempotencyService;

    @Mock
    private ReactiveIdempotencyCall idempotencyCall;

    @Mock
    private TransactionalOperator transactionalOperator;

    private NotificationTransactionService service;

    @BeforeEach
    void setUp() {
        when(transactionalOperator.transactional(any(Mono.class)))
                .thenAnswer(invocation -> invocation.getArgument(0));

        service = new NotificationTransactionService(
                idempotencyService,
                transactionalOperator
        );
    }

    @Test
    void firstEventExecutesNotificationAction() {
        EventEnvelope event = event(42L, Map.of("version", 1));

        stubCall(event);

        when(idempotencyCall.execute(eq(Void.class), any()))
                .thenAnswer(invocation -> {
                    Supplier<Mono<ExecutionResult<Void>>> action =
                            invocation.getArgument(1);

                    return action.get();
                });

        StepVerifier.create(service.process(event))
                .expectNext(true)
                .verifyComplete();

        verify(idempotencyService)
                .operation("NOTIFY_ORDER_CREATED");

        verify(idempotencyCall)
                .key("42");

        verify(idempotencyCall)
                .request(event);
    }

    @Test
    void replaySkipsNotificationAction() {
        EventEnvelope event = event(42L, Map.of("version", 1));

        stubCall(event);

        when(idempotencyCall.execute(eq(Void.class), any()))
                .thenReturn(
                        Mono.just(
                                ExecutionResult.success(null)
                        )
                );

        StepVerifier.create(service.process(event))
                .expectNext(false)
                .verifyComplete();
    }

    @Test
    void conflictIsPropagatedToHandler() {
        EventEnvelope event = event(42L, Map.of("version", 2));

        stubCall(event);

        IdempotencyConflictException conflict =
                new IdempotencyConflictException(
                        new IdempotencyKey(
                                "NOTIFY_ORDER_CREATED",
                                "42"
                        ),
                        "hash-a",
                        "hash-b"
                );

        when(idempotencyCall.execute(eq(Void.class), any()))
                .thenReturn(Mono.error(conflict));

        StepVerifier.create(service.process(event))
                .expectError(IdempotencyConflictException.class)
                .verify();
    }

    private void stubCall(EventEnvelope event) {
        when(idempotencyService.operation("NOTIFY_ORDER_CREATED"))
                .thenReturn(idempotencyCall);

        when(idempotencyCall.key(
                String.valueOf(event.eventId())
        )).thenReturn(idempotencyCall);

        when(idempotencyCall.request(event))
                .thenReturn(idempotencyCall);
    }

    private EventEnvelope event(
            long eventId,
            Map<String, Object> payload
    ) {
        return new EventEnvelope(
                eventId,
                2L,
                3L,
                "OrderCreated",
                payload,
                "corr-1",
                null,
                null
        );
    }
}