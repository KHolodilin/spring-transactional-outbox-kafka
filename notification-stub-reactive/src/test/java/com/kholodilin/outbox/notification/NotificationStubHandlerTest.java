package com.kholodilin.outbox.notification;

import com.kholodilin.idempotency.exception.IdempotencyConflictException;
import com.kholodilin.idempotency.model.IdempotencyKey;
import com.kholodilin.outbox.events.EventEnvelope;
import com.kholodilin.outbox.logging.InstanceMdcInitializer;
import com.kholodilin.outbox.metrics.NotificationStubMetrics;
import com.kholodilin.outbox.tracing.TraceContextSupport;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.test.util.ReflectionTestUtils;
import reactor.core.publisher.Mono;
import reactor.test.StepVerifier;

import java.util.List;
import java.util.Map;
import java.util.function.Supplier;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class NotificationStubHandlerTest {

    @Mock
    private TraceContextSupport traceContextSupport;

    @Mock
    private InstanceMdcInitializer instanceMdcInitializer;

    @Mock
    private NotificationTransactionService transactionService;

    private NotificationStubHandler handler;

    @BeforeEach
    void setUp() {
        NotificationStubMetrics metrics =
                new NotificationStubMetrics(
                        new SimpleMeterRegistry()
                );

        ReflectionTestUtils.invokeMethod(
                metrics,
                "registerMeters"
        );

        handler = new NotificationStubHandler(
                metrics,
                traceContextSupport,
                instanceMdcInitializer,
                transactionService
        );
    }

    @Test
    void ignoresEmptyBatch() {
        StepVerifier.create(
                        handler.handleBatch(List.of())
                )
                .verifyComplete();

        verifyNoInteractions(transactionService);
    }

    @Test
    void processesSuccessfulNotification() {
        stubTraceContext();

        EventEnvelope event =
                event(1L, Map.of("version", 1));

        when(transactionService.process(event))
                .thenReturn(Mono.just(true));

        StepVerifier.create(
                        handler.handleBatch(
                                List.of(record(event, 1L))
                        )
                )
                .verifyComplete();

        verify(transactionService)
                .process(event);
    }

    @Test
    void skipsDuplicateNotification() {
        stubTraceContext();

        EventEnvelope event =
                event(2L, Map.of("version", 1));

        when(transactionService.process(event))
                .thenReturn(Mono.just(false));

        StepVerifier.create(
                        handler.handleBatch(
                                List.of(record(event, 2L))
                        )
                )
                .verifyComplete();

        verify(transactionService)
                .process(event);
    }

    @Test
    void conflictDoesNotFailBatchAndNextRecordContinues() {
        stubTraceContext();

        EventEnvelope conflicting =
                event(3L, Map.of("version", 2));

        EventEnvelope next =
                event(4L, Map.of("version", 1));

        IdempotencyConflictException conflict =
                new IdempotencyConflictException(
                        new IdempotencyKey(
                                "NOTIFY_ORDER_CREATED",
                                "3"
                        ),
                        "hash-a",
                        "hash-b"
                );

        when(transactionService.process(conflicting))
                .thenReturn(Mono.error(conflict));

        when(transactionService.process(next))
                .thenReturn(Mono.just(true));

        StepVerifier.create(
                        handler.handleBatch(
                                List.of(
                                        record(conflicting, 3L),
                                        record(next, 4L)
                                )
                        )
                )
                .verifyComplete();

        verify(transactionService)
                .process(conflicting);

        verify(transactionService)
                .process(next);
    }

    @Test
    void technicalFailureFailsBatch() {
        stubTraceContext();

        EventEnvelope event =
                event(5L, Map.of("version", 1));

        IllegalStateException failure =
                new IllegalStateException(
                        "database unavailable"
                );

        when(transactionService.process(event))
                .thenReturn(Mono.error(failure));

        StepVerifier.create(
                        handler.handleBatch(
                                List.of(record(event, 5L))
                        )
                )
                .expectErrorMatches(
                        error -> error == failure
                )
                .verify();
    }

    @SuppressWarnings("unchecked")
    private void stubTraceContext() {
        doAnswer(invocation -> {
            Supplier<Mono<?>> action =
                    invocation.getArgument(2);

            return action.get();
        }).when(traceContextSupport)
                .withRecordTrace(
                        any(ConsumerRecord.class),
                        anyString(),
                        any(Supplier.class)
                );
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
                "corr",
                null,
                null
        );
    }

    private ConsumerRecord<String, EventEnvelope> record(
            EventEnvelope event,
            long offset
    ) {
        return new ConsumerRecord<>(
                "orders.events",
                0,
                offset,
                String.valueOf(event.customerId()),
                event
        );
    }
}