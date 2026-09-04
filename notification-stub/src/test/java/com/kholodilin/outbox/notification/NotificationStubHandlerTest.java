package com.kholodilin.outbox.notification;

import com.kholodilin.idempotency.exception.IdempotencyConflictException;
import com.kholodilin.idempotency.model.IdempotencyKey;
import com.kholodilin.outbox.events.EventConstants;
import com.kholodilin.outbox.events.EventEnvelope;
import com.kholodilin.outbox.logging.InstanceMdcInitializer;
import com.kholodilin.outbox.metrics.NotificationStubMetrics;
import com.kholodilin.outbox.tracing.TraceContextSupport;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.header.internals.RecordHeader;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.test.util.ReflectionTestUtils;

import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

@ExtendWith(MockitoExtension.class)
class NotificationStubHandlerTest {

    @Mock
    private TraceContextSupport traceContextSupport;

    @Mock
    private InstanceMdcInitializer instanceMdcInitializer;

    @Mock
    private NotificationTransactionService notificationTransactionService;

    @Test
    void ignoresEmptyBatch() {
        NotificationStubHandler handler = newHandler();

        handler.handleBatch(List.of());

        verify(instanceMdcInitializer, never()).enrich();
    }

    @Test
    void processesBatchAndRecordsMetrics() {
        stubTraceContext();
        NotificationStubHandler handler = newHandler();
        EventEnvelope envelope = new EventEnvelope(
                1L,
                2L,
                3L,
                "OrderCreated",
                Map.of("orderId", 2),
                "corr",
                null,
                null
        );
        ConsumerRecord<String, EventEnvelope> record = new ConsumerRecord<>("orders.events", 0, 5L, "3", envelope);
        record.headers().add(new RecordHeader(
                EventConstants.HEADER_TRACEPARENT,
                "00-trace".getBytes(StandardCharsets.UTF_8)
        ));

        handler.handleBatch(List.of(record));

        verify(notificationTransactionService).process(envelope);
        verify(instanceMdcInitializer, times(2)).enrich();
        verify(instanceMdcInitializer, times(2)).clearConsumerContext();
    }

    @Test
    void processesRecordWithoutTraceParentHeader() {
        stubTraceContext();
        NotificationStubHandler handler = newHandler();
        EventEnvelope envelope = new EventEnvelope(
                5L,
                6L,
                7L,
                "OrderCreated",
                Map.of("orderId", 6),
                null,
                null,
                null
        );
        ConsumerRecord<String, EventEnvelope> record = new ConsumerRecord<>("orders.events", 1, 9L, "7", envelope);

        handler.handleBatch(List.of(record));

        verify(instanceMdcInitializer, times(2)).enrich();
    }

    @Test
    void continuesBatchAfterIdempotencyConflict() {
        stubTraceContext();
        NotificationStubHandler handler = newHandler();
        EventEnvelope conflicting = envelope(10L, Map.of("version", 2));
        EventEnvelope next = envelope(11L, Map.of("version", 1));
        doThrow(new IdempotencyConflictException(
                new IdempotencyKey("NOTIFY_ORDER_CREATED", "10"),
                "hash-a",
                "hash-b"
        )).when(notificationTransactionService).process(conflicting);

        handler.handleBatch(List.of(record(conflicting, 10L), record(next, 11L)));

        verify(notificationTransactionService).process(conflicting);
        verify(notificationTransactionService).process(next);
        verify(instanceMdcInitializer, times(3)).clearConsumerContext();
    }

    @Test
    void propagatesTechnicalFailureSoKafkaCanRetryBatch() {
        stubTraceContext();
        NotificationStubHandler handler = newHandler();
        EventEnvelope event = envelope(12L, Map.of("version", 1));
        IllegalStateException failure = new IllegalStateException("database unavailable");
        doThrow(failure).when(notificationTransactionService).process(event);

        assertThatThrownBy(() -> handler.handleBatch(List.of(record(event, 12L))))
                .isSameAs(failure);

        verify(instanceMdcInitializer).clearConsumerContext();
    }

    private NotificationStubHandler newHandler() {
        NotificationStubMetrics metrics = new NotificationStubMetrics(new SimpleMeterRegistry());
        ReflectionTestUtils.invokeMethod(metrics, "registerMeters");
        return new NotificationStubHandler(
                metrics,
                traceContextSupport,
                instanceMdcInitializer,
                notificationTransactionService
        );
    }

    private void stubTraceContext() {
        doAnswer(invocation -> {
            Runnable action = invocation.getArgument(2);
            action.run();
            return null;
        }).when(traceContextSupport).runWithTraceParent(any(), anyString(), any(Runnable.class));
    }

    private EventEnvelope envelope(long eventId, Map<String, Object> payload) {
        return new EventEnvelope(eventId, 2L, 3L, "OrderCreated", payload, "corr", null, null);
    }

    private ConsumerRecord<String, EventEnvelope> record(EventEnvelope event, long offset) {
        return new ConsumerRecord<>("orders.events", 0, offset, String.valueOf(event.customerId()), event);
    }
}
