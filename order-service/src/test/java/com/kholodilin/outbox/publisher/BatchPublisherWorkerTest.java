package com.kholodilin.outbox.publisher;

import tools.jackson.databind.json.JsonMapper;
import com.kholodilin.outbox.config.AppProperties;
import com.kholodilin.outbox.config.MemoryQueueProperties;
import com.kholodilin.outbox.config.OutboxProperties;
import com.kholodilin.outbox.config.PublisherProperties;
import com.kholodilin.outbox.events.EventEnvelope;
import com.kholodilin.outbox.events.OutboxStatus;
import com.kholodilin.outbox.metrics.OutboxMetrics;
import com.kholodilin.outbox.persistence.OutboxJdbcRepository;
import com.kholodilin.outbox.persistence.OutboxRow;
import com.kholodilin.outbox.queue.InMemoryEventQueue;
import com.kholodilin.outbox.tracing.OutboxTracing;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.beans.factory.ObjectProvider;
import org.springframework.test.util.ReflectionTestUtils;

import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyList;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.lenient;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class BatchPublisherWorkerTest {

    @Mock
    private InMemoryEventQueue eventQueue;

    @Mock
    private OutboxJdbcRepository outboxJdbcRepository;

    @Mock
    private KafkaBatchPublisher kafkaBatchPublisher;

    @Mock
    private ObjectProvider<KafkaBatchPublisher> kafkaBatchPublisherProvider;

    @Mock
    private OutboxTracing outboxTracing;

    private BatchPublisherWorker worker;

    @BeforeEach
    void setUp() {
        lenient().when(kafkaBatchPublisherProvider.getObject()).thenReturn(kafkaBatchPublisher);
        lenient().when(outboxTracing.observe(anyString(), any(java.util.function.Supplier.class)))
                .thenAnswer(invocation -> invocation.getArgument(1, java.util.function.Supplier.class).get());
        lenient().doAnswer(invocation -> {
            invocation.getArgument(2, Runnable.class).run();
            return null;
        }).when(outboxTracing).observeWithTraceParent(any(), anyString(), any(Runnable.class));
        OutboxMetrics metrics = new OutboxMetrics(new SimpleMeterRegistry());
        ReflectionTestUtils.invokeMethod(metrics, "registerMeters");
        AppProperties properties = AppProperties.builder()
                .instanceId("pod-1")
                .outbox(OutboxProperties.builder()
                        .publisher(PublisherProperties.builder()
                                .maxRetries(5)
                                .leaseDuration(Duration.ofSeconds(30))
                                .build())
                        .memoryQueue(MemoryQueueProperties.builder()
                                .batchSize(10)
                                .batchWait(Duration.ofMillis(10))
                                .build())
                        .build())
                .build();
        worker = new BatchPublisherWorker(
                eventQueue,
                outboxJdbcRepository,
                kafkaBatchPublisherProvider,
                metrics,
                outboxTracing,
                properties,
                JsonMapper.builder().build()
        );
    }

    @Test
    void marksFailedBeforeMaxRetries() {
        OutboxRow row = sampleRow(2);

        ReflectionTestUtils.invokeMethod(worker, "handleFailures", List.of(row));

        verify(outboxJdbcRepository).markFailed(1L, 3, OutboxStatus.FAILED);
    }

    @Test
    void marksDeadAfterMaxRetries() {
        OutboxRow row = sampleRow(4);

        ReflectionTestUtils.invokeMethod(worker, "handleFailures", List.of(row));

        verify(outboxJdbcRepository).markFailed(1L, 5, OutboxStatus.DEAD);
    }

    @Test
    void extractCorrelationIdFromPayload() {
        String correlationId = ReflectionTestUtils.invokeMethod(
                worker,
                "extractCorrelationId",
                "{\"correlationId\":\"corr-1\",\"orderId\":10}"
        );

        assertThat(correlationId).isEqualTo("corr-1");
    }

    @Test
    void extractCorrelationIdReturnsNullForInvalidJson() {
        String correlationId = ReflectionTestUtils.invokeMethod(worker, "extractCorrelationId", "not-json");

        assertThat(correlationId).isNull();
    }

    @Test
    void sentIdsMapsClaimedRows() {
        @SuppressWarnings("unchecked")
        List<Long> ids = ReflectionTestUtils.invokeMethod(worker, "sentIds", List.of(sampleRow(0)));

        assertThat(ids).containsExactly(1L);
    }

    @Test
    void loopPublishesClaimedBatchAndAcknowledges() throws Exception {
        OutboxRow row = sampleRow(0);
        EventEnvelope envelope = new EventEnvelope(
                1L, 10L, 20L, "OrderCreated", Map.of("orderId", 10), "corr", Instant.now(), "00-trace", null);
        AtomicInteger polls = new AtomicInteger();
        when(eventQueue.poll(anyLong())).thenAnswer(invocation -> {
            if (polls.getAndIncrement() == 0) {
                return 1L;
            }
            ((AtomicBoolean) ReflectionTestUtils.getField(worker, "running")).set(false);
            return null;
        });
        when(eventQueue.drainBatch(9)).thenReturn(List.of());
        when(outboxJdbcRepository.claimByIds(eq(List.of(1L)), eq("pod-1"), any())).thenReturn(List.of(row));
        when(outboxJdbcRepository.toEnvelope(eq(row), any())).thenReturn(envelope);

        ReflectionTestUtils.invokeMethod(worker, "loop");

        verify(kafkaBatchPublisher).publish(List.of(envelope));
        verify(outboxJdbcRepository).markSent(eq(List.of(1L)), any(Instant.class));
        verify(eventQueue).acknowledge(List.of(1L));
    }

    @Test
    void loopReenqueuesWhenClaimReturnsEmpty() throws Exception {
        AtomicInteger polls = new AtomicInteger();
        when(eventQueue.poll(anyLong())).thenAnswer(invocation -> {
            if (polls.getAndIncrement() == 0) {
                return 7L;
            }
            ((AtomicBoolean) ReflectionTestUtils.getField(worker, "running")).set(false);
            return null;
        });
        when(eventQueue.drainBatch(9)).thenReturn(List.of());
        when(outboxJdbcRepository.claimByIds(eq(List.of(7L)), eq("pod-1"), any())).thenReturn(List.of());
        when(outboxJdbcRepository.findReenqueueableIds(List.of(7L))).thenReturn(List.of(7L));

        ReflectionTestUtils.invokeMethod(worker, "loop");

        verify(eventQueue).enqueue(7L);
        verify(eventQueue).acknowledge(List.of(7L));
    }

    @Test
    void loopMarksFailedWhenPublishThrows() throws Exception {
        OutboxRow row = sampleRow(1);
        EventEnvelope envelope = new EventEnvelope(
                1L, 10L, 20L, "OrderCreated", Map.of(), null, Instant.now(), null, null);
        AtomicInteger polls = new AtomicInteger();
        when(eventQueue.poll(anyLong())).thenAnswer(invocation -> {
            if (polls.getAndIncrement() == 0) {
                return 1L;
            }
            ((AtomicBoolean) ReflectionTestUtils.getField(worker, "running")).set(false);
            return null;
        });
        when(eventQueue.drainBatch(9)).thenReturn(List.of());
        when(outboxJdbcRepository.claimByIds(eq(List.of(1L)), eq("pod-1"), any())).thenReturn(List.of(row));
        when(outboxJdbcRepository.toEnvelope(eq(row), any())).thenReturn(envelope);
        doThrow(new RuntimeException("kafka down")).when(kafkaBatchPublisher).publish(anyList());

        ReflectionTestUtils.invokeMethod(worker, "loop");

        verify(outboxJdbcRepository).markFailed(1L, 2, OutboxStatus.FAILED);
        verify(eventQueue).acknowledge(List.of(1L));
    }

    @Test
    void loopExitsOnInterruptedException() throws Exception {
        when(eventQueue.poll(anyLong())).thenThrow(new InterruptedException("stop"));

        ReflectionTestUtils.invokeMethod(worker, "loop");

        assertThat(Thread.currentThread().isInterrupted()).isTrue();
        // Clear interrupt flag so it does not leak into the test runner.
        Thread.interrupted();
    }

    @Test
    void startAndStopLifecycle() {
        worker.start();
        worker.stop();

        AtomicBoolean running = (AtomicBoolean) ReflectionTestUtils.getField(worker, "running");
        assertThat(running).isNotNull();
        assertThat(running.get()).isFalse();
    }

    private OutboxRow sampleRow(int retryCount) {
        return new OutboxRow(
                1L,
                10L,
                20L,
                "OrderCreated",
                "{\"orderId\":10,\"correlationId\":\"corr-1\"}",
                OutboxStatus.PROCESSING,
                retryCount,
                "00-trace"
        );
    }
}
