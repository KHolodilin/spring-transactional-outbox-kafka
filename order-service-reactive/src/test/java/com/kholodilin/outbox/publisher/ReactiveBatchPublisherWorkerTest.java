package com.kholodilin.outbox.publisher;

import tools.jackson.databind.json.JsonMapper;
import com.kholodilin.outbox.config.AppProperties;
import com.kholodilin.outbox.config.MemoryQueueProperties;
import com.kholodilin.outbox.config.OutboxProperties;
import com.kholodilin.outbox.config.PublisherProperties;
import com.kholodilin.outbox.events.EventEnvelope;
import com.kholodilin.outbox.events.OutboxStatus;
import com.kholodilin.outbox.metrics.OutboxMetrics;
import com.kholodilin.outbox.persistence.OutboxR2dbcRepository;
import com.kholodilin.outbox.persistence.OutboxRow;
import com.kholodilin.outbox.queue.InMemoryEventQueue;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.beans.factory.ObjectProvider;
import org.springframework.test.util.ReflectionTestUtils;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.test.StepVerifier;

import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyList;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.lenient;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class ReactiveBatchPublisherWorkerTest {

    @Mock
    private InMemoryEventQueue eventQueue;

    @Mock
    private OutboxR2dbcRepository outboxR2dbcRepository;

    @Mock
    private KafkaBatchPublisher kafkaBatchPublisher;

    @Mock
    private ObjectProvider<KafkaBatchPublisher> kafkaBatchPublisherProvider;

    private ReactiveBatchPublisherWorker worker;

    @BeforeEach
    void setUp() {
        lenient().when(kafkaBatchPublisherProvider.getObject()).thenReturn(kafkaBatchPublisher);
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
        worker = new ReactiveBatchPublisherWorker(
                eventQueue,
                outboxR2dbcRepository,
                kafkaBatchPublisherProvider,
                metrics,
                properties,
                JsonMapper.builder().build()
        );
    }

    @Test
    void marksFailedBeforeMaxRetries() {
        OutboxRow row = sampleRow(2);
        when(outboxR2dbcRepository.markFailed(1L, 3, OutboxStatus.FAILED)).thenReturn(Mono.empty());

        StepVerifier.create(ReflectionTestUtils.<Mono<Void>>invokeMethod(worker, "handleFailures", List.of(row)))
                .verifyComplete();

        verify(outboxR2dbcRepository).markFailed(1L, 3, OutboxStatus.FAILED);
    }

    @Test
    void marksDeadAfterMaxRetries() {
        OutboxRow row = sampleRow(4);
        when(outboxR2dbcRepository.markFailed(1L, 5, OutboxStatus.DEAD)).thenReturn(Mono.empty());

        StepVerifier.create(ReflectionTestUtils.<Mono<Void>>invokeMethod(worker, "handleFailures", List.of(row)))
                .verifyComplete();

        verify(outboxR2dbcRepository).markFailed(1L, 5, OutboxStatus.DEAD);
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
    void processOneBatchPublishesClaimedAndAcknowledges() {
        OutboxRow row = sampleRow(0);
        EventEnvelope envelope = new EventEnvelope(
                1L, 10L, 20L, "OrderCreated", Map.of("orderId", 10), "corr", Instant.now(), "00-trace");
        when(eventQueue.pollMono(any(Duration.class))).thenReturn(Mono.just(1L));
        when(eventQueue.drainBatch(9)).thenReturn(List.of());
        when(outboxR2dbcRepository.claimByIds(eq(List.of(1L)), eq("pod-1"), any(Instant.class)))
                .thenReturn(Flux.just(row));
        when(outboxR2dbcRepository.toEnvelope(eq(row), any())).thenReturn(envelope);
        when(kafkaBatchPublisher.publish(List.of(envelope))).thenReturn(Mono.empty());
        when(outboxR2dbcRepository.markSent(eq(List.of(1L)), any(Instant.class))).thenReturn(Mono.empty());

        StepVerifier.create(ReflectionTestUtils.<Mono<Void>>invokeMethod(worker, "processOneBatch"))
                .verifyComplete();

        verify(kafkaBatchPublisher).publish(List.of(envelope));
        verify(outboxR2dbcRepository).markSent(eq(List.of(1L)), any(Instant.class));
        verify(eventQueue).acknowledge(List.of(1L));
    }

    @Test
    void processOneBatchReenqueuesWhenClaimEmpty() {
        when(eventQueue.pollMono(any(Duration.class))).thenReturn(Mono.just(7L));
        when(eventQueue.drainBatch(9)).thenReturn(List.of());
        when(outboxR2dbcRepository.claimByIds(eq(List.of(7L)), eq("pod-1"), any(Instant.class)))
                .thenReturn(Flux.empty());
        when(outboxR2dbcRepository.findReenqueueableIds(List.of(7L))).thenReturn(Flux.just(7L));
        when(eventQueue.enqueue(7L)).thenReturn(true);

        StepVerifier.create(ReflectionTestUtils.<Mono<Void>>invokeMethod(worker, "processOneBatch"))
                .verifyComplete();

        verify(eventQueue).enqueue(7L);
        verify(eventQueue).acknowledge(List.of(7L));
    }

    @Test
    void processOneBatchMarksFailedWhenPublishFails() {
        OutboxRow row = sampleRow(1);
        EventEnvelope envelope = new EventEnvelope(
                1L, 10L, 20L, "OrderCreated", Map.of(), null, Instant.now(), null);
        when(eventQueue.pollMono(any(Duration.class))).thenReturn(Mono.just(1L));
        when(eventQueue.drainBatch(9)).thenReturn(List.of());
        when(outboxR2dbcRepository.claimByIds(eq(List.of(1L)), eq("pod-1"), any(Instant.class)))
                .thenReturn(Flux.just(row));
        when(outboxR2dbcRepository.toEnvelope(eq(row), any())).thenReturn(envelope);
        when(kafkaBatchPublisher.publish(anyList())).thenReturn(Mono.error(new RuntimeException("kafka down")));
        when(outboxR2dbcRepository.markFailed(1L, 2, OutboxStatus.FAILED)).thenReturn(Mono.empty());

        StepVerifier.create(ReflectionTestUtils.<Mono<Void>>invokeMethod(worker, "processOneBatch"))
                .verifyComplete();

        verify(outboxR2dbcRepository).markFailed(1L, 2, OutboxStatus.FAILED);
        verify(eventQueue).acknowledge(List.of(1L));
    }

    @Test
    void startAndStopLifecycle() {
        when(eventQueue.pollMono(any(Duration.class))).thenReturn(Mono.empty());
        AtomicBoolean running = (AtomicBoolean) ReflectionTestUtils.getField(worker, "running");
        assertThat(running).isNotNull();
        // Prevent infinite empty-batch repeat while covering start()/stop().
        running.set(false);

        worker.start();
        worker.stop();

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
