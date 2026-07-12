package com.kholodilin.outbox.publisher;

import tools.jackson.databind.json.JsonMapper;
import com.kholodilin.outbox.config.AppProperties;
import com.kholodilin.outbox.config.OutboxProperties;
import com.kholodilin.outbox.config.PublisherProperties;
import com.kholodilin.outbox.events.OutboxStatus;
import com.kholodilin.outbox.metrics.OutboxMetrics;
import com.kholodilin.outbox.persistence.OutboxRow;
import com.kholodilin.outbox.persistence.OutboxJdbcRepository;
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

import java.util.List;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.lenient;
import static org.mockito.Mockito.verify;

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
                .outbox(OutboxProperties.builder()
                        .publisher(PublisherProperties.builder().maxRetries(5).build())
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

    private OutboxRow sampleRow(int retryCount) {
        return OutboxRow.builder()
                .id(1L)
                .orderId(10L)
                .customerId(20L)
                .eventType("OrderCreated")
                .payload("{\"orderId\":10}")
                .status(OutboxStatus.PROCESSING)
                .retryCount(retryCount)
                .build();
    }
}
