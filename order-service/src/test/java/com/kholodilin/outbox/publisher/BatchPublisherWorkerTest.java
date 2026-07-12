package com.kholodilin.outbox.publisher;

import tools.jackson.databind.json.JsonMapper;
import com.kholodilin.outbox.config.AppProperties;
import com.kholodilin.outbox.events.OutboxStatus;
import com.kholodilin.outbox.metrics.OutboxMetrics;
import com.kholodilin.outbox.persistence.OutboxJdbcRepository;
import com.kholodilin.outbox.queue.InMemoryEventQueue;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.test.util.ReflectionTestUtils;

import java.util.List;

import static org.mockito.Mockito.verify;

@ExtendWith(MockitoExtension.class)
class BatchPublisherWorkerTest {

    @Mock
    private InMemoryEventQueue eventQueue;

    @Mock
    private OutboxJdbcRepository outboxJdbcRepository;

    @Mock
    private KafkaBatchPublisher kafkaBatchPublisher;

    private BatchPublisherWorker worker;

    @BeforeEach
    void setUp() {
        AppProperties properties = new AppProperties();
        properties.getOutbox().getPublisher().setMaxRetries(5);
        worker = new BatchPublisherWorker(
                eventQueue,
                outboxJdbcRepository,
                kafkaBatchPublisher,
                new OutboxMetrics(new SimpleMeterRegistry()),
                properties,
                JsonMapper.builder().build()
        );
    }

    @Test
    void marksFailedBeforeMaxRetries() {
        OutboxJdbcRepository.OutboxRow row = sampleRow(2);

        ReflectionTestUtils.invokeMethod(worker, "handleFailures", List.of(row));

        verify(outboxJdbcRepository).markFailed(1L, 3, OutboxStatus.FAILED);
    }

    @Test
    void marksDeadAfterMaxRetries() {
        OutboxJdbcRepository.OutboxRow row = sampleRow(4);

        ReflectionTestUtils.invokeMethod(worker, "handleFailures", List.of(row));

        verify(outboxJdbcRepository).markFailed(1L, 5, OutboxStatus.DEAD);
    }

    private OutboxJdbcRepository.OutboxRow sampleRow(int retryCount) {
        return new OutboxJdbcRepository.OutboxRow(
                1L,
                10L,
                20L,
                "OrderCreated",
                "{\"orderId\":10}",
                OutboxStatus.PROCESSING,
                retryCount
        );
    }
}
