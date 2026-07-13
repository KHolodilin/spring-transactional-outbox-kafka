package com.kholodilin.outbox.recovery;

import com.kholodilin.outbox.config.AppProperties;
import com.kholodilin.outbox.config.OutboxProperties;
import com.kholodilin.outbox.config.PublisherProperties;
import com.kholodilin.outbox.config.RecoveryProperties;
import com.kholodilin.outbox.metrics.OutboxMetrics;
import com.kholodilin.outbox.persistence.OutboxJdbcRepository;
import com.kholodilin.outbox.queue.InMemoryEventQueue;
import com.kholodilin.outbox.tracing.OutboxTracing;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.test.util.ReflectionTestUtils;

import java.time.Duration;
import java.time.Instant;
import java.util.List;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyList;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class RecoveryWorkerTest {

    @Mock
    private OutboxJdbcRepository outboxJdbcRepository;

    @Mock
    private InMemoryEventQueue eventQueue;

    @Mock
    private OutboxTracing outboxTracing;

    private OutboxMetrics metrics;
    private RecoveryWorker worker;
    private AppProperties properties;

    @BeforeEach
    void setUp() {
        metrics = new OutboxMetrics(new SimpleMeterRegistry());
        ReflectionTestUtils.invokeMethod(metrics, "registerMeters");
        properties = AppProperties.builder()
                .instanceId("pod-1")
                .outbox(OutboxProperties.builder()
                        .recovery(RecoveryProperties.builder().enabled(true).batchSize(10).build())
                        .publisher(PublisherProperties.builder().leaseDuration(Duration.ofSeconds(30)).build())
                        .build())
                .build();
        worker = new RecoveryWorker(properties, outboxJdbcRepository, eventQueue, metrics, outboxTracing);
    }

    @Test
    void skipsWhenRecoveryDisabled() {
        properties.getOutbox().getRecovery().setEnabled(false);
        worker.recover();
        verify(outboxJdbcRepository, never()).claimRecoverableIds(anyInt(), anyString(), any(Instant.class));
    }

    @Test
    void skipsWhenNoIdsClaimed() {
        when(outboxJdbcRepository.claimRecoverableIds(anyInt(), anyString(), any(Instant.class))).thenReturn(List.of());
        worker.recover();
        verify(outboxJdbcRepository, never()).clearLease(anyList());
    }

    @Test
    void enqueuesUntrackedIdsAndIncrementsMetrics() {
        doAnswer(invocation -> {
            Runnable runnable = invocation.getArgument(1);
            runnable.run();
            return null;
        }).when(outboxTracing).observe(anyString(), any(Runnable.class));
        when(outboxJdbcRepository.claimRecoverableIds(anyInt(), anyString(), any(Instant.class))).thenReturn(List.of(1L, 2L));
        when(eventQueue.isTracked(1L)).thenReturn(true);
        when(eventQueue.isTracked(2L)).thenReturn(false);
        when(eventQueue.enqueue(2L)).thenReturn(true);

        worker.recover();

        verify(outboxJdbcRepository).clearLease(List.of(1L, 2L));
        verify(eventQueue).enqueue(2L);
    }
}
