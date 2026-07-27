package com.kholodilin.outbox.recovery;

import com.kholodilin.outbox.config.AppProperties;
import com.kholodilin.outbox.config.OutboxProperties;
import com.kholodilin.outbox.config.PublisherProperties;
import com.kholodilin.outbox.config.RecoveryProperties;
import com.kholodilin.outbox.metrics.OutboxMetrics;
import com.kholodilin.outbox.persistence.OutboxR2dbcRepository;
import com.kholodilin.outbox.queue.InMemoryEventQueue;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.test.util.ReflectionTestUtils;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.test.StepVerifier;

import java.time.Duration;
import java.time.Instant;
import java.util.List;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyList;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class RecoveryWorkerTest {

    @Mock
    private OutboxR2dbcRepository outboxR2dbcRepository;

    @Mock
    private InMemoryEventQueue eventQueue;

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
        worker = new RecoveryWorker(properties, outboxR2dbcRepository, eventQueue, metrics);
    }

    @Test
    void skipsWhenRecoveryDisabled() {
        properties.getOutbox().getRecovery().setEnabled(false);

        StepVerifier.create(worker.recover()).verifyComplete();

        verify(outboxR2dbcRepository, never()).claimRecoverableIds(anyInt(), anyString(), any(Instant.class));
    }

    @Test
    void skipsWhenNoIdsClaimed() {
        when(outboxR2dbcRepository.claimRecoverableIds(anyInt(), anyString(), any(Instant.class)))
                .thenReturn(Flux.empty());

        StepVerifier.create(worker.recover()).verifyComplete();

        verify(outboxR2dbcRepository, never()).clearLease(anyList());
    }

    @Test
    void enqueuesUntrackedIdsAndClearsLease() {
        when(outboxR2dbcRepository.claimRecoverableIds(anyInt(), anyString(), any(Instant.class)))
                .thenReturn(Flux.just(1L, 2L));
        when(outboxR2dbcRepository.clearLease(List.of(1L, 2L))).thenReturn(Mono.empty());
        when(eventQueue.enqueue(1L)).thenReturn(false);
        when(eventQueue.enqueue(2L)).thenReturn(true);

        StepVerifier.create(worker.recover()).verifyComplete();

        verify(outboxR2dbcRepository).clearLease(List.of(1L, 2L));
        verify(eventQueue).enqueue(1L);
        verify(eventQueue).enqueue(2L);
    }
}
