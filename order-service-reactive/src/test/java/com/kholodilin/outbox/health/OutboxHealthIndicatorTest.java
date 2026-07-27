package com.kholodilin.outbox.health;

import com.kholodilin.outbox.config.AppProperties;
import com.kholodilin.outbox.config.HealthProperties;
import com.kholodilin.outbox.persistence.OutboxR2dbcRepository;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.boot.health.contributor.Status;
import reactor.core.publisher.Mono;
import reactor.test.StepVerifier;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class OutboxHealthIndicatorTest {

    @Mock
    private OutboxR2dbcRepository outboxR2dbcRepository;

    @Test
    void reportsUpWhenPendingBelowCritical() {
        when(outboxR2dbcRepository.countActivePending()).thenReturn(Mono.just(5L));
        OutboxHealthIndicator indicator = new OutboxHealthIndicator(outboxR2dbcRepository, properties(100L));

        StepVerifier.create(indicator.health())
                .assertNext(health -> {
                    assertThat(health.getStatus()).isEqualTo(Status.UP);
                    assertThat(health.getDetails()).containsEntry("activePending", 5L);
                })
                .verifyComplete();
    }

    @Test
    void reportsDownWhenPendingAtCriticalThreshold() {
        when(outboxR2dbcRepository.countActivePending()).thenReturn(Mono.just(100L));
        OutboxHealthIndicator indicator = new OutboxHealthIndicator(outboxR2dbcRepository, properties(100L));

        StepVerifier.create(indicator.health())
                .assertNext(health -> assertThat(health.getStatus()).isEqualTo(Status.DOWN))
                .verifyComplete();
    }

    @Test
    void reportsDownWhenRepositoryErrors() {
        when(outboxR2dbcRepository.countActivePending())
                .thenReturn(Mono.error(new RuntimeException("db down")));
        OutboxHealthIndicator indicator = new OutboxHealthIndicator(outboxR2dbcRepository, properties(100L));

        StepVerifier.create(indicator.health())
                .assertNext(health -> assertThat(health.getStatus()).isEqualTo(Status.DOWN))
                .verifyComplete();
    }

    private static AppProperties properties(long critical) {
        return AppProperties.builder()
                .health(HealthProperties.builder().outboxPendingCritical(critical).build())
                .build();
    }
}
