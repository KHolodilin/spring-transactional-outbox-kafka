package com.kholodilin.outbox.health;

import com.kholodilin.outbox.config.AppProperties;
import com.kholodilin.outbox.persistence.OutboxR2dbcRepository;
import lombok.RequiredArgsConstructor;
import org.springframework.boot.health.contributor.Health;
import org.springframework.boot.health.contributor.ReactiveHealthIndicator;
import org.springframework.stereotype.Component;
import reactor.core.publisher.Mono;

/** Reports DOWN when too many events are stuck in ACTIVE. */
@Component
@RequiredArgsConstructor
public class OutboxHealthIndicator implements ReactiveHealthIndicator {

    private final OutboxR2dbcRepository outboxR2dbcRepository;
    private final AppProperties properties;

    @Override
    public Mono<Health> health() {
        return outboxR2dbcRepository.countActivePending()
                .map(pending -> {
                    if (pending >= properties.getHealth().getOutboxPendingCritical()) {
                        return Health.down().withDetail("activePending", pending).build();
                    }
                    return Health.up().withDetail("activePending", pending).build();
                })
                .onErrorResume(ex -> Mono.just(Health.down(ex).build()));
    }
}
