package com.kholodilin.outbox.health;

import com.kholodilin.outbox.config.AppProperties;
import com.kholodilin.outbox.persistence.OutboxJdbcRepository;
import lombok.RequiredArgsConstructor;
import org.springframework.boot.health.contributor.Health;
import org.springframework.boot.health.contributor.HealthIndicator;
import org.springframework.stereotype.Component;

/** Reports DOWN when too many events are stuck in ACTIVE (NEW/FAILED/PROCESSING). */
@Component
@RequiredArgsConstructor
public class OutboxHealthIndicator implements HealthIndicator {

    private final OutboxJdbcRepository outboxJdbcRepository;
    private final AppProperties properties;

    /**
     * Compares active pending outbox count against {@code app.health.outbox-pending-critical}.
     *
     * @return DOWN when the backlog is at or above the critical threshold
     */
    @Override
    public Health health() {
        long pending = outboxJdbcRepository.countActivePending();
        if (pending >= properties.getHealth().getOutboxPendingCritical()) {
            return Health.down().withDetail("activePending", pending).build();
        }
        return Health.up().withDetail("activePending", pending).build();
    }
}
