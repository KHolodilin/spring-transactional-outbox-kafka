package com.kholodilin.outbox.health;

import com.kholodilin.outbox.config.AppProperties;
import com.kholodilin.outbox.persistence.OutboxJdbcRepository;
import org.springframework.boot.actuate.health.Health;
import org.springframework.boot.actuate.health.HealthIndicator;
import org.springframework.stereotype.Component;

/** Reports DOWN when too many events are stuck in ACTIVE (NEW/FAILED/PROCESSING). */
@Component
public class OutboxHealthIndicator implements HealthIndicator {

    private final OutboxJdbcRepository outboxJdbcRepository;
    private final AppProperties properties;

    public OutboxHealthIndicator(OutboxJdbcRepository outboxJdbcRepository, AppProperties properties) {
        this.outboxJdbcRepository = outboxJdbcRepository;
        this.properties = properties;
    }

    @Override
    public Health health() {
        long pending = outboxJdbcRepository.countActivePending();
        if (pending >= properties.getHealth().getOutboxPendingCritical()) {
            return Health.down().withDetail("activePending", pending).build();
        }
        return Health.up().withDetail("activePending", pending).build();
    }
}
