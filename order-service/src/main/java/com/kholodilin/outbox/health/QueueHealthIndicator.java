package com.kholodilin.outbox.health;

import com.kholodilin.outbox.config.AppProperties;
import com.kholodilin.outbox.queue.InMemoryEventQueue;
import org.springframework.boot.health.contributor.Health;
import org.springframework.boot.health.contributor.HealthIndicator;
import org.springframework.stereotype.Component;

/** Reports DOWN when in-memory queue pressure exceeds the critical threshold. */
@Component
public class QueueHealthIndicator implements HealthIndicator {

    private final InMemoryEventQueue eventQueue;
    private final AppProperties properties;

    public QueueHealthIndicator(InMemoryEventQueue eventQueue, AppProperties properties) {
        this.eventQueue = eventQueue;
        this.properties = properties;
    }

    @Override
    public Health health() {
        double pressure = eventQueue.pressure();
        if (pressure >= properties.getHealth().getQueuePressureCritical()) {
            return Health.down()
                    .withDetail("queueSize", eventQueue.size())
                    .withDetail("queuePressure", pressure)
                    .build();
        }
        return Health.up()
                .withDetail("queueSize", eventQueue.size())
                .withDetail("queuePressure", pressure)
                .build();
    }
}
