package com.kholodilin.outbox.notification;

import com.kholodilin.idempotency.exception.IdempotencyConflictException;
import com.kholodilin.outbox.events.EventEnvelope;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.SpringBootConfiguration;
import org.springframework.boot.autoconfigure.EnableAutoConfiguration;
import org.springframework.boot.kafka.autoconfigure.KafkaAutoConfiguration;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.context.annotation.Import;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.test.context.ActiveProfiles;

import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

@SpringBootTest(classes = NotificationIdempotencyIT.TestApplication.class)
@ActiveProfiles("test")
class NotificationIdempotencyIT {

    private static final AtomicLong EVENT_IDS = new AtomicLong(10_000);

    @Autowired
    private NotificationTransactionService service;

    @Autowired
    private JdbcTemplate jdbcTemplate;

    @Test
    void firstDeliveryRunsAndReplayIsSkipped() {
        long eventId = EVENT_IDS.incrementAndGet();
        EventEnvelope event = event(eventId, Map.of("orderId", 2));

        assertThat(service.process(event)).isTrue();
        assertThat(service.process(event)).isFalse();
        assertThat(recordCount(eventId)).isOne();
    }

    @Test
    void sameEventIdWithDifferentBodyRaisesConflict() {
        long eventId = EVENT_IDS.incrementAndGet();
        EventEnvelope first = event(eventId, Map.of("version", 1));
        EventEnvelope conflicting = event(eventId, Map.of("version", 2));

        assertThat(service.process(first)).isTrue();
        assertThatThrownBy(() -> service.process(conflicting))
                .isInstanceOf(IdempotencyConflictException.class);
        assertThat(recordCount(eventId)).isOne();
    }

    private int recordCount(long eventId) {
        Integer count = jdbcTemplate.queryForObject(
                "SELECT COUNT(*) FROM notification_idempotency_records WHERE idempotency_key = ?",
                Integer.class,
                String.valueOf(eventId)
        );
        return count == null ? 0 : count;
    }

    private EventEnvelope event(long eventId, Map<String, Object> payload) {
        return new EventEnvelope(eventId, 2L, 3L, "OrderCreated", payload, "corr", null, null);
    }

    @SpringBootConfiguration
    @EnableAutoConfiguration(exclude = KafkaAutoConfiguration.class)
    @Import(NotificationTransactionService.class)
    static class TestApplication {
    }
}
