package com.kholodilin.outbox.api;

import com.kholodilin.outbox.events.EventConstants;
import com.kholodilin.outbox.events.OutboxStatus;
import com.kholodilin.outbox.recovery.RecoveryWorker;
import com.kholodilin.outbox.support.AbstractIntegrationTest;
import com.kholodilin.outbox.support.KafkaPublisherMockTestConfig;
import org.junit.jupiter.api.Test;
import org.springframework.context.annotation.Import;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.test.context.ActiveProfiles;

import java.time.Instant;

import static org.assertj.core.api.Assertions.assertThat;

@SpringBootTest
@ActiveProfiles("test")
@Import(KafkaPublisherMockTestConfig.class)
class RecoveryIT extends AbstractIntegrationTest {

    @Autowired
    private JdbcTemplate jdbcTemplate;

    @Autowired
    private RecoveryWorker recoveryWorker;

    @Autowired
    private com.kholodilin.outbox.config.AppProperties properties;

    @Test
    void recoveryEnqueuesAndPublishesNewEvent() {
        Instant now = Instant.now();
        Long eventId = jdbcTemplate.queryForObject(
                """
                        INSERT INTO outbox_events (order_id, customer_id, event_type, payload, status, retry_count, created_at)
                        VALUES (?, ?, ?, ?::jsonb, ?, 0, ?)
                        RETURNING id
                        """,
                Long.class,
                100L,
                55L,
                EventConstants.EVENT_TYPE_ORDER_CREATED,
                "{\"orderId\":100,\"customerId\":55}",
                OutboxStatus.NEW.getCode(),
                now
        );

        properties.getOutbox().getRecovery().setEnabled(true);
        recoveryWorker.recover();

        awaitSentInDatabase(eventId);

        Long customerId = jdbcTemplate.queryForObject(
                "SELECT customer_id FROM outbox_events WHERE id = ?",
                Long.class,
                eventId
        );
        assertThat(customerId).isEqualTo(55L);
    }

    private void awaitSentInDatabase(long eventId) {
        long deadline = System.currentTimeMillis() + 20_000;
        Integer lastStatus = null;
        while (System.currentTimeMillis() < deadline) {
            lastStatus = jdbcTemplate.queryForObject(
                    "SELECT status FROM outbox_events WHERE id = ?",
                    Integer.class,
                    eventId
            );
            if (lastStatus != null && lastStatus == OutboxStatus.SENT.getCode()) {
                return;
            }
            try {
                Thread.sleep(200);
            } catch (InterruptedException ex) {
                Thread.currentThread().interrupt();
                throw new IllegalStateException("Interrupted while waiting for outbox SENT status", ex);
            }
        }
        Integer retryCount = jdbcTemplate.queryForObject(
                "SELECT retry_count FROM outbox_events WHERE id = ?",
                Integer.class,
                eventId
        );
        throw new AssertionError(
                "Outbox event " + eventId + " was not marked SENT within 20s (lastStatus="
                        + lastStatus + ", retryCount=" + retryCount + ")"
        );
    }
}
