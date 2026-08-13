package com.kholodilin.outbox.support;

import com.kholodilin.outbox.model.OutboxStatus;
import com.kholodilin.outbox.publisher.KafkaBatchPublisher;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.test.context.bean.override.mockito.MockitoBean;

/** Shared wiring for order-service integration tests. */
public abstract class AbstractIntegrationTest {

    private static final int SENT = OutboxStatus.SENT.getCode();

    @MockitoBean
    protected KafkaBatchPublisher kafkaBatchPublisher;

    @Autowired
    protected JdbcTemplate jdbcTemplate;

    protected void awaitSentInDatabase(long eventId) {
        long deadline = System.currentTimeMillis() + 20_000;
        Integer lastStatus = null;
        while (System.currentTimeMillis() < deadline) {
            Integer status = queryStatus(eventId);
            if (status != null && status == SENT) {
                return;
            }
            lastStatus = status;
            try {
                Thread.sleep(200);
            } catch (InterruptedException ex) {
                Thread.currentThread().interrupt();
                throw new IllegalStateException("Interrupted while waiting for outbox SENT status", ex);
            }
        }
        Integer status = queryStatus(eventId);
        Integer retryCount = queryRetryCount(eventId);
        throw new AssertionError(
                "Outbox event " + eventId + " was not marked SENT within 20s (lastStatus="
                        + (status != null ? status : lastStatus) + ", retryCount=" + retryCount + ")"
        );
    }

    protected Integer queryStatus(long eventId) {
        return jdbcTemplate.query(
                "SELECT status FROM outbox_events WHERE id = ?",
                rs -> rs.next() ? rs.getInt("status") : null,
                eventId
        );
    }

    protected Integer queryRetryCount(long eventId) {
        return jdbcTemplate.query(
                "SELECT retry_count FROM outbox_events WHERE id = ?",
                rs -> rs.next() ? rs.getInt("retry_count") : null,
                eventId
        );
    }

    protected String queryPartitionKey(long eventId) {
        return jdbcTemplate.query(
                "SELECT partition_key FROM outbox_events WHERE id = ?",
                rs -> rs.next() ? rs.getString("partition_key") : null,
                eventId
        );
    }
}
