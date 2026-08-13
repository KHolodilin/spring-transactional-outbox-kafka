package com.kholodilin.outbox.api;

import com.kholodilin.outbox.events.EventConstants;
import com.kholodilin.outbox.model.OutboxStatus;
import com.kholodilin.outbox.support.AbstractIntegrationTest;
import org.junit.jupiter.api.Test;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.ActiveProfiles;

import java.sql.Timestamp;
import java.time.Instant;

import static org.assertj.core.api.Assertions.assertThat;

@SpringBootTest
@ActiveProfiles("test")
class RecoveryIT extends AbstractIntegrationTest {

    @Test
    void recoveryEnqueuesAndPublishesNewEvent() {
        long eventId = jdbcTemplate.queryForObject(
                """
                        INSERT INTO outbox_events (
                            aggregate_id, partition_key, event_type, payload, headers,
                            status, retry_count, created_at
                        ) VALUES (?, ?, ?, ?::jsonb, NULL, ?, 0, ?)
                        RETURNING id
                        """,
                Long.class,
                "100",
                "55",
                EventConstants.EVENT_TYPE_ORDER_CREATED,
                "{\"orderId\":100,\"customerId\":55}",
                OutboxStatus.NEW.getCode(),
                Timestamp.from(Instant.now())
        );

        awaitSentInDatabase(eventId);

        assertThat(queryStatus(eventId)).isEqualTo(OutboxStatus.SENT.getCode());
        assertThat(queryPartitionKey(eventId)).isEqualTo("55");
    }
}
