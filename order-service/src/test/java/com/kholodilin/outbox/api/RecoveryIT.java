package com.kholodilin.outbox.api;

import com.kholodilin.outbox.events.EventConstants;
import com.kholodilin.outbox.persistence.OutboxRow;
import com.kholodilin.outbox.recovery.RecoveryWorker;
import com.kholodilin.outbox.support.AbstractIntegrationTest;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.ActiveProfiles;

import java.time.Instant;

import static org.assertj.core.api.Assertions.assertThat;

@SpringBootTest
@ActiveProfiles("test")
class RecoveryIT extends AbstractIntegrationTest {

    @Autowired
    private RecoveryWorker recoveryWorker;

    @Autowired
    private com.kholodilin.outbox.config.AppProperties properties;

    @Test
    void recoveryEnqueuesAndPublishesNewEvent() {
        long eventId = outboxJdbcRepository.insertEvent(
                100L,
                55L,
                EventConstants.EVENT_TYPE_ORDER_CREATED,
                "{\"orderId\":100,\"customerId\":55}",
                Instant.now()
        );

        properties.getOutbox().getRecovery().setEnabled(true);
        recoveryWorker.recover();

        awaitSentInDatabase(eventId);

        assertThat(outboxJdbcRepository.findById(eventId))
                .isPresent()
                .get()
                .extracting(OutboxRow::getCustomerId)
                .isEqualTo(55L);
    }
}
