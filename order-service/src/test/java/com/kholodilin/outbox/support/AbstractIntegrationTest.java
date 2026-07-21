package com.kholodilin.outbox.support;

import com.kholodilin.outbox.events.OutboxStatus;
import com.kholodilin.outbox.persistence.OutboxJdbcRepository;
import com.kholodilin.outbox.persistence.OutboxRow;
import com.kholodilin.outbox.publisher.KafkaBatchPublisher;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.test.context.bean.override.mockito.MockitoBean;

import java.util.Optional;

/** Shared wiring for order-service integration tests. */
public abstract class AbstractIntegrationTest {

    @MockitoBean
    protected KafkaBatchPublisher kafkaBatchPublisher;

    @Autowired
    protected OutboxJdbcRepository outboxJdbcRepository;

    protected void awaitSentInDatabase(long eventId) {
        long deadline = System.currentTimeMillis() + 20_000;
        OutboxStatus lastStatus = null;
        while (System.currentTimeMillis() < deadline) {
            Optional<OutboxRow> row = outboxJdbcRepository.findById(eventId);
            if (row.isPresent() && row.get().status() == OutboxStatus.SENT) {
                return;
            }
            lastStatus = row.map(OutboxRow::status).orElse(null);
            try {
                Thread.sleep(200);
            } catch (InterruptedException ex) {
                Thread.currentThread().interrupt();
                throw new IllegalStateException("Interrupted while waiting for outbox SENT status", ex);
            }
        }
        Optional<OutboxRow> row = outboxJdbcRepository.findById(eventId);
        Integer lastStatusCode = lastStatus == null ? null : lastStatus.getCode();
        int retryCount = row.map(OutboxRow::retryCount).orElse(-1);
        throw new AssertionError(
                "Outbox event " + eventId + " was not marked SENT within 20s (lastStatus="
                        + lastStatusCode + ", retryCount=" + retryCount + ")"
        );
    }
}
