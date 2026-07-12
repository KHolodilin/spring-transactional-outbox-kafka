package com.kholodilin.outbox.persistence;

import tools.jackson.databind.json.JsonMapper;
import com.kholodilin.outbox.events.EventEnvelope;
import com.kholodilin.outbox.events.OutboxStatus;
import com.kholodilin.outbox.tracing.OutboxTracing;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.jdbc.core.JdbcTemplate;

import java.util.function.Supplier;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

class OutboxJdbcRepositoryTest {

    private final OutboxTracing outboxTracing = mock(OutboxTracing.class);
    private final OutboxJdbcRepository repository =
            new OutboxJdbcRepository(mock(JdbcTemplate.class), JsonMapper.builder().build(), outboxTracing);

    @BeforeEach
    void setUpOutboxTracingPassthrough() {
        when(outboxTracing.observe(anyString(), any(Supplier.class))).thenAnswer(invocation -> {
            Supplier<?> supplier = invocation.getArgument(1);
            return supplier.get();
        });
        doAnswer(invocation -> {
            Runnable runnable = invocation.getArgument(1);
            runnable.run();
            return null;
        }).when(outboxTracing).observe(anyString(), any(Runnable.class));
    }

    @Test
    void toEnvelopeBuildsKafkaMessage() {
        OutboxRow row = OutboxRow.builder()
                .id(1L)
                .orderId(99L)
                .customerId(10L)
                .eventType("OrderCreated")
                .payload("{\"orderId\":99,\"customerId\":10}")
                .status(OutboxStatus.NEW)
                .retryCount(0)
                .traceParent("00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01")
                .build();

        EventEnvelope envelope = repository.toEnvelope(row, "corr-1");

        assertThat(envelope.getEventId()).isEqualTo(1L);
        assertThat(envelope.getOrderId()).isEqualTo(99L);
        assertThat(envelope.getCustomerId()).isEqualTo(10L);
        assertThat(envelope.getEventType()).isEqualTo("OrderCreated");
        assertThat(envelope.getCorrelationId()).isEqualTo("corr-1");
        assertThat(envelope.getTraceParent()).isEqualTo("00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01");
        assertThat(envelope.getPayload()).containsEntry("orderId", 99);
    }
}
