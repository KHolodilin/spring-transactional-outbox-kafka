package com.kholodilin.outbox.logging;

import com.kholodilin.outbox.config.NotificationStubProperties;
import jakarta.annotation.PostConstruct;
import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Component;

/** Initializes background MDC fields for structured JSON logging. */
@Component
@RequiredArgsConstructor
public class InstanceMdcInitializer {

    private final NotificationStubProperties properties;

    @PostConstruct
    void init() {
        StructuredLogContext.putInstanceFields(properties.getInstanceId());
    }

    /**
     * Re-applies instance id and tracing aliases before logging a batch or record.
     * <p>
     * Needed because Kafka listener threads may not inherit the {@code @PostConstruct} MDC.
     */
    public void enrich() {
        StructuredLogContext.putInstanceFields(properties.getInstanceId());
        StructuredLogContext.enrichTracingAliases();
    }

    /**
     * Clears consumer-scoped MDC keys and restores the instance id for the next record.
     */
    public void clearConsumerContext() {
        StructuredLogContext.clearConsumerContext();
        StructuredLogContext.putInstanceFields(properties.getInstanceId());
    }
}
