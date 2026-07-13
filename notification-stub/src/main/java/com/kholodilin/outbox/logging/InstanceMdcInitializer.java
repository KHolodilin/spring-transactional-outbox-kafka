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

    public void enrich() {
        StructuredLogContext.putInstanceFields(properties.getInstanceId());
        StructuredLogContext.enrichTracingAliases();
    }

    public void clearConsumerContext() {
        StructuredLogContext.clearConsumerContext();
        StructuredLogContext.putInstanceFields(properties.getInstanceId());
    }
}
