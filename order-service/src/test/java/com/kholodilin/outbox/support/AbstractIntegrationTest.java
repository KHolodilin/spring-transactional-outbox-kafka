package com.kholodilin.outbox.support;

import org.springframework.context.annotation.Import;

/** Shared test wiring for order-service integration tests. */
@Import(KafkaPublisherMockTestConfig.class)
public abstract class AbstractIntegrationTest {
}
