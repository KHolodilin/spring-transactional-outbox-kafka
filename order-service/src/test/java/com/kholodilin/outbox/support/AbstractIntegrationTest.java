package com.kholodilin.outbox.support;

import org.springframework.boot.testcontainers.service.connection.ServiceConnection;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.kafka.KafkaContainer;
import org.testcontainers.utility.DockerImageName;

/**
 * Shared Testcontainers Kafka broker for integration tests.
 */
@Testcontainers
public abstract class AbstractIntegrationTest {

    @Container
    @ServiceConnection
    @SuppressWarnings("resource")
    protected static final KafkaContainer KAFKA = new KafkaContainer(
            DockerImageName.parse("confluentinc/cp-kafka:7.6.1")
    );
}
