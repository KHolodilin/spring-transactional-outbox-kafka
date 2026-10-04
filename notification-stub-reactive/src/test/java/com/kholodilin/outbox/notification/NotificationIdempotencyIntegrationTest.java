package com.kholodilin.outbox.notification;

import com.kholodilin.idempotency.exception.IdempotencyConflictException;
import com.kholodilin.outbox.consumer.KafkaNotificationConsumer;
import com.kholodilin.outbox.events.EventEnvelope;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.r2dbc.core.DatabaseClient;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.springframework.test.context.bean.override.mockito.MockitoBean;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import reactor.test.StepVerifier;

import java.util.Map;

@SpringBootTest(
        webEnvironment = SpringBootTest.WebEnvironment.NONE,
        properties = {
                "management.tracing.enabled=false",
                "management.otlp.metrics.export.enabled=false"
        }
)
@Testcontainers
class NotificationIdempotencyIntegrationTest {

    @Container
    static final PostgreSQLContainer<?> POSTGRES =
            new PostgreSQLContainer<>("postgres:16-alpine")
                    .withDatabaseName("outbox_reactive")
                    .withUsername("outbox")
                    .withPassword("outbox");

    @DynamicPropertySource
    static void databaseProperties(
            DynamicPropertyRegistry registry
    ) {
        registry.add(
                "spring.r2dbc.url",
                () -> "r2dbc:postgresql://"
                        + POSTGRES.getHost()
                        + ":"
                        + POSTGRES.getMappedPort(5432)
                        + "/"
                        + POSTGRES.getDatabaseName()
        );

        registry.add(
                "spring.r2dbc.username",
                POSTGRES::getUsername
        );

        registry.add(
                "spring.r2dbc.password",
                POSTGRES::getPassword
        );

        registry.add(
                "spring.liquibase.url",
                POSTGRES::getJdbcUrl
        );

        registry.add(
                "spring.liquibase.user",
                POSTGRES::getUsername
        );

        registry.add(
                "spring.liquibase.password",
                POSTGRES::getPassword
        );
    }

    /*
     * This test verifies R2DBC + idempotency + PostgreSQL.
     * Do not start the real Kafka consumer.
     */
    @MockitoBean
    private KafkaNotificationConsumer kafkaNotificationConsumer;

    @Autowired
    private NotificationTransactionService service;

    @Autowired
    private DatabaseClient databaseClient;

    @BeforeEach
    void cleanDatabase() {
        databaseClient
                .sql("DELETE FROM notification_idempotency_records")
                .fetch()
                .rowsUpdated()
                .block();
    }

    @Test
    void firstEventExecutesNotification() {
        EventEnvelope event =
                event(100L, Map.of("version", 1));

        StepVerifier.create(service.process(event))
                .expectNext(true)
                .verifyComplete();
    }

    @Test
    void replayOfSameEventIsSkipped() {
        EventEnvelope event =
                event(101L, Map.of("version", 1));

        StepVerifier.create(service.process(event))
                .expectNext(true)
                .verifyComplete();

        StepVerifier.create(service.process(event))
                .expectNext(false)
                .verifyComplete();
    }

    @Test
    void sameEventIdWithDifferentPayloadConflicts() {
        EventEnvelope first =
                event(
                        102L,
                        Map.of("version", 1)
                );

        EventEnvelope conflicting =
                event(
                        102L,
                        Map.of("version", 2)
                );

        StepVerifier.create(service.process(first))
                .expectNext(true)
                .verifyComplete();

        StepVerifier.create(service.process(conflicting))
                .expectError(
                        IdempotencyConflictException.class
                )
                .verify();
    }

    private EventEnvelope event(
            long eventId,
            Map<String, Object> payload
    ) {
        return new EventEnvelope(
                eventId,
                200L,
                300L,
                "OrderCreated",
                payload,
                "integration-test",
                null,
                null
        );
    }
}