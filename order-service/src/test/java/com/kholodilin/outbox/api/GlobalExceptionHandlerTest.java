package com.kholodilin.outbox.api;

import com.kholodilin.outbox.idempotency.IdempotencyConflictException;
import com.kholodilin.outbox.metrics.OutboxMetrics;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.http.HttpStatus;
import org.springframework.http.ProblemDetail;
import org.springframework.jdbc.CannotGetJdbcConnectionException;
import org.springframework.test.util.ReflectionTestUtils;

import java.sql.SQLTransientConnectionException;

import static org.assertj.core.api.Assertions.assertThat;

class GlobalExceptionHandlerTest {

    private SimpleMeterRegistry registry;
    private GlobalExceptionHandler handler;

    @BeforeEach
    void setUp() {
        registry = new SimpleMeterRegistry();
        OutboxMetrics metrics = new OutboxMetrics(registry);
        ReflectionTestUtils.invokeMethod(metrics, "registerMeters");
        handler = new GlobalExceptionHandler(metrics);
    }

    @Test
    void mapsIdempotencyConflictTo409ProblemDetail() {
        ProblemDetail problem = handler.handleIdempotencyConflict(
                new IdempotencyConflictException("Key conflict"));

        assertThat(problem.getStatus()).isEqualTo(HttpStatus.CONFLICT.value());
        assertThat(problem.getTitle()).isEqualTo("Idempotency conflict");
        assertThat(problem.getDetail()).isEqualTo("Key conflict");
    }

    @Test
    void mapsCannotGetJdbcConnectionTo429() {
        CannotGetJdbcConnectionException ex = new CannotGetJdbcConnectionException(
                "Failed to obtain JDBC Connection",
                new SQLTransientConnectionException("HikariPool-1 - Connection is not available"));

        ProblemDetail problem = handler.handleCannotGetJdbcConnection(ex);

        assertThat(problem.getStatus()).isEqualTo(HttpStatus.TOO_MANY_REQUESTS.value());
        assertThat(problem.getTitle()).isEqualTo("Database pool exhausted");
        assertThat(problem.getDetail()).contains("Connection is not available");
        assertThat(registry.find("outbox.pool_exhausted.rejects").counter().count()).isEqualTo(1.0);
    }

    @Test
    void mapsSqlTransientConnectionTo429() {
        ProblemDetail problem = handler.handleSqlTransientConnection(
                new SQLTransientConnectionException("Connection is not available, request timed out"));

        assertThat(problem.getStatus()).isEqualTo(HttpStatus.TOO_MANY_REQUESTS.value());
        assertThat(problem.getTitle()).isEqualTo("Database pool exhausted");
        assertThat(registry.find("outbox.pool_exhausted.rejects").counter().count()).isEqualTo(1.0);
    }
}
