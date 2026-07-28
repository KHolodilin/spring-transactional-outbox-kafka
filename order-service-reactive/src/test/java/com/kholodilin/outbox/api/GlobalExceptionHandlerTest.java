package com.kholodilin.outbox.api;

import com.kholodilin.outbox.idempotency.IdempotencyConflictException;
import com.kholodilin.outbox.metrics.OutboxMetrics;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import io.r2dbc.spi.R2dbcTimeoutException;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.http.HttpStatus;
import org.springframework.http.ProblemDetail;
import org.springframework.test.util.ReflectionTestUtils;

import java.util.concurrent.TimeoutException;

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
    void mapsR2dbcTimeoutTo429() {
        ProblemDetail problem = handler.handleR2dbcTimeout(
                new R2dbcTimeoutException("Connection acquisition timed out"));

        assertThat(problem.getStatus()).isEqualTo(HttpStatus.TOO_MANY_REQUESTS.value());
        assertThat(problem.getTitle()).isEqualTo("Database pool exhausted");
        assertThat(problem.getDetail()).contains("acquisition timed out");
        assertThat(registry.find("outbox.pool_exhausted.rejects").counter().count()).isEqualTo(1.0);
    }

    @Test
    void mapsTimeoutExceptionTo429() {
        ProblemDetail problem = handler.handleTimeout(new TimeoutException("Pool acquire timed out"));

        assertThat(problem.getStatus()).isEqualTo(HttpStatus.TOO_MANY_REQUESTS.value());
        assertThat(problem.getTitle()).isEqualTo("Database pool exhausted");
        assertThat(registry.find("outbox.pool_exhausted.rejects").counter().count()).isEqualTo(1.0);
    }
}
