package com.kholodilin.outbox.api;

import com.kholodilin.outbox.idempotency.IdempotencyConflictException;
import com.kholodilin.outbox.metrics.OutboxMetrics;
import io.r2dbc.spi.R2dbcTimeoutException;
import lombok.RequiredArgsConstructor;
import org.springframework.http.HttpStatus;
import org.springframework.http.ProblemDetail;
import org.springframework.web.bind.annotation.ExceptionHandler;
import org.springframework.web.bind.annotation.RestControllerAdvice;

import java.net.URI;
import java.util.concurrent.TimeoutException;

/** Maps idempotency and pool-exhaustion errors to RFC 7807 {@link ProblemDetail}. */
@RestControllerAdvice
@RequiredArgsConstructor
public class GlobalExceptionHandler {

    private final OutboxMetrics metrics;

    /**
     * Maps {@link IdempotencyConflictException} to HTTP 409 Problem Details.
     *
     * @param ex conflict thrown by the idempotency check
     * @return RFC 7807 body with status CONFLICT
     */
    @ExceptionHandler(IdempotencyConflictException.class)
    public ProblemDetail handleIdempotencyConflict(IdempotencyConflictException ex) {
        ProblemDetail problem = ProblemDetail.forStatusAndDetail(HttpStatus.CONFLICT, ex.getMessage());
        problem.setTitle("Idempotency conflict");
        problem.setType(URI.create("about:blank"));
        return problem;
    }

    /**
     * Maps R2DBC timeouts (including pool acquire) to HTTP 429.
     *
     * @param ex R2DBC timeout / acquire failure
     * @return RFC 7807 body with status TOO_MANY_REQUESTS
     */
    @ExceptionHandler(R2dbcTimeoutException.class)
    public ProblemDetail handleR2dbcTimeout(R2dbcTimeoutException ex) {
        return poolExhausted(ex.getMessage());
    }

    /**
     * Maps generic acquire/wait timeouts nested under reactive DB calls to HTTP 429.
     *
     * @param ex timeout while waiting for a pool connection
     * @return RFC 7807 body with status TOO_MANY_REQUESTS
     */
    @ExceptionHandler(TimeoutException.class)
    public ProblemDetail handleTimeout(TimeoutException ex) {
        return poolExhausted(ex.getMessage());
    }

    private ProblemDetail poolExhausted(String detail) {
        metrics.incrementPoolExhaustedRejects();
        ProblemDetail problem = ProblemDetail.forStatusAndDetail(
                HttpStatus.TOO_MANY_REQUESTS,
                detail != null ? detail : "Database connection pool exhausted");
        problem.setTitle("Database pool exhausted");
        problem.setType(URI.create("about:blank"));
        return problem;
    }
}
