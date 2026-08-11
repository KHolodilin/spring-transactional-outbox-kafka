package com.kholodilin.outbox.api;

import com.kholodilin.idempotency.exception.IdempotencyConflictException;
import com.kholodilin.outbox.metrics.OutboxMetrics;
import lombok.RequiredArgsConstructor;
import org.springframework.http.HttpStatus;
import org.springframework.http.ProblemDetail;
import org.springframework.jdbc.CannotGetJdbcConnectionException;
import org.springframework.web.bind.annotation.ExceptionHandler;
import org.springframework.web.bind.annotation.RestControllerAdvice;

import java.net.URI;
import java.sql.SQLTransientConnectionException;

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
     * Maps Hikari pool exhaustion to HTTP 429 (fallback if the create bulkhead is bypassed).
     *
     * @param ex Spring wrapper when a JDBC connection cannot be obtained
     * @return RFC 7807 body with status TOO_MANY_REQUESTS
     */
    @ExceptionHandler(CannotGetJdbcConnectionException.class)
    public ProblemDetail handleCannotGetJdbcConnection(CannotGetJdbcConnectionException ex) {
        return poolExhausted(ex.getMostSpecificCause() != null
                ? ex.getMostSpecificCause().getMessage()
                : ex.getMessage());
    }

    /**
     * Maps transient JDBC connection timeouts (Hikari) to HTTP 429.
     *
     * @param ex SQL transient connection failure
     * @return RFC 7807 body with status TOO_MANY_REQUESTS
     */
    @ExceptionHandler(SQLTransientConnectionException.class)
    public ProblemDetail handleSqlTransientConnection(SQLTransientConnectionException ex) {
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
