package com.kholodilin.outbox.api;

import com.kholodilin.outbox.idempotency.IdempotencyConflictException;
import org.springframework.http.HttpStatus;
import org.springframework.http.ProblemDetail;
import org.springframework.web.bind.annotation.ExceptionHandler;
import org.springframework.web.bind.annotation.RestControllerAdvice;

import java.net.URI;

/** Maps idempotency and validation errors to RFC 7807 {@link org.springframework.http.ProblemDetail}. */
@RestControllerAdvice
public class GlobalExceptionHandler {

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
}
