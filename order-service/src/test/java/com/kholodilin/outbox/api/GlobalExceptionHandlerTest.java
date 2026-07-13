package com.kholodilin.outbox.api;

import com.kholodilin.outbox.idempotency.IdempotencyConflictException;
import org.junit.jupiter.api.Test;
import org.springframework.http.HttpStatus;
import org.springframework.http.ProblemDetail;

import static org.assertj.core.api.Assertions.assertThat;

class GlobalExceptionHandlerTest {

    private final GlobalExceptionHandler handler = new GlobalExceptionHandler();

    @Test
    void mapsIdempotencyConflictTo409ProblemDetail() {
        ProblemDetail problem = handler.handleIdempotencyConflict(
                new IdempotencyConflictException("Key conflict"));

        assertThat(problem.getStatus()).isEqualTo(HttpStatus.CONFLICT.value());
        assertThat(problem.getTitle()).isEqualTo("Idempotency conflict");
        assertThat(problem.getDetail()).isEqualTo("Key conflict");
    }
}
