package com.kholodilin.outbox.events;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class IdempotencyStatusTest {

    @Test
    void resolvesStatusByCode() {
        assertThat(IdempotencyStatus.fromCode(0)).isEqualTo(IdempotencyStatus.PROCESSING);
        assertThat(IdempotencyStatus.fromCode(1)).isEqualTo(IdempotencyStatus.COMPLETED);
        assertThat(IdempotencyStatus.fromCode(2)).isEqualTo(IdempotencyStatus.FAILED);
    }

    @Test
    void exposesPersistedCodes() {
        assertThat(IdempotencyStatus.COMPLETED.getCode()).isEqualTo(1);
    }

    @Test
    void rejectsUnknownCode() {
        assertThatThrownBy(() -> IdempotencyStatus.fromCode(99))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("99");
    }
}
