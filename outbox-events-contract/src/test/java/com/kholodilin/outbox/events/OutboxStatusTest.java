package com.kholodilin.outbox.events;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class OutboxStatusTest {

    @Test
    void activeStatusesAreBelowArchiveThreshold() {
        assertThat(OutboxStatus.NEW.isActive()).isTrue();
        assertThat(OutboxStatus.PROCESSING.isActive()).isTrue();
        assertThat(OutboxStatus.FAILED.isActive()).isTrue();
        assertThat(OutboxStatus.DEAD.isArchive()).isTrue();
        assertThat(OutboxStatus.SENT.isArchive()).isTrue();
    }

    @Test
    void mapsKnownCodes() {
        assertThat(OutboxStatus.fromCode(0)).isEqualTo(OutboxStatus.NEW);
        assertThat(OutboxStatus.fromCode(110)).isEqualTo(OutboxStatus.SENT);
    }

    @Test
    void rejectsUnknownCode() {
        assertThatThrownBy(() -> OutboxStatus.fromCode(999))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("999");
    }
}
