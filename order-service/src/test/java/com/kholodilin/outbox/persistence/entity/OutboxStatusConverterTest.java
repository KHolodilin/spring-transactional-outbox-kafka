package com.kholodilin.outbox.persistence.entity;

import com.kholodilin.outbox.events.OutboxStatus;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class OutboxStatusConverterTest {

    private final OutboxStatusConverter converter = new OutboxStatusConverter();

    @Test
    void convertsEnumToDatabaseColumn() {
        assertThat(converter.convertToDatabaseColumn(OutboxStatus.SENT)).isEqualTo(OutboxStatus.SENT.getCode());
        assertThat(converter.convertToDatabaseColumn(null)).isNull();
    }

    @Test
    void convertsDatabaseColumnToEnum() {
        assertThat(converter.convertToEntityAttribute(OutboxStatus.NEW.getCode())).isEqualTo(OutboxStatus.NEW);
        assertThat(converter.convertToEntityAttribute(null)).isNull();
    }
}
