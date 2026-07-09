package com.kholodilin.outbox.persistence.entity;

import com.kholodilin.outbox.events.OutboxStatus;
import jakarta.persistence.Convert;
import jakarta.persistence.Converter;
import jakarta.persistence.AttributeConverter;

/** Maps {@link OutboxStatus} enum codes to {@code outbox_events.status} INT column. */
@Converter
public class OutboxStatusConverter implements AttributeConverter<OutboxStatus, Integer> {

    @Override
    public Integer convertToDatabaseColumn(OutboxStatus attribute) {
        return attribute == null ? null : attribute.getCode();
    }

    @Override
    public OutboxStatus convertToEntityAttribute(Integer dbData) {
        return dbData == null ? null : OutboxStatus.fromCode(dbData);
    }
}
