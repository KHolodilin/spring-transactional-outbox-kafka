package com.kholodilin.outbox.persistence.entity;

import com.kholodilin.outbox.events.OutboxStatus;
import jakarta.persistence.Convert;
import jakarta.persistence.Converter;
import jakarta.persistence.AttributeConverter;

/** Maps {@link OutboxStatus} enum codes to {@code outbox_events.status} INT column. */
@Converter
public class OutboxStatusConverter implements AttributeConverter<OutboxStatus, Integer> {

    /**
     * @param attribute enum value, or {@code null}
     * @return persisted INT code, or {@code null}
     */
    @Override
    public Integer convertToDatabaseColumn(OutboxStatus attribute) {
        return attribute == null ? null : attribute.getCode();
    }

    /**
     * @param dbData INT from {@code outbox_events.status}, or {@code null}
     * @return matching {@link OutboxStatus}, or {@code null}
     */
    @Override
    public OutboxStatus convertToEntityAttribute(Integer dbData) {
        return dbData == null ? null : OutboxStatus.fromCode(dbData);
    }
}
