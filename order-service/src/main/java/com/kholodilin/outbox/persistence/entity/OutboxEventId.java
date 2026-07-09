package com.kholodilin.outbox.persistence.entity;

import lombok.AllArgsConstructor;
import lombok.EqualsAndHashCode;
import lombok.NoArgsConstructor;

import java.io.Serializable;

/** Composite primary key for range-partitioned {@link OutboxEventEntity}. */
@NoArgsConstructor
@AllArgsConstructor
@EqualsAndHashCode
public class OutboxEventId implements Serializable {
    private Integer status;
    private Long id;
}
