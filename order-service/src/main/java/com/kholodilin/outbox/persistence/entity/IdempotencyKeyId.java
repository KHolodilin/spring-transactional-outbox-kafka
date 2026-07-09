package com.kholodilin.outbox.persistence.entity;

import lombok.AllArgsConstructor;
import lombok.EqualsAndHashCode;
import lombok.NoArgsConstructor;

import java.io.Serializable;

/** Composite primary key for hash-partitioned {@link IdempotencyKeyEntity}. */
@NoArgsConstructor
@AllArgsConstructor
@EqualsAndHashCode
public class IdempotencyKeyId implements Serializable {
    private Long customerId;
    private Long id;
}
