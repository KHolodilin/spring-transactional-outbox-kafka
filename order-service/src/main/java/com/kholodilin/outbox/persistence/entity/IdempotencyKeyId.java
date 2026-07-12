package com.kholodilin.outbox.persistence.entity;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;

import java.io.Serializable;

/** Composite primary key for hash-partitioned {@link IdempotencyKeyEntity}. */
@NoArgsConstructor
@AllArgsConstructor
@Getter
@Setter
@Builder
@EqualsAndHashCode
public class IdempotencyKeyId implements Serializable {
    private Long customerId;
    private Long id;
}
