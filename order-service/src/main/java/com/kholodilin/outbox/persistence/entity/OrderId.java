package com.kholodilin.outbox.persistence.entity;

import lombok.AllArgsConstructor;
import lombok.EqualsAndHashCode;
import lombok.NoArgsConstructor;

import java.io.Serializable;

/** Composite primary key for hash-partitioned {@link OrderEntity}. */
@NoArgsConstructor
@AllArgsConstructor
@EqualsAndHashCode
public class OrderId implements Serializable {
    private Long customerId;
    private Long id;
}
