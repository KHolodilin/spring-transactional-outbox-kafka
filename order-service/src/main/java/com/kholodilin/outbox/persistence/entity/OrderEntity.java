package com.kholodilin.outbox.persistence.entity;

import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.Id;
import jakarta.persistence.IdClass;
import jakarta.persistence.Table;
import lombok.Getter;
import lombok.Setter;

import java.math.BigDecimal;
import java.time.Instant;

/**
 * JPA mapping for hash-partitioned {@code orders} table.
 * <p>
 * Writes go through {@link com.kholodilin.outbox.persistence.OrderJdbcRepository};
 * this entity exists for schema validation ({@code ddl-auto: validate}) only.
 */
@Entity
@Table(name = "orders")
@IdClass(OrderId.class)
@Getter
@Setter
public class OrderEntity {

    @Id
    @Column(name = "customer_id")
    private Long customerId;

    @Id
    private Long id;

    @Column(nullable = false)
    private String status;

    @Column(name = "total_amount", nullable = false)
    private BigDecimal totalAmount;

    @Column(name = "created_at", nullable = false)
    private Instant createdAt;

    @Column(name = "updated_at", nullable = false)
    private Instant updatedAt;
}
