package com.kholodilin.outbox.persistence.entity;

import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.GeneratedValue;
import jakarta.persistence.GenerationType;
import jakarta.persistence.Id;
import jakarta.persistence.Table;
import lombok.Getter;
import lombok.Setter;

import java.math.BigDecimal;
import java.time.Instant;

/**
 * JPA mapping for {@code order_items}.
 * <p>
 * Writes go through {@link com.kholodilin.outbox.persistence.OrderJdbcRepository};
 * no FK to {@code orders} — referential integrity is enforced in application code.
 */
@Entity
@Table(name = "order_items")
@Getter
@Setter
public class OrderItemEntity {

    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    private Long id;

    @Column(name = "order_id", nullable = false)
    private Long orderId;

    @Column(name = "customer_id", nullable = false)
    private Long customerId;

    @Column(name = "product_id", nullable = false)
    private String productId;

    @Column(nullable = false)
    private int quantity;

    @Column(nullable = false)
    private BigDecimal price;

    @Column(name = "created_at", nullable = false)
    private Instant createdAt;
}
