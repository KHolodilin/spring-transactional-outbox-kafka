package com.kholodilin.outbox.persistence;

import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Repository;

import java.math.BigDecimal;
import java.sql.Timestamp;
import java.time.Instant;

/**
 * JDBC writes for {@code orders} and {@code order_items}.
 * <p>
 * Uses plain JDBC (not JPA) because {@code orders} is hash-partitioned by {@code customer_id}.
 * No FK on {@code order_items.order_id} — referential integrity is enforced in application code.
 */
@Repository
public class OrderJdbcRepository {

    private final JdbcTemplate jdbcTemplate;

    public OrderJdbcRepository(JdbcTemplate jdbcTemplate) {
        this.jdbcTemplate = jdbcTemplate;
    }

    public long insertOrder(Long customerId, BigDecimal totalAmount, Instant now) {
        Long id = jdbcTemplate.queryForObject(
                """
                        INSERT INTO orders (customer_id, status, total_amount, created_at, updated_at)
                        VALUES (?, ?, ?, ?, ?)
                        RETURNING id
                        """,
                Long.class,
                customerId,
                "CREATED",
                totalAmount,
                Timestamp.from(now),
                Timestamp.from(now)
        );
        return id;
    }

    public void insertOrderItem(Long orderId, Long customerId, String productId, int quantity, BigDecimal price, Instant now) {
        jdbcTemplate.update(
                """
                        INSERT INTO order_items (order_id, customer_id, product_id, quantity, price, created_at)
                        VALUES (?, ?, ?, ?, ?, ?)
                        """,
                orderId,
                customerId,
                productId,
                quantity,
                price,
                Timestamp.from(now)
        );
    }
}
