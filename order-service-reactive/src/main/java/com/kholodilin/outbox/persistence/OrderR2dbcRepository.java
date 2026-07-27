package com.kholodilin.outbox.persistence;

import lombok.RequiredArgsConstructor;
import org.springframework.r2dbc.core.DatabaseClient;
import org.springframework.stereotype.Repository;
import reactor.core.publisher.Mono;

import java.math.BigDecimal;
import java.time.Instant;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;

/** R2DBC writes for {@code orders} and {@code order_items}. */
@Repository
@RequiredArgsConstructor
public class OrderR2dbcRepository {

    private final DatabaseClient databaseClient;

    public Mono<Long> insertOrder(Long customerId, BigDecimal totalAmount, Instant now) {
        OffsetDateTime ts = toOffset(now);
        return databaseClient.sql("""
                        INSERT INTO orders (customer_id, status, total_amount, created_at, updated_at)
                        VALUES (:customerId, :status, :totalAmount, :createdAt, :updatedAt)
                        RETURNING id
                        """)
                .bind("customerId", customerId)
                .bind("status", "CREATED")
                .bind("totalAmount", totalAmount)
                .bind("createdAt", ts)
                .bind("updatedAt", ts)
                .map((row, metadata) -> row.get("id", Long.class))
                .one();
    }

    public Mono<Void> insertOrderItem(
            Long orderId,
            Long customerId,
            String productId,
            int quantity,
            BigDecimal price,
            Instant now
    ) {
        return databaseClient.sql("""
                        INSERT INTO order_items (order_id, customer_id, product_id, quantity, price, created_at)
                        VALUES (:orderId, :customerId, :productId, :quantity, :price, :createdAt)
                        """)
                .bind("orderId", orderId)
                .bind("customerId", customerId)
                .bind("productId", productId)
                .bind("quantity", quantity)
                .bind("price", price)
                .bind("createdAt", toOffset(now))
                .fetch()
                .rowsUpdated()
                .then();
    }

    private static OffsetDateTime toOffset(Instant instant) {
        return instant.atOffset(ZoneOffset.UTC);
    }
}
