package com.kholodilin.outbox.persistence.entity;

import com.kholodilin.outbox.events.IdempotencyStatus;
import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.EnumType;
import jakarta.persistence.Enumerated;
import jakarta.persistence.GeneratedValue;
import jakarta.persistence.GenerationType;
import jakarta.persistence.Id;
import jakarta.persistence.IdClass;
import jakarta.persistence.Table;
import lombok.Getter;
import lombok.Setter;
import org.hibernate.annotations.JdbcTypeCode;
import org.hibernate.type.SqlTypes;

import java.time.Instant;

/**
 * JPA mapping for hash-partitioned {@code idempotency_keys} table.
 * <p>
 * Writes go through {@link com.kholodilin.outbox.persistence.IdempotencyJdbcRepository};
 * this entity exists for schema validation ({@code ddl-auto: validate}) only.
 */
@Entity
@Table(name = "idempotency_keys")
@IdClass(IdempotencyKeyId.class)
@Getter
@Setter
public class IdempotencyKeyEntity {

    @Id
    @Column(name = "customer_id")
    private Long customerId;

    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    private Long id;

    @Column(name = "idempotency_key", nullable = false)
    private String idempotencyKey;

    @Column(name = "request_hash", nullable = false)
    private String requestHash;

    @Enumerated(EnumType.ORDINAL)
    @Column(nullable = false)
    private IdempotencyStatus status;

    @JdbcTypeCode(SqlTypes.JSON)
    @Column(name = "response_body")
    private String responseBody;

    @Column(name = "created_at", nullable = false)
    private Instant createdAt;

    @Column(name = "updated_at", nullable = false)
    private Instant updatedAt;
}
