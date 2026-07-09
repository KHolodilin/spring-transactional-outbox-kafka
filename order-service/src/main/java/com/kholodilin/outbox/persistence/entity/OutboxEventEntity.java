package com.kholodilin.outbox.persistence.entity;

import com.kholodilin.outbox.events.OutboxStatus;
import jakarta.persistence.Column;
import jakarta.persistence.Convert;
import jakarta.persistence.Entity;
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
 * JPA mapping for range-partitioned {@code outbox_events}.
 * <p>
 * Publisher/recovery use {@link com.kholodilin.outbox.persistence.OutboxJdbcRepository} for batch updates.
 */
@Entity
@Table(name = "outbox_events")
@IdClass(OutboxEventId.class)
@Getter
@Setter
public class OutboxEventEntity {

    @Id
    @Convert(converter = OutboxStatusConverter.class)
    @Column(nullable = false)
    private OutboxStatus status;

    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    private Long id;

    @Column(name = "order_id", nullable = false)
    private Long orderId;

    @Column(name = "customer_id", nullable = false)
    private Long customerId;

    @Column(name = "event_type", nullable = false)
    private String eventType;

    @JdbcTypeCode(SqlTypes.JSON)
    @Column(nullable = false)
    private String payload;

    @Column(name = "retry_count", nullable = false)
    private int retryCount;

    @Column(name = "locked_by")
    private String lockedBy;

    @Column(name = "locked_until")
    private Instant lockedUntil;

    @Column(name = "sent_at")
    private Instant sentAt;

    @Column(name = "created_at", nullable = false)
    private Instant createdAt;
}
