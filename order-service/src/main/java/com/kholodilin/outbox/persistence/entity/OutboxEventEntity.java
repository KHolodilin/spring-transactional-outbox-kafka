package com.kholodilin.outbox.persistence.entity;

import com.kholodilin.outbox.events.OutboxStatus;
import com.kholodilin.outbox.events.PartitionState;
import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.EnumType;
import jakarta.persistence.Enumerated;
import jakarta.persistence.GeneratedValue;
import jakarta.persistence.GenerationType;
import jakarta.persistence.Id;
import jakarta.persistence.Table;
import lombok.Getter;
import lombok.Setter;
import org.hibernate.annotations.JdbcTypeCode;
import org.hibernate.type.SqlTypes;

import java.time.Instant;

/**
 * JPA mapping for {@code outbox_events}.
 * <p>
 * Publisher/recovery use {@link com.kholodilin.outbox.persistence.OutboxJdbcRepository} for batch updates.
 */
@Entity
@Table(name = "outbox_events")
@Getter
@Setter
public class OutboxEventEntity {

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

    @Enumerated(EnumType.STRING)
    @Column(nullable = false)
    private OutboxStatus status;

    @Enumerated(EnumType.STRING)
    @Column(name = "partition_state", nullable = false)
    private PartitionState partitionState;

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
