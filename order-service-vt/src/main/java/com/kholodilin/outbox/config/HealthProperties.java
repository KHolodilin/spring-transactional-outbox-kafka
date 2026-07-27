package com.kholodilin.outbox.config;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;

/** Thresholds for Actuator health indicators (queue pressure, pending outbox rows). */
@Getter
@Setter
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class HealthProperties {

    /** Queue fill ratio at or above which the queue health indicator reports DOWN. */
    @Builder.Default
    private double queuePressureCritical = 0.95;

    /** Active outbox row count at or above which the outbox health indicator reports DOWN. */
    @Builder.Default
    private long outboxPendingCritical = 10000;
}
