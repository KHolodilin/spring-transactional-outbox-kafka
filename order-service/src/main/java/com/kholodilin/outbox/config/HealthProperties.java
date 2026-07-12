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

    @Builder.Default
    private double queuePressureCritical = 0.95;

    @Builder.Default
    private long outboxPendingCritical = 10000;
}
