package com.kholodilin.outbox.config;

/** Thresholds for Actuator health indicators (queue pressure, pending outbox rows). */
public class HealthProperties {

    private double queuePressureCritical = 0.95;
    private long outboxPendingCritical = 10000;

    public double getQueuePressureCritical() {
        return queuePressureCritical;
    }

    public void setQueuePressureCritical(double queuePressureCritical) {
        this.queuePressureCritical = queuePressureCritical;
    }

    public long getOutboxPendingCritical() {
        return outboxPendingCritical;
    }

    public void setOutboxPendingCritical(long outboxPendingCritical) {
        this.outboxPendingCritical = outboxPendingCritical;
    }
}
