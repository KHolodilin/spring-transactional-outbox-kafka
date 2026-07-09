package com.kholodilin.outbox.config;

import java.time.Duration;

/** Bounded per-pod queue between DB commit and Kafka publish. */
public class MemoryQueueProperties {

    private int capacity = 10000;
    private int batchSize = 100;
    private Duration batchWait = Duration.ofMillis(50);
    private double usageThreshold = 0.8;

    public int getCapacity() {
        return capacity;
    }

    public void setCapacity(int capacity) {
        this.capacity = capacity;
    }

    public int getBatchSize() {
        return batchSize;
    }

    public void setBatchSize(int batchSize) {
        this.batchSize = batchSize;
    }

    public Duration getBatchWait() {
        return batchWait;
    }

    public void setBatchWait(Duration batchWait) {
        this.batchWait = batchWait;
    }

    public double getUsageThreshold() {
        return usageThreshold;
    }

    public void setUsageThreshold(double usageThreshold) {
        this.usageThreshold = usageThreshold;
    }
}
