package com.kholodilin.outbox.config;

/** Token-bucket capacity and refill rate for rate limiting. */
public class RateLimitBucketProperties {

    private long capacity;
    private long refillPerSecond;

    public RateLimitBucketProperties() {
    }

    public RateLimitBucketProperties(long capacity, long refillPerSecond) {
        this.capacity = capacity;
        this.refillPerSecond = refillPerSecond;
    }

    public long getCapacity() {
        return capacity;
    }

    public void setCapacity(long capacity) {
        this.capacity = capacity;
    }

    public long getRefillPerSecond() {
        return refillPerSecond;
    }

    public void setRefillPerSecond(long refillPerSecond) {
        this.refillPerSecond = refillPerSecond;
    }
}
