package com.kholodilin.outbox.config;

/** Token-bucket limits with adaptive backpressure when queue pressure is high. */
public class RateLimitProperties {

    private double throttleMultiplier = 0.5;
    private RateLimitBucketProperties global = new RateLimitBucketProperties(1000, 100);
    private RateLimitBucketProperties perCustomer = new RateLimitBucketProperties(100, 10);
    private RateLimitBucketProperties perIp = new RateLimitBucketProperties(50, 5);

    public double getThrottleMultiplier() {
        return throttleMultiplier;
    }

    public void setThrottleMultiplier(double throttleMultiplier) {
        this.throttleMultiplier = throttleMultiplier;
    }

    public RateLimitBucketProperties getGlobal() {
        return global;
    }

    public void setGlobal(RateLimitBucketProperties global) {
        this.global = global;
    }

    public RateLimitBucketProperties getPerCustomer() {
        return perCustomer;
    }

    public void setPerCustomer(RateLimitBucketProperties perCustomer) {
        this.perCustomer = perCustomer;
    }

    public RateLimitBucketProperties getPerIp() {
        return perIp;
    }

    public void setPerIp(RateLimitBucketProperties perIp) {
        this.perIp = perIp;
    }
}
