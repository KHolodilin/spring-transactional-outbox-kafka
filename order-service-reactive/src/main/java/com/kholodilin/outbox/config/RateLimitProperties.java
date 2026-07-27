package com.kholodilin.outbox.config;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;

@Getter
@Setter
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class RateLimitProperties {

    @Builder.Default
    private double throttleMultiplier = 0.5;

    @Builder.Default
    private RateLimitBucketProperties global = RateLimitBucketProperties.builder().capacity(1000).refillPerSecond(100).build();

    @Builder.Default
    private RateLimitBucketProperties perCustomer = RateLimitBucketProperties.builder().capacity(100).refillPerSecond(10).build();

    @Builder.Default
    private RateLimitBucketProperties perIp = RateLimitBucketProperties.builder().capacity(50).refillPerSecond(5).build();
}
