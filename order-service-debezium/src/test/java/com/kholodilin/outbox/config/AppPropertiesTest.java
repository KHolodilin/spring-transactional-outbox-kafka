package com.kholodilin.outbox.config;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

class AppPropertiesTest {

    @Test
    void builderMatchesServletDefaults() {
        AppProperties properties = AppProperties.builder().instanceId("pod-a").build();

        assertThat(properties.getInstanceId()).isEqualTo("pod-a");
        assertThat(properties.getRateLimit().getThrottleMultiplier()).isEqualTo(0.5);
        assertThat(properties.getRateLimit().getUsageThreshold()).isEqualTo(0.8);
        assertThat(properties.getRateLimit().getGlobal().getCapacity()).isEqualTo(1000);
        assertThat(properties.getRateLimit().getGlobal().getRefillPerSecond()).isEqualTo(100);
        assertThat(properties.getRateLimit().getPerCustomer().getCapacity()).isEqualTo(100);
        assertThat(properties.getRateLimit().getPerCustomer().getRefillPerSecond()).isEqualTo(10);
        assertThat(properties.getRateLimit().getPerIp().getCapacity()).isEqualTo(50);
        assertThat(properties.getRateLimit().getPerIp().getRefillPerSecond()).isEqualTo(5);
        assertThat(properties.getBulkhead().getMaxConcurrentCreates()).isEqualTo(55);
        assertThat(properties.getCdc().getSlotName()).isEqualTo("debezium_outbox");
        assertThat(properties.getCdc().getLagCapacityBytes()).isEqualTo(10_485_760L);
    }
}
