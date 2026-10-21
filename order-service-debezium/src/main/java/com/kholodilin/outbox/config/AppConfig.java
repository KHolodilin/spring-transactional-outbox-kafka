package com.kholodilin.outbox.config;

import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Configuration;

/** Enables {@code app.*} binding for the rate limit and create bulkhead. */
@Configuration
@EnableConfigurationProperties(AppProperties.class)
public class AppConfig {
}
