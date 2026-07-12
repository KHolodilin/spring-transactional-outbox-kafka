package com.kholodilin.outbox.config;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;

/** Kafka topic and publisher settings. */
@Getter
@Setter
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class KafkaProperties {

    /** Target topic for outbox events published by {@code order-service}. */
    @Builder.Default
    private String topic = "orders.events";
}
