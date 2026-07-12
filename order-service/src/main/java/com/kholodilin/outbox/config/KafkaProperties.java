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

    @Builder.Default
    private String topic = "orders.events";
}
