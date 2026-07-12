package com.kholodilin.outbox.support;

import com.kholodilin.outbox.publisher.KafkaBatchPublisher;
import org.mockito.Mockito;
import org.springframework.boot.test.context.TestConfiguration;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Primary;

/** Replaces the real Kafka publisher in integration tests; unit tests cover Kafka wire format. */
@TestConfiguration
public class KafkaPublisherMockTestConfig {

    @Bean
    @Primary
    KafkaBatchPublisher kafkaBatchPublisher() {
        return Mockito.mock(KafkaBatchPublisher.class);
    }
}
