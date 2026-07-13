package com.kholodilin.outbox.health;

import com.kholodilin.outbox.config.AppProperties;
import com.kholodilin.outbox.config.HealthProperties;
import com.kholodilin.outbox.config.KafkaProperties;
import org.apache.kafka.clients.producer.Producer;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.boot.health.contributor.Health;
import org.springframework.boot.health.contributor.Status;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.core.ProducerFactory;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class KafkaHealthIndicatorTest {

    @Mock
    private KafkaTemplate<String, Object> kafkaTemplate;

    @Mock
    private ProducerFactory<String, Object> producerFactory;

    @Test
    void reportsUpWhenTopicPartitionsExist() {
        @SuppressWarnings("unchecked")
        Producer<String, Object> producer = mock(Producer.class);
        when(kafkaTemplate.getProducerFactory()).thenReturn(producerFactory);
        when(producerFactory.createProducer()).thenReturn(producer);
        when(producer.partitionsFor("orders")).thenReturn(List.of());

        KafkaHealthIndicator indicator = new KafkaHealthIndicator(kafkaTemplate, AppProperties.builder()
                .kafka(KafkaProperties.builder().topic("orders").build())
                .build());

        Health health = indicator.health();

        assertThat(health.getStatus()).isEqualTo(Status.UP);
    }

    @Test
    void reportsDownWhenKafkaUnreachable() {
        when(kafkaTemplate.getProducerFactory()).thenReturn(producerFactory);
        when(producerFactory.createProducer()).thenThrow(new RuntimeException("broker down"));

        KafkaHealthIndicator indicator = new KafkaHealthIndicator(kafkaTemplate, AppProperties.builder()
                .kafka(KafkaProperties.builder().topic("orders").build())
                .build());

        Health health = indicator.health();

        assertThat(health.getStatus()).isEqualTo(Status.DOWN);
    }
}
