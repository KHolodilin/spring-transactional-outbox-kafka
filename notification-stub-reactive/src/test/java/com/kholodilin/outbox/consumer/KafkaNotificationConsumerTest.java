package com.kholodilin.outbox.consumer;

import com.kholodilin.outbox.config.NotificationStubProperties;
import com.kholodilin.outbox.events.EventEnvelope;
import com.kholodilin.outbox.notification.NotificationStubHandler;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.kafka.receiver.KafkaReceiver;
import reactor.kafka.receiver.ReceiverOffset;
import reactor.kafka.receiver.ReceiverRecord;
import reactor.test.StepVerifier;

import java.util.List;
import java.util.Map;

import static org.mockito.Mockito.*;

@ExtendWith(MockitoExtension.class)
class KafkaNotificationConsumerTest {

    @Mock
    private KafkaReceiver<String, EventEnvelope> kafkaReceiver;

    @Mock
    private NotificationStubHandler handler;

    private KafkaNotificationConsumer consumer;

    @BeforeEach
    void setUp() {
        consumer = new KafkaNotificationConsumer(
                kafkaReceiver,
                handler,
                new NotificationStubProperties()
        );
    }

    @Test
    void acknowledgesOnlyHighestOffsetPerPartition() {
        TestRecord p0o1 = record(0, 1L);
        TestRecord p0o3 = record(0, 3L);
        TestRecord p0o2 = record(0, 2L);

        TestRecord p1o4 = record(1, 4L);
        TestRecord p1o7 = record(1, 7L);

        List<ReceiverRecord<String, EventEnvelope>> batch =
                List.of(
                        p0o1.record(),
                        p0o3.record(),
                        p0o2.record(),
                        p1o4.record(),
                        p1o7.record()
                );

        when(handler.handleBatch(batch))
                .thenReturn(Mono.empty());

        StepVerifier.create(
                        consumer.processBatch(batch)
                )
                .verifyComplete();

        verify(p0o1.offset(), never())
                .acknowledge();

        verify(p0o2.offset(), never())
                .acknowledge();

        verify(p0o3.offset())
                .acknowledge();

        verify(p1o4.offset(), never())
                .acknowledge();

        verify(p1o7.offset())
                .acknowledge();
    }

    @Test
    void startSubscribesAfterReadyAndIsIdempotent() {
        when(kafkaReceiver.receive()).thenReturn(Flux.never());

        verify(kafkaReceiver, never()).receive();

        consumer.start();
        consumer.start();

        verify(kafkaReceiver, times(1)).receive();

        consumer.stop();
    }

    @Test
    void acknowledgesMalformedNullValue() {
        TestRecord poison = nullValueRecord(8, 0L);

        consumer.acknowledgeMalformedRecord(poison.record());

        verify(poison.offset()).acknowledge();
    }

    @Test
    void doesNotAcknowledgeValidRecordInMalformedHook() {
        TestRecord valid = record(0, 1L);

        consumer.acknowledgeMalformedRecord(valid.record());

        verify(valid.offset(), never()).acknowledge();
    }

    @Test
    void technicalFailureDoesNotAcknowledge() {
        TestRecord testRecord =
                record(0, 10L);

        List<ReceiverRecord<String, EventEnvelope>> batch =
                List.of(testRecord.record());

        IllegalStateException failure =
                new IllegalStateException(
                        "database unavailable"
                );

        when(handler.handleBatch(batch))
                .thenReturn(Mono.error(failure));

        StepVerifier.create(
                        consumer.processBatch(batch)
                )
                .expectErrorMatches(
                        error -> error == failure
                )
                .verify();

        verify(testRecord.offset(), never())
                .acknowledge();
    }

    private TestRecord record(
            int partition,
            long offset
    ) {
        EventEnvelope event =
                new EventEnvelope(
                        offset,
                        2L,
                        3L,
                        "OrderCreated",
                        Map.of("version", 1),
                        "corr",
                        null,
                        null
                );

        ConsumerRecord<String, EventEnvelope> consumerRecord =
                new ConsumerRecord<>(
                        "orders.events",
                        partition,
                        offset,
                        String.valueOf(event.customerId()),
                        event
                );

        ReceiverOffset receiverOffset =
                mock(ReceiverOffset.class);

        ReceiverRecord<String, EventEnvelope> receiverRecord =
                new ReceiverRecord<>(
                        consumerRecord,
                        receiverOffset
                );

        return new TestRecord(
                receiverRecord,
                receiverOffset
        );
    }

    private TestRecord nullValueRecord(int partition, long offset) {
        ConsumerRecord<String, EventEnvelope> consumerRecord =
                new ConsumerRecord<>(
                        "orders.events",
                        partition,
                        offset,
                        "9001",
                        null
                );

        ReceiverOffset receiverOffset = mock(ReceiverOffset.class);
        ReceiverRecord<String, EventEnvelope> receiverRecord =
                new ReceiverRecord<>(consumerRecord, receiverOffset);

        return new TestRecord(receiverRecord, receiverOffset);
    }

    private record TestRecord(
            ReceiverRecord<String, EventEnvelope> record,
            ReceiverOffset offset
    ) {
    }
}