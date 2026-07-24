package com.kholodilin.outbox.publisher;

import com.kholodilin.outbox.config.AppProperties;
import com.kholodilin.outbox.config.KafkaProperties;
import com.kholodilin.outbox.events.EventConstants;
import com.kholodilin.outbox.events.EventEnvelope;
import com.kholodilin.outbox.tracing.TraceContextSupport;
import io.micrometer.tracing.Span;
import io.micrometer.tracing.TraceContext;
import io.micrometer.tracing.Tracer;
import io.micrometer.tracing.propagation.Propagator;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.header.Header;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.mockito.junit.jupiter.MockitoSettings;
import org.mockito.quality.Strictness;
import org.springframework.beans.factory.ObjectProvider;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;

import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
@MockitoSettings(strictness = Strictness.LENIENT)
class KafkaBatchPublisherTest {

    private static final String TRACE_PARENT = "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01";

    @Mock
    private KafkaTemplate<String, Object> kafkaTemplate;

    @Mock
    private Tracer tracer;

    @Mock
    private Propagator propagator;

    @Mock
    private ObjectProvider<Tracer> tracerProvider;

    @Mock
    private ObjectProvider<Propagator> propagatorProvider;

    @Mock
    private Span nextSpan;

    @Mock
    private Span childSpan;

    @Mock
    private Span.Builder spanBuilder;

    @Mock
    private Tracer.SpanInScope spanInScope;

    @Mock
    private TraceContext traceContext;

    private TraceContextSupport traceContextSupport;
    private KafkaBatchPublisher publisher;

    @BeforeEach
    void setUp() {
        when(tracerProvider.getIfAvailable()).thenReturn(tracer);
        when(propagatorProvider.getIfAvailable()).thenReturn(propagator);
        when(tracer.nextSpan()).thenReturn(nextSpan);
        when(nextSpan.name(any())).thenReturn(nextSpan);
        when(nextSpan.start()).thenReturn(childSpan);
        when(propagator.extract(any(), any())).thenReturn(spanBuilder);
        when(spanBuilder.name(any())).thenReturn(spanBuilder);
        when(spanBuilder.start()).thenReturn(childSpan);
        when(tracer.withSpan(childSpan)).thenReturn(spanInScope);
        when(tracer.currentSpan()).thenReturn(childSpan);
        when(childSpan.context()).thenReturn(traceContext);
        traceContextSupport = new TraceContextSupport(tracerProvider, propagatorProvider);
        AppProperties properties = new AppProperties();
        KafkaProperties kafka = new KafkaProperties();
        kafka.setTopic(EventConstants.TOPIC_ORDERS);
        properties.setKafka(kafka);
        publisher = new KafkaBatchPublisher(
                kafkaTemplate,
                properties,
                traceContextSupport,
                tracerProvider,
                propagatorProvider
        );

        when(kafkaTemplate.send(any(ProducerRecord.class)))
                .thenReturn(CompletableFuture.completedFuture(new SendResult<>(null, null)));

        doAnswer(invocation -> {
            Map<String, String> map = invocation.getArgument(1);
            map.put("traceparent", TRACE_PARENT);
            return null;
        }).when(propagator).inject(eq(traceContext), any(), any());
    }

    @Test
    void usesCustomerIdAsPartitionKey() {
        EventEnvelope envelope = sampleEnvelope(null);

        publisher.publish(List.of(envelope));

        ArgumentCaptor<ProducerRecord<String, Object>> captor = ArgumentCaptor.forClass(ProducerRecord.class);
        org.mockito.Mockito.verify(kafkaTemplate).send(captor.capture());
        assertThat(captor.getValue().topic()).isEqualTo(EventConstants.TOPIC_ORDERS);
        assertThat(captor.getValue().key()).isEqualTo("42");
        assertThat(captor.getValue().value()).isEqualTo(envelope);
    }

    @Test
    void addsTraceparentHeaderWhenSpanIsActive() {
        EventEnvelope envelope = sampleEnvelope(TRACE_PARENT);

        publisher.publish(List.of(envelope));

        ArgumentCaptor<ProducerRecord<String, Object>> captor = ArgumentCaptor.forClass(ProducerRecord.class);
        org.mockito.Mockito.verify(kafkaTemplate).send(captor.capture());
        Header header = captor.getValue().headers().lastHeader(EventConstants.HEADER_TRACEPARENT);
        assertThat(header).isNotNull();
        assertThat(new String(header.value(), StandardCharsets.UTF_8)).isEqualTo(TRACE_PARENT);
    }

    @Test
    void addsCorrelationIdHeaderWhenPresent() {
        EventEnvelope envelope = new EventEnvelope(
                1L,
                10L,
                42L,
                EventConstants.EVENT_TYPE_ORDER_CREATED,
                Map.of("orderId", 10),
                "corr-99",
                Instant.parse("2026-07-12T10:00:00Z"),
                null,
                null
        );

        publisher.publish(List.of(envelope));

        ArgumentCaptor<ProducerRecord<String, Object>> captor = ArgumentCaptor.forClass(ProducerRecord.class);
        org.mockito.Mockito.verify(kafkaTemplate).send(captor.capture());
        Header header = captor.getValue().headers().lastHeader(EventConstants.HEADER_CORRELATION_ID);
        assertThat(header).isNotNull();
        assertThat(new String(header.value(), StandardCharsets.UTF_8)).isEqualTo("corr-99");
    }

    @Test
    void addsTracestateHeaderWhenPropagatorInjectsIt() {
        doAnswer(invocation -> {
            Map<String, String> map = invocation.getArgument(1);
            map.put("traceparent", TRACE_PARENT);
            map.put("tracestate", "vendor=state");
            return null;
        }).when(propagator).inject(eq(traceContext), any(), any());
        EventEnvelope envelope = sampleEnvelope(TRACE_PARENT);

        publisher.publish(List.of(envelope));

        ArgumentCaptor<ProducerRecord<String, Object>> captor = ArgumentCaptor.forClass(ProducerRecord.class);
        org.mockito.Mockito.verify(kafkaTemplate).send(captor.capture());
        Header header = captor.getValue().headers().lastHeader(EventConstants.HEADER_TRACESTATE);
        assertThat(header).isNotNull();
        assertThat(new String(header.value(), StandardCharsets.UTF_8)).isEqualTo("vendor=state");
    }

    private EventEnvelope sampleEnvelope(String traceParent) {
        return new EventEnvelope(
                1L,
                10L,
                42L,
                EventConstants.EVENT_TYPE_ORDER_CREATED,
                Map.of("orderId", 10),
                null,
                Instant.parse("2026-07-12T10:00:00Z"),
                traceParent,
                null
        );
    }
}
