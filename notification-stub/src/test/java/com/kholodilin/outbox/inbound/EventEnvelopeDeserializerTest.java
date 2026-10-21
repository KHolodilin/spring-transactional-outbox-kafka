package com.kholodilin.outbox.inbound;

import com.kholodilin.outbox.events.EventConstants;
import com.kholodilin.outbox.events.EventEnvelope;
import org.apache.kafka.common.errors.SerializationException;
import org.apache.kafka.common.header.Headers;
import org.apache.kafka.common.header.internals.RecordHeader;
import org.apache.kafka.common.header.internals.RecordHeaders;
import org.junit.jupiter.api.Test;
import tools.jackson.databind.DeserializationFeature;
import tools.jackson.databind.JsonNode;
import tools.jackson.databind.json.JsonMapper;

import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

class EventEnvelopeDeserializerTest {

    private static final String TOPIC = "orders.events";

    private final JsonMapper objectMapper = JsonMapper.builder()
            .configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false)
            .build();
    private final OrderPayloadParser orderPayloadParser = new OrderPayloadParser(objectMapper);
    private final EventEnvelopeDeserializer deserializer = new EventEnvelopeDeserializer(
            objectMapper,
            new InboundMessageParserRegistry(new ServletEnvelopeParser(objectMapper), orderPayloadParser)
    );

    @Test
    void noMessageTypeUsesServletParser() {
        EventEnvelope envelope = deserializer.deserialize(TOPIC, new RecordHeaders(), servletJson().getBytes(StandardCharsets.UTF_8));

        assertThat(envelope.eventId()).isEqualTo(1L);
        assertThat(envelope.orderId()).isEqualTo(10L);
        assertThat(envelope.customerId()).isEqualTo(20L);
        assertThat(envelope.eventType()).isEqualTo("OrderCreated");
        assertThat(envelope.correlationId()).isEqualTo("corr");
        assertThat(envelope.occurredAt()).isEqualTo(Instant.parse("2026-07-12T10:15:30Z"));
        assertThat(envelope.payload()).containsEntry("orderId", 10);
        assertThat(envelope.traceParent()).isNull();
    }

    @Test
    void messageVersionWithoutMessageTypeStillUsesServletParser() {
        Headers headers = new RecordHeaders()
                .add(new RecordHeader(InboundMessageParserRegistry.HEADER_MESSAGE_VERSION, utf8("1")));

        EventEnvelope envelope = deserializer.deserialize(TOPIC, headers, servletJson().getBytes(StandardCharsets.UTF_8));

        assertThat(envelope.eventId()).isEqualTo(1L);
        assertThat(envelope.eventType()).isEqualTo("OrderCreated");
    }

    @Test
    void headerLessDeserializeUsesServletParser() {
        EventEnvelope envelope = deserializer.deserialize(TOPIC, servletJson().getBytes(StandardCharsets.UTF_8));

        assertThat(envelope.eventId()).isEqualTo(1L);
        assertThat(envelope.occurredAt()).isEqualTo(Instant.parse("2026-07-12T10:15:30Z"));
    }

    @Test
    void nullHeadersUseServletParser() {
        EventEnvelope envelope = deserializer.deserialize(
                TOPIC,
                null,
                servletJson().getBytes(StandardCharsets.UTF_8)
        );

        assertThat(envelope.eventId()).isEqualTo(1L);
    }

    @Test
    void nullMessageTypeValueUsesServletParser() {
        Headers headers = mock(Headers.class);
        org.apache.kafka.common.header.Header header = mock(org.apache.kafka.common.header.Header.class);
        when(headers.lastHeader(InboundMessageParserRegistry.HEADER_MESSAGE_TYPE)).thenReturn(header);
        when(header.value()).thenReturn(null);

        EventEnvelope envelope = deserializer.deserialize(TOPIC, headers, servletJson().getBytes(StandardCharsets.UTF_8));

        assertThat(envelope.eventId()).isEqualTo(1L);
    }

    @Test
    void orderPayloadBuildsEnvelopeFromHeadersAndJson() {
        byte[] body = orderPayloadJson().getBytes(StandardCharsets.UTF_8);

        EventEnvelope first = deserializer.deserialize(TOPIC, orderPayloadHeaders(), body);
        EventEnvelope second = deserializer.deserialize(TOPIC, orderPayloadHeaders(), body);

        assertThat(first).isEqualTo(second);
        assertThat(first.eventId()).isEqualTo(15L);
        assertThat(first.eventType()).isEqualTo("OrderCreated");
        assertThat(first.orderId()).isEqualTo(10L);
        assertThat(first.customerId()).isEqualTo(20L);
        assertThat(first.correlationId()).isEqualTo("corr");
        assertThat(first.occurredAt()).isNull();
        assertThat(first.traceParent()).isNull();
        assertThat(first.payload()).containsEntry("orderId", 10);
        assertThat(first.payload()).containsKey("items");
        @SuppressWarnings("unchecked")
        List<Map<String, Object>> items = (List<Map<String, Object>>) first.payload().get("items");
        assertThat(items).singleElement().extracting(item -> item.get("productId")).isEqualTo("sku-1");
    }

    @Test
    void lastOrderPayloadHeaderWins() {
        Headers headers = new RecordHeaders()
                .add(new RecordHeader(InboundMessageParserRegistry.HEADER_MESSAGE_TYPE, utf8("other")))
                .add(new RecordHeader(InboundMessageParserRegistry.HEADER_MESSAGE_TYPE, utf8("order-payload")))
                .add(new RecordHeader(InboundMessageParserRegistry.HEADER_MESSAGE_VERSION, utf8("1")))
                .add(new RecordHeader(EventConstants.HEADER_EVENT_ID, utf8("9")))
                .add(new RecordHeader("eventType", utf8("OrderCreated")));

        EventEnvelope envelope = deserializer.deserialize(
                TOPIC,
                headers,
                "{\"orderId\":4,\"customerId\":5}".getBytes(StandardCharsets.UTF_8)
        );

        assertThat(envelope.eventId()).isEqualTo(9L);
        assertThat(envelope.orderId()).isEqualTo(4L);
    }

    @Test
    void orderPayloadOmitsAbsentJsonFields() {
        Headers headers = new RecordHeaders()
                .add(new RecordHeader(InboundMessageParserRegistry.HEADER_MESSAGE_TYPE, utf8("order-payload")))
                .add(new RecordHeader(InboundMessageParserRegistry.HEADER_MESSAGE_VERSION, utf8("1")))
                .add(new RecordHeader(EventConstants.HEADER_EVENT_ID, utf8("8")));

        EventEnvelope envelope = deserializer.deserialize(TOPIC, headers, "{}".getBytes(StandardCharsets.UTF_8));

        assertThat(envelope.orderId()).isNull();
        assertThat(envelope.customerId()).isNull();
        assertThat(envelope.correlationId()).isNull();
        assertThat(envelope.payload()).isEmpty();
    }

    @Test
    void orderPayloadAllowsMissingOptionalFields() {
        Headers headers = new RecordHeaders()
                .add(new RecordHeader(InboundMessageParserRegistry.HEADER_MESSAGE_TYPE, utf8("order-payload")))
                .add(new RecordHeader(InboundMessageParserRegistry.HEADER_MESSAGE_VERSION, utf8("1")))
                .add(new RecordHeader(EventConstants.HEADER_EVENT_ID, utf8("8")));

        EventEnvelope envelope = deserializer.deserialize(
                TOPIC,
                headers,
                "{\"orderId\":null,\"customerId\":null,\"correlationId\":null}".getBytes(StandardCharsets.UTF_8)
        );

        assertThat(envelope.eventId()).isEqualTo(8L);
        assertThat(envelope.eventType()).isNull();
        assertThat(envelope.orderId()).isNull();
        assertThat(envelope.customerId()).isNull();
        assertThat(envelope.correlationId()).isNull();
        assertThat(envelope.occurredAt()).isNull();
    }

    @Test
    void orderPayloadReadsLargeIdsAsLong() {
        Headers headers = new RecordHeaders()
                .add(new RecordHeader(InboundMessageParserRegistry.HEADER_MESSAGE_TYPE, utf8("order-payload")))
                .add(new RecordHeader(InboundMessageParserRegistry.HEADER_MESSAGE_VERSION, utf8("1")))
                .add(new RecordHeader(EventConstants.HEADER_EVENT_ID, utf8("3000000000")))
                .add(new RecordHeader("eventType", utf8("OrderCreated")));

        EventEnvelope envelope = deserializer.deserialize(
                TOPIC,
                headers,
                "{\"orderId\":3000000001,\"customerId\":3000000002}".getBytes(StandardCharsets.UTF_8)
        );

        assertThat(envelope.eventId()).isEqualTo(3_000_000_000L);
        assertThat(envelope.orderId()).isEqualTo(3_000_000_001L);
        assertThat(envelope.customerId()).isEqualTo(3_000_000_002L);
    }

    @Test
    void unknownMessageTypeFailsWithoutUsingServletParser() {
        Headers headers = new RecordHeaders()
                .add(new RecordHeader(InboundMessageParserRegistry.HEADER_MESSAGE_TYPE, utf8("legacy")))
                .add(new RecordHeader(InboundMessageParserRegistry.HEADER_MESSAGE_VERSION, utf8("1")));

        assertThatThrownBy(() -> deserializer.deserialize(TOPIC, headers, servletJson().getBytes(StandardCharsets.UTF_8)))
                .isInstanceOf(SerializationException.class)
                .hasMessageContaining("messageType=legacy")
                .hasMessageContaining("messageVersion=1");
    }

    @Test
    void unknownMessageVersionFails() {
        Headers headers = orderPayloadHeaders();
        headers.remove(InboundMessageParserRegistry.HEADER_MESSAGE_VERSION);
        headers.add(new RecordHeader(InboundMessageParserRegistry.HEADER_MESSAGE_VERSION, utf8("2")));

        assertThatThrownBy(() -> deserializer.deserialize(TOPIC, headers, orderPayloadJson().getBytes(StandardCharsets.UTF_8)))
                .isInstanceOf(SerializationException.class)
                .hasMessageContaining("messageType=order-payload")
                .hasMessageContaining("messageVersion=2");
    }

    @Test
    void orderPayloadWithoutVersionFails() {
        Headers headers = new RecordHeaders()
                .add(new RecordHeader(InboundMessageParserRegistry.HEADER_MESSAGE_TYPE, utf8("order-payload")))
                .add(new RecordHeader(EventConstants.HEADER_EVENT_ID, utf8("15")));

        assertThatThrownBy(() -> deserializer.deserialize(TOPIC, headers, orderPayloadJson().getBytes(StandardCharsets.UTF_8)))
                .isInstanceOf(SerializationException.class)
                .hasMessageContaining("messageVersion=null");
    }

    @Test
    void emptyMessageTypeFails() {
        Headers headers = new RecordHeaders()
                .add(new RecordHeader(InboundMessageParserRegistry.HEADER_MESSAGE_TYPE, utf8("")))
                .add(new RecordHeader(InboundMessageParserRegistry.HEADER_MESSAGE_VERSION, utf8("1")));

        assertThatThrownBy(() -> deserializer.deserialize(TOPIC, headers, orderPayloadJson().getBytes(StandardCharsets.UTF_8)))
                .isInstanceOf(SerializationException.class)
                .hasMessageContaining("messageType=");
    }

    @Test
    void missingEventIdFails() {
        Headers headers = new RecordHeaders()
                .add(new RecordHeader(InboundMessageParserRegistry.HEADER_MESSAGE_TYPE, utf8("order-payload")))
                .add(new RecordHeader(InboundMessageParserRegistry.HEADER_MESSAGE_VERSION, utf8("1")));

        assertThatThrownBy(() -> deserializer.deserialize(TOPIC, headers, orderPayloadJson().getBytes(StandardCharsets.UTF_8)))
                .isInstanceOf(SerializationException.class)
                .hasMessageContaining("eventId");
    }

    @Test
    void blankEventIdFails() {
        Headers headers = new RecordHeaders()
                .add(new RecordHeader(InboundMessageParserRegistry.HEADER_MESSAGE_TYPE, utf8("order-payload")))
                .add(new RecordHeader(InboundMessageParserRegistry.HEADER_MESSAGE_VERSION, utf8("1")))
                .add(new RecordHeader(EventConstants.HEADER_EVENT_ID, utf8("  ")));

        assertThatThrownBy(() -> deserializer.deserialize(TOPIC, headers, orderPayloadJson().getBytes(StandardCharsets.UTF_8)))
                .isInstanceOf(SerializationException.class)
                .hasMessageContaining("eventId");
    }

    @Test
    void nonNumericEventIdFails() {
        Headers headers = new RecordHeaders()
                .add(new RecordHeader(InboundMessageParserRegistry.HEADER_MESSAGE_TYPE, utf8("order-payload")))
                .add(new RecordHeader(InboundMessageParserRegistry.HEADER_MESSAGE_VERSION, utf8("1")))
                .add(new RecordHeader(EventConstants.HEADER_EVENT_ID, utf8("abc")));

        assertThatThrownBy(() -> deserializer.deserialize(TOPIC, headers, orderPayloadJson().getBytes(StandardCharsets.UTF_8)))
                .isInstanceOf(SerializationException.class)
                .hasMessageContaining("not a number")
                .hasCauseInstanceOf(NumberFormatException.class);
    }

    @Test
    void orderPayloadAcceptsJsonStringPublishedByConnect() {
        byte[] value = "\"{\\\"orderId\\\":4,\\\"customerId\\\":5,\\\"correlationId\\\":\\\"corr\\\"}\""
                .getBytes(StandardCharsets.UTF_8);

        EventEnvelope envelope = deserializer.deserialize(TOPIC, orderPayloadHeaders(), value);

        assertThat(envelope.orderId()).isEqualTo(4L);
        assertThat(envelope.customerId()).isEqualTo(5L);
        assertThat(envelope.correlationId()).isEqualTo("corr");
        assertThat(envelope.payload()).containsEntry("orderId", 4);
    }

    @Test
    void orderPayloadStringThatIsNotJsonFails() {
        assertThatThrownBy(() -> deserializer.deserialize(
                TOPIC,
                orderPayloadHeaders(),
                "\"not-json\"".getBytes(StandardCharsets.UTF_8)
        ))
                .isInstanceOf(SerializationException.class)
                .hasMessageContaining("JSON object");
    }

    @Test
    void orderPayloadStringThatIsNotAnObjectFails() {
        assertThatThrownBy(() -> deserializer.deserialize(
                TOPIC,
                orderPayloadHeaders(),
                "\"[]\"".getBytes(StandardCharsets.UTF_8)
        ))
                .isInstanceOf(SerializationException.class)
                .hasMessageContaining("JSON object");
    }

    @Test
    void nonObjectOrderPayloadFails() {
        assertThatThrownBy(() -> deserializer.deserialize(TOPIC, orderPayloadHeaders(), "[]".getBytes(StandardCharsets.UTF_8)))
                .isInstanceOf(SerializationException.class)
                .hasMessageContaining("JSON object");
    }

    @Test
    void nullOrderPayloadBodyFails() {
        Headers headers = orderPayloadHeaders();

        assertThatThrownBy(() -> orderPayloadParser.parse(null, headers))
                .isInstanceOf(SerializationException.class)
                .hasMessageContaining("JSON object");
    }

    @Test
    void nonNumericOrderIdFails() {
        assertThatThrownBy(() -> deserializer.deserialize(
                TOPIC,
                orderPayloadHeaders(),
                "{\"orderId\":\"x\",\"customerId\":20}".getBytes(StandardCharsets.UTF_8)
        ))
                .isInstanceOf(SerializationException.class)
                .hasMessageContaining("orderId");
    }

    @Test
    void nonNumericCustomerIdFails() {
        assertThatThrownBy(() -> deserializer.deserialize(
                TOPIC,
                orderPayloadHeaders(),
                "{\"orderId\":10,\"customerId\":true}".getBytes(StandardCharsets.UTF_8)
        ))
                .isInstanceOf(SerializationException.class)
                .hasMessageContaining("customerId");
    }

    @Test
    void nonTextualCorrelationIdFails() {
        assertThatThrownBy(() -> deserializer.deserialize(
                TOPIC,
                orderPayloadHeaders(),
                "{\"orderId\":10,\"customerId\":20,\"correlationId\":7}".getBytes(StandardCharsets.UTF_8)
        ))
                .isInstanceOf(SerializationException.class)
                .hasMessageContaining("correlationId");
    }

    @Test
    void nullRecordReturnsNull() {
        assertThat(deserializer.deserialize(TOPIC, orderPayloadHeaders(), (byte[]) null)).isNull();
    }

    @Test
    void invalidJsonFails() {
        assertThatThrownBy(() -> deserializer.deserialize(TOPIC, new RecordHeaders(), "{".getBytes(StandardCharsets.UTF_8)))
                .isInstanceOf(SerializationException.class)
                .hasMessageContaining("JSON");
    }

    @Test
    void servletArrayBodyFails() {
        assertThatThrownBy(() -> deserializer.deserialize(TOPIC, new RecordHeaders(), "[]".getBytes(StandardCharsets.UTF_8)))
                .isInstanceOf(SerializationException.class)
                .hasMessageContaining("EventEnvelope");
    }

    @Test
    void servletJsonNullReturnsNullEnvelope() {
        JsonNode body = objectMapper.readTree("null");

        EventEnvelope envelope = new ServletEnvelopeParser(objectMapper).parse(body, new RecordHeaders());

        assertThat(envelope).isNull();
    }

    private static Headers orderPayloadHeaders() {
        return new RecordHeaders()
                .add(new RecordHeader(InboundMessageParserRegistry.HEADER_MESSAGE_TYPE, utf8("order-payload")))
                .add(new RecordHeader(InboundMessageParserRegistry.HEADER_MESSAGE_VERSION, utf8("1")))
                .add(new RecordHeader(EventConstants.HEADER_EVENT_ID, utf8("15")))
                .add(new RecordHeader("eventType", utf8("OrderCreated")))
                .add(new RecordHeader(EventConstants.HEADER_ORDER_ID, utf8("999")));
    }

    private static String servletJson() {
        return """
                {
                  "eventId": 1,
                  "orderId": 10,
                  "customerId": 20,
                  "eventType": "OrderCreated",
                  "payload": {"orderId": 10},
                  "correlationId": "corr",
                  "occurredAt": "2026-07-12T10:15:30Z",
                  "ignored": true
                }
                """;
    }

    private static String orderPayloadJson() {
        return """
                {
                  "orderId": 10,
                  "customerId": 20,
                  "correlationId": "corr",
                  "items": [{"productId": "sku-1", "quantity": 2, "price": 10.50}]
                }
                """;
    }

    private static byte[] utf8(String value) {
        return value.getBytes(StandardCharsets.UTF_8);
    }
}
