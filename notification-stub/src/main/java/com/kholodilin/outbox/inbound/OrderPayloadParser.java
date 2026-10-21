package com.kholodilin.outbox.inbound;

import com.kholodilin.outbox.events.EventConstants;
import com.kholodilin.outbox.events.EventEnvelope;
import org.apache.kafka.common.errors.SerializationException;
import org.apache.kafka.common.header.Headers;
import tools.jackson.core.JacksonException;
import tools.jackson.core.type.TypeReference;
import tools.jackson.databind.JsonNode;
import tools.jackson.databind.json.JsonMapper;

import java.util.Map;

/**
 * Builds one {@link EventEnvelope} from a Debezium {@code order-payload} version {@code 1} record.
 * <p>
 * {@code eventId} and {@code eventType} come from headers. {@code orderId}, {@code customerId},
 * and {@code correlationId} come from the domain JSON. {@code occurredAt} stays empty so a
 * redelivery keeps the same idempotency fingerprint. {@code payload} is that same JSON object.
 * Kafka Connect's JSON converter publishes the payload column as a JSON string, so a textual
 * value is parsed once more.
 */
public final class OrderPayloadParser implements InboundMessageParser {

    private static final String HEADER_EVENT_TYPE = "eventType";

    private static final TypeReference<Map<String, Object>> PAYLOAD_TYPE = new TypeReference<>() {
    };

    private final JsonMapper objectMapper;

    /**
     * @param objectMapper mapper from {@link com.kholodilin.outbox.config.KafkaConsumerConfig#kafkaObjectMapper()}
     */
    public OrderPayloadParser(JsonMapper objectMapper) {
        this.objectMapper = objectMapper;
    }

    @Override
    public EventEnvelope parse(JsonNode body, Headers headers) {
        body = domainObject(body);
        long eventId = eventId(headers);
        String eventType = HeaderCodec.text(headers, HEADER_EVENT_TYPE);
        Long orderId = jsonLong(body, EventConstants.HEADER_ORDER_ID);
        Long customerId = jsonLong(body, EventConstants.HEADER_CUSTOMER_ID);
        String correlationId = jsonText(body, EventConstants.HEADER_CORRELATION_ID);
        Map<String, Object> payload = payload(body);
        return new EventEnvelope(eventId, orderId, customerId, eventType, payload, correlationId, null, null);
    }

    private JsonNode domainObject(JsonNode body) {
        if (body != null && body.isString()) {
            try {
                body = objectMapper.readTree(body.stringValue());
            } catch (JacksonException ex) {
                throw new SerializationException("order-payload value must be a JSON object", ex);
            }
        }
        if (body == null || !body.isObject()) {
            throw new SerializationException("order-payload value must be a JSON object");
        }
        return body;
    }

    private static long eventId(Headers headers) {
        String rawEventId = HeaderCodec.text(headers, EventConstants.HEADER_EVENT_ID);
        if (rawEventId == null || rawEventId.isBlank()) {
            throw new SerializationException("order-payload record is missing the eventId header");
        }
        try {
            return Long.parseLong(rawEventId);
        } catch (NumberFormatException ex) {
            throw new SerializationException("order-payload eventId header is not a number", ex);
        }
    }

    private static Long jsonLong(JsonNode body, String field) {
        JsonNode node = body.get(field);
        if (node == null || node.isNull()) {
            return null;
        }
        if (!node.isNumber()) {
            throw new SerializationException("order-payload field " + field + " must be a number");
        }
        return node.longValue();
    }

    private static String jsonText(JsonNode body, String field) {
        JsonNode node = body.get(field);
        if (node == null || node.isNull()) {
            return null;
        }
        if (!node.isString()) {
            throw new SerializationException("order-payload field " + field + " must be a string");
        }
        return node.stringValue();
    }

    private Map<String, Object> payload(JsonNode body) {
        return objectMapper.treeToValue(body, PAYLOAD_TYPE);
    }
}
