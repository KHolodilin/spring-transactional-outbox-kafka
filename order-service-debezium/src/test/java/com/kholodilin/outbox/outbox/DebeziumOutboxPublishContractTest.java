package com.kholodilin.outbox.outbox;

import org.junit.jupiter.api.Test;
import tools.jackson.databind.JsonNode;
import tools.jackson.databind.json.JsonMapper;

import java.nio.file.Files;
import java.nio.file.Path;

import static org.assertj.core.api.Assertions.assertThat;

class DebeziumOutboxPublishContractTest {

    private static final Path REPO_ROOT = Path.of("").toAbsolutePath().getParent();

    @Test
    void connectorPublishesDomainPayloadWithMessageHeaders() throws Exception {
        JsonNode connector = JsonMapper.builder().build().readTree(
                Files.readString(REPO_ROOT.resolve("docker/debezium/outbox-connector.json"))
        );
        JsonNode config = connector.get("config");

        assertThat(connector.get("name").asString()).isEqualTo("outbox-events");
        assertThat(config.get("database.dbname").asString()).isEqualTo("outbox_debezium");
        assertThat(config.get("database.user").asString()).isEqualTo("outbox");
        assertThat(config.get("plugin.name").asString()).isEqualTo("pgoutput");
        assertThat(config.get("table.include.list").asString()).isEqualTo("public.outbox_events");
        assertThat(config.get("snapshot.mode").asString()).isEqualTo("no_data");
        assertThat(config.get("tombstones.on.delete").asString()).isEqualTo("false");
        assertThat(config.get("publish.via.partition.root").asString()).isEqualTo("true");
        assertThat(config.get("extended.headers.enabled").asString()).isEqualTo("false");
        assertThat(config.get("publication.autocreate.mode").asString()).isEqualTo("filtered");
        assertThat(config.get("key.converter").asString()).isEqualTo("org.apache.kafka.connect.storage.StringConverter");
        assertThat(config.get("value.converter").asString()).isEqualTo("org.apache.kafka.connect.json.JsonConverter");
        assertThat(config.get("value.converter.schemas.enable").asString()).isEqualTo("false");
        assertThat(config.get("producer.override.interceptor.classes").asString())
                .isEqualTo("com.kholodilin.outbox.connect.ConnectTraceProducerInterceptor");
        assertThat(config.get("transforms").asString()).isEqualTo("outbox,messageType,messageVersion");
        assertThat(config.get("transforms.outbox.type").asString())
                .isEqualTo("io.debezium.transforms.outbox.EventRouter");
        assertThat(config.get("transforms.outbox.table.field.event.payload").asString()).isEqualTo("payload");
        assertThat(config.get("transforms.outbox.table.expand.json.payload").asString()).isEqualTo("false");
        assertThat(config.get("transforms.outbox.route.topic.replacement").asString()).isEqualTo("orders.events");
        assertThat(config.get("transforms.outbox.table.op.invalid.behavior").asString()).isEqualTo("warn");
        assertThat(config.get("transforms.outbox.table.fields.additional.placement").asString())
                .isEqualTo("id:header:eventId,aggregate_id:header:orderId,partition_key:header:customerId,event_type:header:eventType,trace_parent:header:traceparent");
        assertThat(config.get("transforms.messageType.value.literal").asString()).isEqualTo("order-payload");
        assertThat(config.get("transforms.messageVersion.value.literal").asString()).isEqualTo("1");
        assertThat(config.toString()).doesNotContain("envelope:");

        JsonNode configOnly = JsonMapper.builder().build().readTree(
                Files.readString(REPO_ROOT.resolve("docker/debezium/outbox-connector-config.json"))
        );
        assertThat(configOnly).isEqualTo(config);
    }

    @Test
    void composeStartsOrderServiceConnectAndTheStub() throws Exception {
        String compose = Files.readString(REPO_ROOT.resolve("docker/compose.debezium.yml"));

        assertThat(compose).contains("name: outbox-debezium");
        assertThat(compose).contains("POSTGRES_DB: outbox_debezium");
        assertThat(compose).contains("\"5435:5432\"");
        assertThat(compose).contains("wal_level=logical");
        assertThat(compose).contains("\"19095:9092\"");
        assertThat(compose).contains("KAFKA_AUTO_CREATE_TOPICS_ENABLE: \"false\"");
        assertThat(compose).contains("--topic orders.events");
        assertThat(compose).contains("--partitions 10");
        assertThat(compose).contains("--topic connect-configs");
        assertThat(compose).contains("--topic connect-offsets");
        assertThat(compose).contains("--topic connect-status");
        assertThat(compose).contains("\"8096:8086\"");
        assertThat(compose).contains("\"8097:8081\"");
        assertThat(compose).contains("notification-stub:");
        assertThat(compose).contains("DB_NAME: outbox_debezium");
        assertThat(compose).contains("quay.io/debezium/connect:3.2.4.Final");
        assertThat(compose).contains("\"8087:8083\"");
        assertThat(compose).contains("./debezium/outbox-connector.json:/connector.json:ro");
        assertThat(compose).contains("./debezium/outbox-connector-config.json:/connector-config.json:ro");
        assertThat(compose).contains("OTEL_EXPORTER_OTLP_TRACES_ENDPOINT: http://tempo:4318/v1/traces");
        assertThat(compose).contains("OTEL_SERVICE_NAME: debezium-connect");
        assertThat(compose).contains("profiles: [\"observability\"]");
        assertThat(compose).contains("\"3003:3000\"");
        assertThat(compose).contains("\"5604:5601\"");
        assertThat(compose).contains("orders-technical-dashboard.json");
        assertThat(compose).contains("outbox-dashboard.json");
        assertThat(compose).contains("postgres-dashboard.json");
        assertThat(compose).contains("notification-stub-dashboard.json");
        assertThat(compose).contains("tracing-dashboard.json");
        assertThat(compose).contains("prometheus-debezium.yml");
        assertThat(compose).doesNotContain("ghcr.io");
    }
}
