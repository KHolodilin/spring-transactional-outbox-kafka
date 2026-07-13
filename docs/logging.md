# Centralized Logging with OpenSearch

> Spec: [OpenSearch Logging Technical Specification](spring-transactional-outbox-kafka-OpenSearch-Logging-Spec.md)

## Architecture

```text
order-service / notification-stub
  ├─ ConsoleAppender  → stdout (human-readable)
  └─ JSON RollingFile → ./logs/<service>/app.json
            │
            ▼
       Fluent Bit (docker)
            │
            ▼
       OpenSearch (:9200)
            │
            ▼
   OpenSearch Dashboards (:5601)
```

Micrometer Tracing (`traceId`, `spanId`) is reused — no second tracing stack.

## Quick start

### 1. Start infrastructure

```bash
docker compose up -d
```

Includes OpenSearch (`9200`), OpenSearch Dashboards (`5601`), and Fluent Bit.

Apply the index template (once):

```bash
curl -X PUT "http://localhost:9200/_index_template/spring-outbox-logs" \
  -H "Content-Type: application/json" \
  -d @monitoring/opensearch/index-template.json
```

### 2. Run services (host, `dev` profile)

```bash
mvn -pl order-service spring-boot:run -Dspring-boot.run.profiles=dev
mvn -pl notification-stub spring-boot:run -Dspring-boot.run.profiles=dev
```

JSON logs are written to:

```text
./logs/order-service/app.json
./logs/notification-stub/app.json
```

Fluent Bit tails `./logs` (mounted as `/var/log/app` in the container).

### 3. Generate traffic

```bash
curl -X POST http://localhost:8080/api/v1/orders \
  -H "Content-Type: application/json" \
  -H "Idempotency-Key: $(uuidgen)" \
  -d '{"customerId":42,"items":[{"productId":"sku-1","quantity":1,"price":10.5}],"correlationId":"log-demo-1"}'
```

### 4. OpenSearch Dashboards

1. Open http://localhost:5601
2. **Stack Management → Index Patterns** → create `spring-outbox-logs-local-*`, time field `@timestamp`
3. Import saved objects: **Stack Management → Saved Objects → Import** → `monitoring/opensearch-dashboards/saved-objects.ndjson`
4. Open dashboard **Transactional Outbox Overview**

## Saved searches

| Query | Purpose |
|-------|---------|
| `trace.id:"<id>"` | End-to-end trace |
| `correlationId:"<id>"` | Business correlation |
| `order.id:781` | Single order |
| `outbox.id:1542` | Outbox event |
| `event.action:"outbox.publish.failed"` | Publish failures |
| `service.name:"order-service"` | Producer logs |
| `service.name:"notification-stub"` | Consumer logs |
| `log.level:"ERROR"` | Errors |

## Structured fields

Common JSON fields: `@timestamp`, `log.level`, `message`, `service.name`, `instance.id`, `trace.id`, `span.id`, `correlationId`, `customerId`, `idempotencyKey`, `event.action`.

Invariant: `locked_by == instance.id == app.instance-id`.

## Troubleshooting

| Symptom | Check |
|---------|-------|
| No JSON files | `app.logging.json.path` (default `./logs`), not running with `test` profile |
| Fluent Bit idle | `docker compose logs fluent-bit`, ensure `./logs/<service>/app.json` exists |
| No indices in OpenSearch | `curl http://localhost:9200/_cat/indices?v`, index template applied |
| Empty Dashboards | Index pattern `spring-outbox-logs-local-*`, widen time range |

## Security

Never log passwords, JWT, Authorization headers, secrets, or full Kafka payloads. `idempotencyKey` is a technical UUID identifier.
