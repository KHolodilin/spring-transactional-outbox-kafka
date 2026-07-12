# Technical Specification
# Distributed Tracing for spring-transactional-outbox-kafka

## 1. Goal

Implement production-ready distributed tracing for the Transactional Outbox workflow.

The tracing must allow developers to follow an event from the incoming HTTP request through database persistence, Outbox, Kafka publishing and Kafka consumer processing.

The solution must integrate with Grafana Tempo using OpenTelemetry.

---

# 2. Technology Stack

## Application
- Spring Boot 4
- Micrometer Tracing
- OpenTelemetry

## Transport
- OTLP (OpenTelemetry Protocol)

## Storage
- Grafana Tempo

## Visualization
- Grafana

OpenTelemetry Collector is **not required** in the first version.

---

# 3. High-Level Architecture

```text
HTTP Request
    │
    ▼
Order Service
    │
    ├── Business Logic
    ├── PostgreSQL
    ├── Outbox
    │
    ▼
Outbox Publisher
    │
    ▼
Kafka Producer
    │
    ▼
Kafka Topic
    │
    ▼
Kafka Consumer
    │
    ▼
Business Processing
```

All operations should belong to the same distributed trace whenever possible.

---

# 4. HTTP Tracing

Every incoming HTTP request automatically creates a root trace.

Captured information:

- HTTP method
- URI
- Status code
- Duration
- Service name

HTTP request/response bodies must not be exported.

---

# 5. Database Tracing

Database operations should appear as child spans.

Example:

```text
INSERT orders
INSERT outbox
SELECT outbox batch
UPDATE outbox status
```

SQL parameters must never be exported.

---

# 6. Outbox Tracing

Create dedicated spans:

```text
outbox.save
outbox.batch.fetch
outbox.publish
outbox.mark.sent
outbox.retry
outbox.archive
```

Recommended attributes:

```text
outbox.event.type
outbox.batch.size
outbox.status
outbox.retry.count
```

Never export event payloads.

---

# 7. Trace Context Persistence

To preserve distributed tracing across asynchronous publishing, store the trace context together with every Outbox record.

Recommended database column:

```text
trace_parent
```

instead of only:

```text
trace_id
```

Store the full W3C Trace Context.

Example:

```text
00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01
```

---

# 8. Kafka Producer

Before publishing an event:

- Restore tracing context from the Outbox record.
- Continue the original trace.
- Propagate tracing through Kafka headers.

Required headers:

```text
traceparent
tracestate (optional)
```

---

# 9. Kafka Consumer

The consumer automatically continues the trace received from Kafka.

Expected trace:

```text
HTTP Request
├── Save Order
├── Save Outbox
├── Kafka Publish
└── Kafka Consumer
    ├── Deserialize
    ├── Business Logic
    └── Complete
```

---

# 10. Scheduled Publisher

Scheduled publisher should create spans:

```text
batch.fetch
batch.publish
batch.complete
```

If `traceparent` exists inside the Outbox record, continue the original trace instead of creating a new one.

---

# 11. Logging Integration

Every log record should contain:

```text
traceId
spanId
```

Example:

```text
INFO traceId=4bf92f... spanId=1ac245...
Published batch size=100
```

---

# 12. Tempo Export

```text
Spring Boot
      │
      ▼
    OTLP
      │
      ▼
 Grafana Tempo
      │
      ▼
   Grafana
```

Example configuration:

```yaml
management:
  tracing:
    enabled: true
    sampling:
      probability: 1.0

  otlp:
    tracing:
      endpoint: http://tempo:4318/v1/traces
```

---

# 13. Sampling

Development:

```text
100%
```

Production:

```text
Configurable (recommended 10%)
```

---

# 14. Security

Never export:

- HTTP bodies
- Kafka payloads
- SQL parameters
- Passwords
- Tokens
- Secrets
- Personal information

---

# 15. Docker Compose

```text
application
postgres
kafka
prometheus
grafana
tempo
```

OpenTelemetry Collector is intentionally omitted in v1.

---

# 16. Acceptance Criteria

- HTTP requests create traces.
- Database operations appear as spans.
- Outbox operations are visible.
- Kafka Producer is traced.
- Kafka Consumer continues the same trace.
- Trace context is propagated through Kafka headers.
- Outbox stores `traceparent`.
- Logs contain `traceId` and `spanId`.
- Grafana displays the complete end-to-end trace.
- Tracing can be disabled without affecting application behavior.

---

# 17. Expected End-to-End Trace

```text
HTTP POST /orders
│
├── Controller
├── Validation
├── Save Order
├── Save Outbox
├── Commit Transaction
├── Outbox Publisher
├── Kafka Producer
├── Kafka Broker
├── Kafka Consumer
├── Deserialize
├── Business Processing
└── Acknowledge Message
```
