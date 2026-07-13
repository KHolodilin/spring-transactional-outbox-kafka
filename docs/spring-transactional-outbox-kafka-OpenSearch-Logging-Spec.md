# Technical Specification

# Centralized Logging with OpenSearch

## Goal

Implement a production-ready centralized logging solution for
**spring-transactional-outbox-kafka**.

The solution must provide:

-   Human-readable logs for developers.
-   Structured JSON logs for centralized storage.
-   Correlation through existing Micrometer Tracing (`traceId`,
    `spanId`).
-   Search and analytics in OpenSearch.
-   Support for multiple Spring Boot services.

------------------------------------------------------------------------

# Architecture

``` text
                +----------------+
                | order-service  |
                +----------------+
                  │          │
                  │          │
                  │          ├──────────────► Console Appender (human-readable)
                  │
                  └──────────────► JSON File Appender
                                     │
                +----------------+
                |notification    |
                |stub            |
                +----------------+
                  │          │
                  │          ├──────────────► Console Appender (human-readable)
                  │
                  └──────────────► JSON File Appender
                                     │
                                     ▼
                                Fluent Bit
                                     │
                                     ▼
                                OpenSearch
                                     │
                                     ▼
                         OpenSearch Dashboards
```

------------------------------------------------------------------------

# Services

Logging must be enabled for:

-   order-service
-   notification-stub

The `outbox-events-contract` module does not produce logs.

------------------------------------------------------------------------

# Existing Components

Already implemented:

-   Micrometer Tracing
-   traceId
-   spanId

Do **not** add another tracing implementation.

------------------------------------------------------------------------

# Logging Strategy

Each service must use **two Logback appenders**.

## Console Appender

Purpose:

-   IntelliJ
-   docker logs
-   Local debugging

Output format:

-   Plain text
-   Human-readable
-   No JSON

Example:

``` text
2026-07-13 15:20:01.421 INFO [order-service] [traceId=fc8a81d2] Outbox batch published batchSize=100 durationMs=47
```

------------------------------------------------------------------------

## JSON File Appender

Purpose:

-   Structured logging
-   Fluent Bit ingestion
-   OpenSearch indexing

Output:

-   JSON
-   Rolling files
-   ECS/Logstash compatible

Example:

``` json
{
  "@timestamp":"2026-07-13T15:20:01Z",
  "service.name":"order-service",
  "trace.id":"fc8a81d2",
  "span.id":"e123c98e",
  "correlationId":"req-8d27a50c",
  "customerId":"43",
  "idempotencyKey":"96cb1a9e-84cb-4a43-b769-68fdd6d72df8",
  "event.action":"outbox.batch.published",
  "order.id":781,
  "customer.id":43,
  "outbox.id":1542
}
```

------------------------------------------------------------------------

# Required Fields

Common:

-   @timestamp
-   log.level
-   message
-   service.name
-   service.version
-   deployment.environment
-   instance.id
-   logger.name
-   thread.name
-   trace.id
-   span.id
-   correlationId
-   customerId
-   idempotencyKey

The fields `correlationId`, `customerId`, and `idempotencyKey` are already
placed into MDC by `order-service` and must be included automatically in
the JSON output of the JSON File Appender.

`idempotencyKey` must be treated as a technical identifier. It must not
contain credentials or personal data. If this cannot be guaranteed, the
value must be hashed or masked before being placed into MDC.

order-service:

-   outbox.id
-   order.id
-   customer.id
-   event.type
-   outbox.status
-   outbox.status_code
-   outbox.retry_count
-   outbox.batch_size
-   kafka.topic
-   kafka.partition
-   duration.ms

notification-stub:

-   event.type
-   order.id
-   customer.id
-   notification.channel
-   notification.status
-   kafka.topic
-   kafka.partition
-   kafka.offset
-   duration.ms

------------------------------------------------------------------------


# MDC Correlation Fields

The following MDC fields are used by `order-service`:

```java
MDC.put("correlationId", correlationId);
MDC.put("customerId", String.valueOf(request.getCustomerId()));
MDC.put("idempotencyKey", idempotencyKey);
```

Requirements:

- The Console Appender must display `correlationId`, `customerId`, and
  `idempotencyKey` when present.
- The JSON File Appender must export these MDC values as separate JSON
  fields.
- The fields must be indexed as `keyword` in OpenSearch.
- The fields must be available for exact-match search and filtering.
- MDC values must be cleared after request processing to avoid leaking
  context between reused application threads.
- `trace.id` and `span.id` remain tracing identifiers generated by
  Micrometer Tracing.
- `correlationId` is an application-level request correlation identifier
  and does not replace `trace.id`.
- `customerId` and `customer.id` must not contain conflicting values. If
  both fields are emitted, they must represent the same customer.
- New code should prefer one canonical OpenSearch field name. The
  existing MDC names may be retained for backward compatibility.

Recommended Console Appender output:

```text
2026-07-13 15:20:01.421 INFO [order-service] [traceId=fc8a81d2] [spanId=e123c98e] [correlationId=req-8d27a50c] [customerId=43] [idempotencyKey=96cb1a9e-84cb-4a43-b769-68fdd6d72df8] Outbox batch published
```

Recommended OpenSearch mapping:

```text
correlationId   keyword
customerId      keyword
idempotencyKey  keyword
trace.id        keyword
span.id         keyword
```

------------------------------------------------------------------------

# event.action

HTTP

-   http.request.accepted
-   http.request.completed
-   http.request.rejected
-   http.validation.failed

Idempotency

-   idempotency.request.created
-   idempotency.response.reused
-   idempotency.conflict

Outbox

-   outbox.event.persisted
-   outbox.batch.loaded
-   outbox.batch.published
-   outbox.publish.failed
-   outbox.event.sent
-   outbox.event.archived
-   outbox.retry
-   outbox.recovery.completed

Notification Stub

-   notification.batch.received
-   notification.processing.started
-   notification.processed
-   notification.processing.failed

------------------------------------------------------------------------

# Log Levels

INFO

-   Business events only

WARN

-   Retry
-   Queue pressure
-   Kafka unavailable
-   Slow processing

ERROR

-   Unexpected exceptions
-   Publish failures
-   Consumer failures
-   Recovery failures

Stacktrace required.

DEBUG

-   SQL
-   Kafka headers
-   Queue internals
-   EventEnvelope
-   Batch diagnostics

------------------------------------------------------------------------


# Instance Identity

Every application instance must have a unique identifier.

Configuration:

```yaml
app:
  instance-id: ${HOSTNAME:local-${random.uuid}}
```

Requirements:

- `app.instance-id` is the canonical identifier of a running application instance.
- `instance.id` must be included in every structured JSON log.
- `instance.id` should be displayed by the Console Appender.
- The database column `locked_by` must always contain the same value as `app.instance-id`.
- The following invariant must always hold:

```text
locked_by == instance.id == app.instance-id
```

This identifier is used for:

- multi-pod diagnostics;
- recovery analysis;
- lease ownership;
- Kafka publisher diagnostics;
- OpenSearch filtering and aggregation.

Example:

```json
{
  "service.name": "order-service",
  "instance.id": "order-service-7f7c66dbdd-x8r9v",
  "locked_by": "order-service-7f7c66dbdd-x8r9v",
  "trace.id": "fc8a81d2",
  "span.id": "e123c98e"
}
```

OpenSearch mapping:

```text
instance.id keyword
locked_by keyword
```

Typical searches:

```text
instance.id:"order-service-7f7c66dbdd-x8r9v"

locked_by:"order-service-7f7c66dbdd-x8r9v"
```

------------------------------------------------------------------------

# Fluent Bit

Responsibilities:

-   Read JSON log files
-   Parse JSON
-   Buffer locally
-   Retry automatically
-   Batch delivery
-   Forward to OpenSearch

------------------------------------------------------------------------

# OpenSearch

Daily indices:

-   spring-outbox-logs-local-YYYY.MM.DD
-   spring-outbox-logs-test-YYYY.MM.DD
-   spring-outbox-logs-prod-YYYY.MM.DD

Separate services using:

-   service.name

------------------------------------------------------------------------

# Dashboard

Transactional Outbox Overview

Producer:

-   Requests
-   Outbox events
-   Published batches
-   Publish failures
-   Retry count

Consumer:

-   Consumed events
-   Processed events
-   Failed events

End-to-end search:

-   trace.id
-   span.id
-   correlationId
-   customerId
-   idempotencyKey
-   order.id
-   customer.id
-   outbox.id

------------------------------------------------------------------------


# Saved Searches

The following searches must work in OpenSearch Dashboards:

```text
trace.id:"fc8a81d2"
span.id:"e123c98e"
correlationId:"req-8d27a50c"
customerId:"43"
idempotencyKey:"96cb1a9e-84cb-4a43-b769-68fdd6d72df8"
order.id:781
customer.id:43
outbox.id:1542
event.type:"OrderCreated"
event.action:"outbox.publish.failed"
event.action:"notification.processing.failed"
log.level:"ERROR"
service.name:"order-service"
service.name:"notification-stub"
```

------------------------------------------------------------------------

# Security

Never log:

-   passwords
-   JWT
-   Authorization headers
-   secrets
-   sensitive personal data
-   full Kafka payloads

------------------------------------------------------------------------

# Docker Compose

Infrastructure:

-   OpenSearch
-   OpenSearch Dashboards
-   Fluent Bit

Applications:

-   ConsoleAppender -\> stdout
-   JSON RollingFileAppender -\> /var/log/app/\*.json

Fluent Bit reads only JSON files.

------------------------------------------------------------------------

# Documentation

Create:

-   docs/logging.md

Describe:

-   architecture
-   startup
-   troubleshooting
-   dashboards
-   searches

------------------------------------------------------------------------

# Acceptance Criteria

-   Human-readable logs available in console.
-   Structured JSON logs written to rolling files.
-   Fluent Bit collects JSON logs.
-   OpenSearch stores logs.
-   Dashboards visualize logs.
-   Search works by:
    -   trace.id
    -   span.id
    -   correlationId
    -   customerId
    -   idempotencyKey
    -   order.id
    -   customer.id
    -   outbox.id
-   Logs from order-service and notification-stub are correlated using
    the same trace.id.
-   MDC fields `correlationId`, `customerId`, and `idempotencyKey` are
    present in JSON logs when set by the application.
-   Exact-match search works for `correlationId`, `customerId`, and
    `idempotencyKey`.
-   MDC context is cleared after request completion.
-   `instance.id` is present in every log record.
-   `locked_by` always equals `instance.id`.
