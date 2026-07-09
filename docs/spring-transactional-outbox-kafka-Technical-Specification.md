# spring-transactional-outbox-kafka

> **Technical Specification (High-Level)**
>
> This repository demonstrates a production-oriented implementation of the Transactional Outbox pattern using Spring Boot 4, PostgreSQL and Kafka.

## Project Goal

Build a resilient event publishing pipeline with the following flow:

```text
Client
    │
    ▼
POST /api/v1/events
    │
    ▼
Rate Limiter
    │
    ▼
Idempotency Check
    │
    ▼
PostgreSQL Transaction
    │
    ├── idempotency_keys
    └── outbox_events (NEW)
    │
    ▼
Commit
    │
    ▼
afterCommit()
    │
    ▼
Memory Queue (eventId only)
    │
    ▼
Kafka Publisher (Batch)
    │
    ▼
Kafka
    │
    ▼
Event Log Consumer
```

## Repository Structure

- event-api-service
- event-log-consumer
- common-events

## Technology Stack

- Java 21+
- Spring Boot 4
- Spring Web MVC
- Spring Data JPA
- JdbcTemplate (batch)
- PostgreSQL
- Kafka
- Liquibase
- Lombok
- Bucket4j
- Micrometer
- Spring Boot Actuator
- Testcontainers
- Docker Compose

## Core Principles

- Transactional Outbox
- PostgreSQL as durable queue
- Memory Queue as hot queue
- Kafka Publisher works **only** through Memory Queue
- Recovery Worker only enqueues ids
- Batch publishing
- Multi-pod safe processing
- Idempotency-Key support
- Adaptive backpressure
- Active / Archive partitions
- No foreign keys on hot-path tables

## Processing Flow

1. Client calls POST /api/v1/events with Idempotency-Key.
2. Rate limiter validates request.
3. Idempotency service validates request hash.
4. Single DB transaction:
   - insert/update idempotency_keys
   - insert outbox_events(status=NEW, partition_state=ACTIVE)
5. Commit.
6. afterCommit() -> enqueue(eventId).
7. Publisher collects batches from Memory Queue.
8. Loads payloads from outbox_events_active.
9. Publishes to Kafka.
10. Marks SENT and moves record to ARCHIVE.
11. Consumer receives Kafka batch.

## idempotency_keys

- Hash partitioning by customer_id (10 partitions)
- BIGINT identity primary key
- UNIQUE(customer_id, idempotency_key)
- B-tree unique index
- No hash index

Statuses:
- PROCESSING
- COMPLETED
- FAILED

Behavior:
- Same key + same request hash => return saved response.
- Same key + different hash => HTTP 409 Conflict.

## outbox_events

Contains:

- BIGINT identity id
- customer_id
- event_type
- event_version
- payload (jsonb)
- status
- partition_state
- retry_count
- locked_by
- locked_until
- sent_at

Partitioning:

- ACTIVE
- ARCHIVE

ACTIVE contains:
- NEW
- PROCESSING
- FAILED
- DEAD

ARCHIVE contains:
- SENT

## Memory Queue

Stores only event ids.

- bounded queue
- dedup set
- batch processing
- configurable batch size
- configurable batch wait

## Batch Publisher

Algorithm:

- take first id
- drain queue
- load payload batch
- publish batch
- update statuses in batch

## Multi-pod

Uses PostgreSQL lease:

- SELECT ... FOR UPDATE SKIP LOCKED
- locked_by
- locked_until

Each pod owns its own Memory Queue.

## Rate Limiting

Bucket4j

Limits:

- global
- per customer
- per IP

Adaptive throttling when queue usage exceeds threshold.

## Logging

Lombok @Slf4j

Structured logs:

- eventId
- customerId
- idempotencyKey
- correlationId

### Log levels

**INFO** — краткие бизнес-события:

- запрос принят / отклонён (rate limit, validation)
- идемпотентный ответ возвращён
- событие записано в outbox
- батч опубликован в Kafka (batch size, duration)
- событие SENT / перенесено в ARCHIVE
- recovery: количество восстановленных id
- ошибки публикации / retry (без лишних деталей)

**DEBUG** — подробные логи для отладки:

- request/response body (с маскированием чувствительных полей)
- SQL batch-операции и параметры
- Memory Queue (size, drained ids, dedup hits)
- lease (locked_by, locked_until, pod id)
- Kafka (topic, partition, offset, headers)
- retry/backoff (attempt, delay)
- шаги afterCommit() и batch publisher pipeline
- stack trace при исключениях

Уровни логирования задаются в `application.yml` (см. Configuration).

## Configuration

Все настраиваемые параметры — в **YAML** (`application.yml` + profile-файлы).

Принципы:

- `application.yml` — общие defaults
- `application-dev.yml` — локальная разработка (DEBUG, Testcontainers/Docker Compose)
- `application-prod.yml` — production (INFO, без секретов в файле)
- секреты и endpoints — через env vars (`${VAR:default}`)
- типизированные бины через `@ConfigurationProperties` (prefix `app.*`)

### Пример структуры

```yaml
spring:
  application:
    name: event-api-service
  datasource: ...
  kafka: ...

logging:
  level:
    root: INFO
    com.example.eventapi: INFO
    com.example.eventapi.publisher: DEBUG

management:
  endpoints:
    web:
      exposure:
        include: health,metrics,prometheus

app:
  outbox:
    memory-queue:
      capacity: 10000
      batch-size: 100
      batch-wait: 50ms
      usage-threshold: 0.8
    publisher:
      lease-duration: 30s
      max-retries: 5
      retry-backoff: 1s
    recovery:
      enabled: true
      interval: 10s
      batch-size: 500
  rate-limit:
    global:
      capacity: 1000
      refill-per-second: 100
    per-customer:
      capacity: 100
      refill-per-second: 10
    per-ip:
      capacity: 50
      refill-per-second: 5
  kafka:
    topic: events
```

### Что обязательно выносить в YAML

| Область | Параметры |
|---------|-----------|
| Memory Queue | capacity, batch-size, batch-wait, usage-threshold |
| Publisher | lease-duration, max-retries, retry-backoff |
| Recovery Worker | enabled, interval, batch-size |
| Rate Limiting | global / per-customer / per-ip limits |
| Kafka | topic, producer/consumer settings |
| Logging | уровни по package (`logging.level.*`) |
| Actuator | exposed endpoints, health details |

Без хардкода в коде — только defaults в YAML + override через env.

## Metrics

Micrometer:

- queue size
- queue usage
- publish latency
- publish failures
- retry count
- recovery count
- rate limit rejects

## Health

Actuator

Custom indicators:

- Kafka
- Queue
- Outbox

## Database

Managed only through Liquibase.

## Tests

- Unit
- Integration
- Testcontainers
- Kafka
- PostgreSQL

## Failure Scenarios

- crash after commit
- queue full
- Kafka unavailable
- duplicate requests
- multi-pod recovery
- retry
- dead-letter
