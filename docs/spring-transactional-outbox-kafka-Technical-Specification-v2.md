# spring-transactional-outbox-kafka

## Goal

Production-oriented implementation of the **Transactional Outbox Pattern** on Spring Boot 4.

The project keeps the classic business flow (`Order -> Outbox`) but improves the publishing pipeline by introducing a single in-memory publishing path.

## Repository

```
spring-transactional-outbox-kafka/
├── order-service
├── notification-stub
├── outbox-events-contract
└── docker-compose.yml
```

## Build System

- Apache Maven (multi-module project)

## Technology

- Java 21
- Spring Boot 4
- Spring Web MVC
- Spring Data JPA
- JdbcTemplate
- PostgreSQL
- Kafka
- Liquibase
- Lombok
- Bucket4j
- Micrometer
- Actuator
- Testcontainers
- Docker Compose

## Main API

```
POST /api/v1/orders
Idempotency-Key: <uuid>
```

## Processing Flow

```
Client
   │
   ▼
RateLimiter
   │
   ▼
Idempotency
   │
   ▼
@Transactional
   │
   ├── insert orders
   ├── insert order_items
   ├── insert outbox_events(status=NEW, partition_state=ACTIVE)
   └── complete idempotency_keys
   │
 Commit
   │
afterCommit()
   │
enqueue(eventId)
   │
   ▼
Memory Queue
   │
   ▼
Kafka Batch Publisher
   │
   ▼
Kafka
   │
   ▼
notification-stub
```

## Recovery Flow

Recovery never publishes directly.

```
Recovery Worker
      │
claim ACTIVE events
      │
enqueue(eventId)
      │
Memory Queue
      │
Kafka Publisher
      │
Kafka
```

There is only one publishing pipeline:

```
outbox_events
      │
      ├── Fast Path (afterCommit)
      │
      └── Recovery Path
               │
               ▼
          Memory Queue
               │
               ▼
        Kafka Batch Publisher
               │
               ▼
             Kafka
```

## orders

- BIGINT identity primary key
- hash partitioning by customer_id (10 partitions)
- no foreign keys on hot path

## order_items

- BIGINT identity primary key
- order_id
- no FK (application-level integrity)

## idempotency_keys

- hash partitioning by customer_id (10 partitions)
- unique(customer_id,idempotency_key)
- B-tree unique index
- statuses:
  - PROCESSING
  - COMPLETED
  - FAILED

Behavior:

- same key + same request hash → return stored response
- same key + different hash → HTTP 409 Conflict

## outbox_events

Stores ready-to-publish Kafka payload.

Statuses:

- NEW
- PROCESSING
- FAILED
- DEAD
- SENT

Partitions:

- ACTIVE
- ARCHIVE

Publisher reads only ACTIVE.

## Memory Queue

Stores only event ids.

- bounded
- deduplicated
- batch processing

Publisher algorithm:

1. take first id
2. drain queue
3. load payload batch from PostgreSQL
4. publish to Kafka (**partition key = `customer_id`**)
5. archive successful events

## Kafka

Messages are published to a single topic.

**Partition key:** `customer_id` (string representation).

Rationale:

- all events for the same customer go to the same Kafka partition
- per-customer ordering is preserved within a partition
- aligns with PostgreSQL hash partitioning by `customer_id` in `orders` and `idempotency_keys`

Producer:

- `KafkaTemplate` / `ProducerRecord` with key = `customerId`
- value = serialized `EventEnvelope` (JSON)

Consumer:

- `notification-stub` reads batch; per-partition ordering is guaranteed by Kafka for a given `customer_id`

## notification-stub

Demo **notification stub** — imitates downstream notification delivery (email/push/SMS) after an order event.

Not a production notification service.

Responsibilities:

- consume `OrderCreated` (and similar) events from Kafka
- deserialize `EventEnvelope`
- mock notification: structured log / in-memory record (no real SMTP, push, or DB)
- INFO: notification stub processed (orderId, customerId, batch size)
- DEBUG: full event details

No persistence, no external integrations — stub only for demonstrating the outbox pipeline end-to-end.

## Multi-pod

Each pod has its own Memory Queue.

Coordination:

- FOR UPDATE SKIP LOCKED
- locked_by
- locked_until

## Rate limiting

Bucket4j

- global
- per customer
- per IP

Adaptive backpressure when queue usage grows.

## Logging

Lombok @Slf4j

Structured logging:

- eventId
- orderId
- customerId
- idempotencyKey
- correlationId

## Database migrations

Liquibase only.

## Monitoring

Micrometer + Actuator.

Metrics:

- queue size
- queue pressure
- publish latency
- publish failures
- retry count
- recovery count

## Tests

- Unit
- Integration
- Kafka
- PostgreSQL
- Testcontainers

## Failure Scenarios

- crash after commit
- queue full
- Kafka unavailable
- duplicate HTTP requests
- duplicate enqueue
- multi-pod recovery
- retry
- dead-letter

## Project Highlights

- Spring Boot 4
- Transactional Outbox
- Fast Path + Recovery Path
- Single publishing pipeline
- Durable queue
- Bounded Memory Queue
- Kafka Batch Publisher
- Kafka partition key by customer_id
- notification-stub (demo downstream)
- Idempotency-Key
- Active/Archive partitions
- Multi-pod safe
- High-load architecture




## Logging

### INFO

Business-level events only:

- request accepted / rejected (rate limit, validation)
- idempotent response returned
- outbox event persisted
- Kafka batch published (batch size, duration)
- event marked as SENT and moved to ARCHIVE
- recovery summary (number of recovered event ids)
- publish failures / retry summary (without excessive details)

### DEBUG

Detailed diagnostics:

- request / response body (sensitive fields masked)
- SQL batch operations and parameters
- Memory Queue internals (queue size, drained ids, dedup hits)
- lease information (locked_by, locked_until, instance id)
- Kafka metadata (topic, partition, offset, headers)
- retry / backoff (attempt number, delay)
- afterCommit() processing
- batch publisher pipeline steps
- full stack trace for exceptions

Logging levels are configured in **application.yml** and profile-specific configuration files.

---

## Configuration

All configurable parameters are stored in YAML configuration.

Configuration files:

- application.yml — common defaults
- application-dev.yml — local development (DEBUG, Docker Compose / Testcontainers)
- application-prod.yml — production (INFO, no secrets)

Principles:

- secrets and endpoints are provided through environment variables using `${VAR:default}`
- strongly typed configuration via `@ConfigurationProperties` using the `app.*` prefix
