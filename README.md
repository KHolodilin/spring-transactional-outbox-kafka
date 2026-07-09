# spring-transactional-outbox-kafka

Production-oriented **Transactional Outbox** demo on Spring Boot 4, PostgreSQL and Kafka.

## Modules

| Module | Role |
|--------|------|
| `order-service` | `POST /api/v1/orders`, transactional outbox, Kafka publisher |
| `outbox-events-contract` | Shared DTOs/enums |
| `notification-stub` | Demo downstream notification consumer (mock) |

## Architecture

```text
Client -> order-service -> PostgreSQL (orders + outbox)
                |
         afterCommit / recovery
                |
          Memory Queue
                |
         Kafka (key=customerId)
                |
        notification-stub
```

## Quick start

### 1. Infrastructure

```bash
docker compose up -d
```

### 2. Build

```bash
mvn clean verify
```

### 3. Run services

```bash
mvn -pl order-service spring-boot:run -Dspring-boot.run.profiles=dev
mvn -pl notification-stub spring-boot:run -Dspring-boot.run.profiles=dev
```

## API example

```bash
curl -X POST http://localhost:8080/api/v1/orders \
  -H "Content-Type: application/json" \
  -H "Idempotency-Key: 550e8400-e29b-41d4-a716-446655440000" \
  -d '{
    "customerId": 42,
    "items": [{"productId": "sku-1", "quantity": 2, "price": 10.50}],
    "correlationId": "demo-1"
  }'
```

## Configuration

Key settings in `order-service/src/main/resources/application.yml`:

| Prefix | Examples |
|--------|----------|
| `app.outbox.memory-queue` | capacity, batch-size, usage-threshold |
| `app.outbox.publisher` | lease-duration, max-retries |
| `app.outbox.recovery` | enabled, interval, batch-size |
| `app.rate-limit` | global / per-customer / per-ip |
| `app.kafka.topic` | `orders.events` |

Kafka messages use **partition key = `customerId`**.

## Failure scenarios (local)

| Scenario | Expected behavior |
|----------|-------------------|
| Crash after commit | Recovery worker re-enqueues NEW events |
| Queue full | Event stays NEW, recovery picks it up |
| Kafka unavailable | Retry -> FAILED -> recovery -> DEAD after max retries |
| Duplicate HTTP request | Idempotent 200 response |
| Duplicate enqueue | Memory queue dedup |

## Docs

- [Technical Specification v2](docs/spring-transactional-outbox-kafka-Technical-Specification-v2.md)
- [Implementation Plan](docs/spring-transactional-outbox-kafka-Implementation-Plan.md)
