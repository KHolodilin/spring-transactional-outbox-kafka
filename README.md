# Transactional Outbox Pattern with Kafka and PostgreSQL

[![CI](https://github.com/KHolodilin/spring-transactional-outbox-kafka/actions/workflows/ci.yml/badge.svg)](https://github.com/KHolodilin/spring-transactional-outbox-kafka/actions/workflows/ci.yml)
[![codecov](https://codecov.io/gh/KHolodilin/spring-transactional-outbox-kafka/graph/badge.svg)](https://codecov.io/gh/KHolodilin/spring-transactional-outbox-kafka)
[![Java](https://img.shields.io/badge/Java-21-orange?logo=openjdk)](https://openjdk.org/)
[![Spring Boot](https://img.shields.io/badge/Spring%20Boot-4.0-brightgreen?logo=springboot)](https://spring.io/projects/spring-boot)
[![License](https://img.shields.io/badge/License-Apache%202.0-blue.svg)](LICENSE)
[![Kafka](https://img.shields.io/badge/Kafka-transactional%20outbox-black?logo=apachekafka)](https://kafka.apache.org/)
[![PostgreSQL](https://img.shields.io/badge/PostgreSQL-outbox%20store-336791?logo=postgresql)](https://www.postgresql.org/)
[![OpenSearch](https://img.shields.io/badge/OpenSearch-centralized%20logging-005EB8?logo=opensearch)](https://opensearch.org/)
[![Grafana](https://img.shields.io/badge/Grafana-metrics%20%26%20tracing-F46800?logo=grafana)](https://grafana.com/)

Production-oriented **Transactional Outbox** demo on Spring Boot 4, PostgreSQL and Kafka.

## Modules

| Module | Role |
|--------|------|
| `order-service` | `POST /api/v1/orders`, transactional outbox, Kafka publisher |
| `outbox-events-contract` | Shared DTOs/enums |
| `notification-stub` | Demo downstream notification consumer (mock) |
| `load-tests` | Gatling load tests (baseline `POST /api/v1/orders`) |

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

### 4. Load testing (Gatling)

Baseline simulation lives in `load-tests` module.

```bash
# default baseline: 50/100/200 RPS stages, p95<200ms, errors<0.5%
mvn -pl load-tests gatling:test -Dgatling.simulationClass=com.kholodilin.outbox.loadtests.CreateOrderSimulation
```

Smoke run example (useful in CI):

```bash
mvn -pl load-tests gatling:test \
  -Dgatling.simulationClass=com.kholodilin.outbox.loadtests.CreateOrderSimulation \
  -DstageDurationSeconds=15 -DrampSeconds=5 \
  -Drps1=10 -Drps2=10 -Drps3=10
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

## Observability

The local stack includes **Prometheus**, **Grafana**, **Grafana Tempo**, **OpenSearch**, **OpenSearch Dashboards**, and **Fluent Bit** (via Docker Compose). Applications run on the host and export metrics through Actuator, traces through OTLP, and structured JSON logs to module-local `logs/` directories.

| Service | URL | Notes |
|---------|-----|-------|
| Prometheus | http://localhost:9090 | scrapes apps on host + `postgres-exporter` |
| Grafana | http://localhost:3000 | login `admin` / `admin` (dev only) |
| Grafana Tempo | http://localhost:3200 | OTLP HTTP ingest on `:4318` |
| OpenSearch | http://localhost:9200 | centralized JSON logs (`spring-outbox-logs-local-*`) |
| OpenSearch Dashboards | http://localhost:5601 | classic dashboard + PPL queries |
| postgres-exporter | http://localhost:9187/metrics | standard PostgreSQL metrics |

### 1. Start infrastructure (including monitoring)

```bash
docker compose up -d
```

Index template and saved objects (dashboards, PPL queries) are applied automatically by `opensearch-init` and `opensearch-dashboards-init`.

### 2. Run services and generate traffic

```bash
mvn -pl order-service spring-boot:run -Dspring-boot.run.profiles=dev
mvn -pl notification-stub spring-boot:run -Dspring-boot.run.profiles=dev
```

JSON logs (when running via `mvn -pl …`):

```text
order-service/logs/order-service/app.json
notification-stub/logs/notification-stub/app.json
```

Fluent Bit tails those paths from the host. Full guide: [docs/logging.md](docs/logging.md).

Verify metrics endpoints:

```bash
curl -s http://localhost:8080/actuator/prometheus | head
curl -s http://localhost:8081/actuator/prometheus | head
```

Create an order (see API example above), then open Grafana — dashboards **Transactional Outbox**, **Notification Stub**, **PostgreSQL**, and **Distributed Tracing** are provisioned automatically.

### Centralized logging (OpenSearch)

Structured JSON logs include `correlationId`, `customerId`, `idempotencyKey`, `event.action`, `trace.id`, `service.name`, and more.

**OpenSearch Dashboards** (http://localhost:5601):

| Where | What |
|-------|------|
| **Dashboards** → *Transactional Outbox Overview* | classic saved searches (Discover-style) |
| **Observability → Logs → Queries** | PPL saved queries (results on **Events** tab) |

Notable saved queries:

| Query | Purpose |
|-------|---------|
| **Logs by customerId** | all logs for one customer — edit `customerId = '42'` in the query |
| **Log volume by customerId** | stats table: log count per customerId |
| **Outbox publish failures** | `event.action = outbox.publish.failed` |
| **Rate limit rejected** | HTTP 429 from rate limiter |
| **Logs by correlationId** | filter by correlationId prefix |

Example PPL:

```sql
source = spring-outbox-logs-local-* | where customerId = '42' | sort - @timestamp
```

In Discover/KQL: `customerId:"42"` or `customer.id:42`.

### Distributed tracing (Tempo)

With `dev` profile, both services export traces via Spring Boot 4 OTLP config (`management.opentelemetry.tracing.export.otlp.endpoint`, default `http://localhost:4318/v1/traces`, 100% sampling).

1. Start `docker compose up -d` (includes Tempo).
2. Run `order-service` and `notification-stub` with profile `dev`.
3. `POST /api/v1/orders` (see API example).
4. Grafana → **Distributed Tracing** dashboard → browse recent traces or paste a `traceId` from logs (`traceId=...` in logback output).

End-to-end trace path: HTTP → outbox save → batch publish → Kafka consumer.

Disable tracing without code changes:

```bash
TRACING_ENABLED=false mvn -pl order-service spring-boot:run -Dspring-boot.run.profiles=dev
```

Production sampling defaults to `10%` (`TRACING_SAMPLING=0.1` in `application-prod.yml`).

Prometheus targets: http://localhost:9090/targets (`order-service`, `notification-stub`, `postgres` should be **UP** while the stack is running).

### Custom outbox metrics (`order-service`)

| Metric | Description |
|--------|-------------|
| `outbox.queue.size` | In-memory queue size |
| `outbox.queue.pressure` | Queue fill ratio (0–1) |
| `outbox.publish.latency` | Kafka publish duration |
| `outbox.publish.failures` | Failed publish attempts |
| `outbox.retry.count` | Publisher retries |
| `outbox.recovery.count` | Events re-enqueued by recovery worker |
| `outbox.rate_limit.rejects` | HTTP 429 responses |

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
- [Distributed Tracing Spec](docs/spring-transactional-outbox-kafka-Distributed-Tracing-Spec.md)
- [OpenSearch Logging Spec](docs/spring-transactional-outbox-kafka-OpenSearch-Logging-Spec.md)
- [Logging guide](docs/logging.md)
