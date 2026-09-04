# Spring Boot Transactional Outbox with Kafka & PostgreSQL

[![CI](https://github.com/KHolodilin/spring-transactional-outbox-kafka/actions/workflows/ci.yml/badge.svg)](https://github.com/KHolodilin/spring-transactional-outbox-kafka/actions/workflows/ci.yml)
[![GitHub Release](https://img.shields.io/github/v/release/KHolodilin/spring-transactional-outbox-kafka)](https://github.com/KHolodilin/spring-transactional-outbox-kafka/releases/latest)
[![codecov](https://codecov.io/gh/KHolodilin/spring-transactional-outbox-kafka/graph/badge.svg)](https://codecov.io/gh/KHolodilin/spring-transactional-outbox-kafka)
[![Java](https://img.shields.io/badge/Java-21-orange?logo=openjdk)](https://openjdk.org/)
[![Spring Boot](https://img.shields.io/badge/Spring%20Boot-4.1-brightgreen?logo=springboot)](https://spring.io/projects/spring-boot)
[![License](https://img.shields.io/badge/License-Apache%202.0-blue.svg)](LICENSE)
[![Kafka](https://img.shields.io/badge/Kafka-transactional%20outbox-black?logo=apachekafka)](https://kafka.apache.org/)
[![PostgreSQL](https://img.shields.io/badge/PostgreSQL-outbox%20store-336791?logo=postgresql)](https://www.postgresql.org/)
[![OpenSearch](https://img.shields.io/badge/OpenSearch-centralized%20logging-005EB8?logo=opensearch)](https://opensearch.org/)
[![Grafana](https://img.shields.io/badge/Grafana-metrics%20%26%20tracing-F46800?logo=grafana)](https://grafana.com/)

Production-ready Spring Boot example of the **Transactional Outbox Pattern** with idempotency, crash recovery, batch publishing, load testing, and observability.
<p align="center">
  <a href="docs/images/hero-architecture.png">
    <img alt="Architecture diagram" src="docs/images/hero-architecture.png" />
  </a>
</p>

## 💡 Why this project?

There are several well-established ways to implement the Transactional Outbox pattern. Each approach solves the same reliability problem, but with different trade-offs in latency, operational complexity, infrastructure, and database load.

This project targets teams already using **Spring Boot, PostgreSQL, and Kafka** that want low-latency event delivery without continuous database polling or an additional CDC platform.

### ⚖️ Comparison

| Approach | ✅ Pros | ⚠️ Cons |
|----------|----------|----------|
| **Database Polling** | ✅ Simple to understand<br>✅ Works with almost any relational database<br>✅ Easy to implement | ❌ Continuously polls the database<br>❌ Additional latency between commit and publishing<br>❌ Database load grows with polling frequency |
| **PostgreSQL LISTEN / NOTIFY** | ✅ Near real-time notifications<br>✅ Built into PostgreSQL<br>✅ No polling during normal operation | ⚠️ Notifications are not durable<br>⚠️ Recovery scanning is still required after failures<br>⚠️ PostgreSQL-specific solution |
| **Debezium (CDC)** | ✅ Reliable Change Data Capture<br>✅ No application polling<br>✅ Excellent for large event-driven platforms | ❌ Requires Kafka Connect and Debezium infrastructure<br>❌ Higher operational complexity<br>❌ Event publishing is managed outside the application |
| **Memory Queue + Recovery (this project)** | ✅ No continuous database polling during normal operation<br>✅ Single publishing pipeline for normal and recovery flows<br>✅ PostgreSQL remains the durable source of truth<br>✅ Recovery scans only unpublished events in the ACTIVE partition | ⚠️ Requires an in-memory queue per application instance<br>⚠️ Recovery worker is still required after unexpected failures |

### 🎯 Why this approach?

Instead of using PostgreSQL as both a database and a message queue, this project separates those responsibilities.

- **PostgreSQL** provides durable event storage.
- **Memory Queue** delivers events with minimal latency.
- **Recovery Worker** restores events after failures.
- **Kafka Batch Publisher** is shared by both the normal and recovery paths.

During normal operation, the application never continuously polls the database for new events.

After a transaction commits, the event identifier is immediately placed into the in-memory queue. If the application crashes or the queue cannot process the event, the recovery worker scans only the **ACTIVE** partition, re-enqueues unpublished events, and sends them through the **same publishing pipeline**.

The result is a solution that provides:

- ✅ Low-latency event delivery
- ✅ Minimal PostgreSQL load during normal operation
- ✅ Constant recovery scan cost using Active / Archive partitioning
- ✅ A single, consistent publishing pipeline
- ✅ Production-ready observability with metrics, structured logging, and distributed tracing

Servlet (`order-service`) and Virtual Threads (`order-service-vt`) get that pipeline from reusable starters. Reactive (`order-service-reactive`) keeps its own R2DBC stack.

| Starter | Version | Repository |
|---------|---------|------------|
| `spring-boot-outbox-starter` | `0.1.0` | [KHolodilin/spring-boot-outbox-starter](https://github.com/KHolodilin/spring-boot-outbox-starter) |
| `spring-boot-idempotency-starter` | `0.3.2` | [KHolodilin/spring-boot-idempotency-starter](https://github.com/KHolodilin/spring-boot-idempotency-starter) |

## 🔄 How it works

The project is built around three core workflows:

1. **Normal Flow** — processes newly created outbox events with minimal latency.
2. **Recovery Flow** — restores unpublished events after crashes or temporary failures.
3. **Idempotent Request Flow** — prevents duplicate order creation and duplicate outbox events.

The **Normal Flow** and **Recovery Flow** converge into the same publishing pipeline, sharing the Memory Queue and Kafka Batch Publisher.

### ✅ Normal Flow

```mermaid
sequenceDiagram
    actor Client
    participant Service as Order Service
    participant DB as PostgreSQL
    participant Queue as Memory Queue
    participant Publisher as Kafka Batch Publisher
    participant Kafka

    Client->>Service: POST /api/v1/orders

    Service->>DB: Save order
    Service->>DB: Save outbox event (NEW)
    DB-->>Service: Commit

    Service->>Queue: Enqueue eventId
    Service-->>Client: HTTP 201 Created

    Publisher->>Queue: Read event IDs
    Publisher->>DB: Load event payloads
    Publisher->>Kafka: Publish events
    Publisher->>DB: Update event status
```

The business transaction stores the order, the outbox event, and the idempotency record in a single database transaction.

After the transaction commits, only the **event ID** is placed into the Memory Queue. The publisher reads event IDs in batches, loads the corresponding payloads from PostgreSQL, publishes them to Kafka, and updates the event status.

During normal operation, the application does not continuously poll PostgreSQL for new events.

### 🔁 Recovery Flow

```mermaid
sequenceDiagram
    participant Recovery as Recovery Worker
    participant DB as PostgreSQL
    participant Queue as Memory Queue
    participant Publisher as Kafka Batch Publisher
    participant Kafka

    Recovery->>DB: Find unpublished ACTIVE events
    DB-->>Recovery: Event IDs

    Recovery->>Queue: Re-enqueue event IDs

    Publisher->>Queue: Read event IDs
    Publisher->>DB: Load event payloads
    Publisher->>Kafka: Publish events
    Publisher->>DB: Update event status
```

If an event is committed but is not processed through the normal flow, it remains safely stored in PostgreSQL.

The Recovery Worker periodically scans unpublished events from the **ACTIVE** partition and places their IDs back into the Memory Queue.

> **One Publishing Pipeline**
>
> Recovery never publishes events directly. Both the Normal Flow and Recovery Flow use the same Memory Queue, batch loading logic, Kafka Batch Publisher, and event status update process.
>
> This eliminates duplicate publishing logic and keeps the behavior consistent across normal operation and recovery.

### 📦 Why Active / Archive?

```mermaid
flowchart LR
    A["ACTIVE<br/>NEW<br/>PROCESSING<br/>FAILED"]
    B["Recovery Worker"]
    C["SENT"]
    D["ARCHIVE"]

    A -->|Scan unpublished events| B
    B -->|Re-enqueue event IDs| A
    A -->|Published| C
    C --> D
```

Only unpublished events remain in the **ACTIVE** partition.

Once an event is successfully published, it is moved to the **ARCHIVE** partition and is never scanned by the Recovery Worker again.

As a result, recovery performance depends only on the number of active events instead of the total history stored in the outbox table.

### 🔑 Idempotent Request Flow

Servlet and Virtual Threads peers use [`spring-boot-idempotency-starter`](https://github.com/KHolodilin/spring-boot-idempotency-starter) (fluent API) and [`spring-boot-outbox-starter`](https://github.com/KHolodilin/spring-boot-outbox-starter) with PostgreSQL tables `idempotency_records` / `outbox_events`. The idempotency outcome is committed in the **same transaction** as the order and outbox row.

The Notification Stub also uses the idempotency starter to process each Kafka `eventId` once, storing consumer-side records in its own `notification_idempotency_records` table.

```mermaid
sequenceDiagram
    actor Client
    participant Service as Order Service
    participant DB as PostgreSQL

    Client->>Service: POST /api/v1/orders<br/>Idempotency-Key

    Service->>Service: Fingerprint request (starter)
    Service->>DB: Lookup idempotency_records

    alt New request
        DB-->>Service: Not found
        Service->>DB: Business transaction + persist outcome
        Service-->>Client: HTTP 201 Created

    else Same key + same request
        DB-->>Service: Stored response
        Service-->>Client: HTTP 200 OK (replay)

    else Same key + different request
        DB-->>Service: Fingerprint mismatch
        Service-->>Client: HTTP 409 Conflict
    end
```

Each request is uniquely identified by the combination of **customerId** and **Idempotency-Key** (`operation = CREATE_ORDER:{customerId}`).

For a new request, the service executes the business transaction and stores the response. If the same request is received again with an identical payload, the stored response is returned immediately. If the payload differs, the request is rejected with **HTTP 409 Conflict**.

## 🔭 Observability

The project includes production-ready observability out of the box, providing complete visibility into the event delivery pipeline.

By combining **metrics**, **structured logging**, and **distributed tracing**, you can quickly understand system behavior, investigate failures, and troubleshoot performance issues.

### 📊 Metrics

*Powered by **Prometheus** and **Grafana***.

Monitor application health, queue utilization, publishing latency, retry activity, recovery operations, and standard Spring Boot, JVM, and PostgreSQL metrics.

**Grafana Dashboard**

![Grafana Dashboard](docs/images/grafana-dashboard.png)

*Monitor queue utilization, publishing latency, recovery activity, JVM health, and application performance in real time.*

In addition to standard metrics, the project exposes custom metrics for the Transactional Outbox pipeline.

| Metric | Description |
|---------|-------------|
| `outbox.queue.size` | Current Memory Queue size |
| `outbox.queue.pressure` | Memory Queue utilization ratio |
| `outbox.publish.latency` | Kafka publishing latency |
| `outbox.publish.failures` | Number of failed publish attempts |
| `outbox.retry.count` | Publisher retry count |
| `outbox.recovery.count` | Events restored by the Recovery Worker |
| `outbox.rate_limit.rejects` | HTTP 429 responses |

### 🔍 Structured Logging

*Powered by **Fluent Bit** and **OpenSearch***.

All services produce structured JSON logs, making it easy to investigate failures and trace business operations across the system.

**OpenSearch Dashboards**

![OpenSearch Dashboard](docs/images/opensearch-dashboard.png)

*Search requests, investigate failures, and correlate business events using structured log fields.*

Every log entry includes searchable business and tracing identifiers:

- `correlationId`
- `customerId`
- `idempotencyKey`
- `traceId`
- `eventId`
- `instanceId`

The project also provides preconfigured dashboards and useful saved queries.

| Query | Purpose |
|---------|-------------|
| Logs by `customerId` | View the complete processing history for a customer |
| Logs by `correlationId` | Trace a single request across all services |
| Outbox publish failures | Investigate failed Kafka publishing |
| Rate limit rejected | Find HTTP 429 responses |

### 🔗 Distributed Tracing

*Powered by **OpenTelemetry** and **Grafana Tempo***.

Follow every request across the complete processing pipeline—from the REST API through the business transaction and Kafka publishing to downstream services.

**Grafana Tempo Trace**

![Distributed Tracing](docs/images/distributed-tracing.png)

*Visualize the complete lifecycle of a request, identify latency bottlenecks, and understand interactions between application components.*

Distributed tracing helps you:

- Follow requests across service boundaries
- Identify latency bottlenecks
- Understand asynchronous processing
- Correlate traces with logs and metrics

### 🌐 Local Services

After `docker compose --profile observability up -d` (clone) or the same profile on a flavor file, the following services are available:

| Service | URL | Purpose |
|---------|-----|---------|
| Grafana | http://localhost:3000 | Dashboards (`admin / admin`) |
| Prometheus | http://localhost:9090 | Metrics collection |
| Grafana Tempo | http://localhost:3200 | Distributed tracing backend |
| OpenSearch | http://localhost:9200 | Structured log storage |
| OpenSearch Dashboards | http://localhost:5601 | Log search and dashboards |
| PostgreSQL Exporter | http://localhost:9187/metrics | PostgreSQL metrics |

> 💡 **Tip**
>
> After starting the application, create a few orders using the REST API, then open Grafana, OpenSearch Dashboards, and Tempo. Viewing metrics, logs, and traces together is the fastest way to understand how events move through the system.

## 🚀 Quick Start

You only need **Docker**. Java and Maven are not required.

Pick a stack — servlet, WebFlux, or Virtual Threads. Host ports do not overlap; all three can run at once.

| Stack | Compose asset | Order | Stub | Grafana |
|-------|---------------|-------|------|---------|
| Servlet + JDBC | `compose.servlet.yml` | http://localhost:8090 | :8091 | :3000 |
| WebFlux + R2DBC | `compose.reactive.yml` | http://localhost:8092 | :8093 | :3001 |
| Virtual Threads + JDBC | `compose.vt.yml` | http://localhost:8094 | :8095 | :3002 |

Compose modes (same on every flavor file):

| Mode | Command | What starts |
|------|---------|-------------|
| Default | `docker compose -f docker/compose.servlet.yml up -d` | Postgres, Kafka, order-service, notification-stub |
| Observability | `docker compose -f docker/compose.servlet.yml --profile observability up -d` | Default plus Grafana, Prometheus, Tempo, OpenSearch |

### 🐳 1. Download a compose file and start

PowerShell (servlet):

```powershell
Invoke-WebRequest -Uri https://github.com/KHolodilin/spring-transactional-outbox-kafka/releases/latest/download/compose.servlet.yml -OutFile compose.yml
docker compose -f compose.yml up -d
```

bash (servlet):

```bash
curl -fsSL -o compose.yml https://github.com/KHolodilin/spring-transactional-outbox-kafka/releases/latest/download/compose.servlet.yml
docker compose -f compose.yml up -d
```

Replace `compose.servlet.yml` with `compose.reactive.yml` or `compose.vt.yml` for the other stacks.

From a clone, the same files live in `docker/`:

```bash
docker compose -f docker/compose.servlet.yml up -d
```

Order Service: http://localhost:8090 — Notification Stub: http://localhost:8091

### 🛒 2. Create an order

Send a request with a unique `Idempotency-Key`:

```bash
curl -X POST http://localhost:8090/api/v1/orders \
  -H "Content-Type: application/json" \
  -H "Idempotency-Key: 550e8400-e29b-41d4-a716-446655440000" \
  -d '{
    "customerId": 42,
    "items": [
      {
        "productId": "sku-1",
        "quantity": 2,
        "price": 10.50
      }
    ],
    "correlationId": "quick-start-demo"
  }'
```

The request creates:

1. An order
2. An idempotency record
3. A transactional outbox event

All three records are committed in the same PostgreSQL transaction.

After the commit, the outbox event ID is placed into the Memory Queue, published to Kafka, and consumed by the Notification Stub.

### ♻️ 3. Verify idempotency

Repeat the same request with the same `Idempotency-Key` and payload:

```bash
curl -X POST http://localhost:8090/api/v1/orders \
  -H "Content-Type: application/json" \
  -H "Idempotency-Key: 550e8400-e29b-41d4-a716-446655440000" \
  -d '{
    "customerId": 42,
    "items": [
      {
        "productId": "sku-1",
        "quantity": 2,
        "price": 10.50
      }
    ],
    "correlationId": "quick-start-demo"
  }'
```

The service returns the previously stored response without creating another order or outbox event.

Using the same key with a different request payload returns:

```text
HTTP 409 Conflict
```

### 🧭 4. Explore metrics, logs, and traces

```bash
docker compose -f docker/compose.servlet.yml --profile observability up -d
```

The same `--profile observability` flag works on `compose.reactive.yml` (Grafana :3001) and `compose.vt.yml` (Grafana :3002). Each stack provisions only its Orders Technical dashboard.

Open the local observability services (servlet example):

| Service | URL | What to check |
|---------|-----|---------------|
| Grafana | http://localhost:3000 | **Orders Technical** (servlet). Reactive → :3001, VT → :3002 |
| Prometheus | http://localhost:9100 | Application and PostgreSQL metrics |
| OpenSearch Dashboards | http://localhost:5601 | Structured application logs |
| Grafana Tempo | http://localhost:3200 | Distributed trace storage |
| OpenSearch | http://localhost:9200 | Indexed JSON logs |

Grafana credentials for the local environment:

```text
Username: admin
Password: admin
```

You can also verify the application metrics directly:

```bash
curl http://localhost:8090/actuator/prometheus
curl http://localhost:8091/actuator/prometheus
```

### 🛑 5. Stop the environment

```bash
docker compose -f compose.yml down
```

From a clone:

```bash
docker compose -f docker/compose.servlet.yml --profile observability down
```

To remove containers together with local volumes and stored data:

```bash
docker compose -f docker/compose.servlet.yml --profile observability down -v
```

### 🛠️ Run from source (Java 21 + Maven)

Root `docker-compose.yml` starts Postgres and Kafka for host processes. Observability is opt-in:

```bash
docker compose up -d
docker compose --profile observability up -d
mvn clean verify
mvn -pl order-service spring-boot:run -Dspring-boot.run.profiles=dev
mvn -pl notification-stub spring-boot:run -Dspring-boot.run.profiles=dev
```

Peer services stay on `:8083` (reactive) and `:8084` (virtual threads). See [CONTRIBUTING.md](CONTRIBUTING.md).

## ⚡ Load Testing

The project includes a ready-to-run **Gatling** benchmark for validating the complete event delivery pipeline under sustained load.

The benchmark exercises the entire flow—from the REST API through PostgreSQL, the Transactional Outbox, the Memory Queue, Kafka, and the Notification Stub.

### ▶️ Run the benchmark

```bash
mvn -pl load-tests gatling:test \
  -Dgatling.simulationClass=com.kholodilin.outbox.loadtests.CreateOrderSimulation
```

For a quick smoke test:

```bash
mvn -pl load-tests gatling:test \
  -Dgatling.simulationClass=com.kholodilin.outbox.loadtests.CreateOrderSimulation \
  -DstageDurationSeconds=15 \
  -DrampSeconds=5 \
  -Drps1=10 \
  -Drps2=10 \
  -Drps3=10
```

### Servlet vs reactive vs Virtual Threads A/B

Peer services for sequential Gatling A/B (one under load at a time). See [docs/ab-load-comparison.md](docs/ab-load-comparison.md).

Maven peers use `:8080` / `:8083` / `:8084`. Docker flavors can run together:

| Module | Maven | Docker | Grafana |
|--------|-------|--------|---------|
| `order-service` | 8080 | 8090 (`compose.servlet.yml`) | 3000 |
| `order-service-reactive` | 8083 | 8092 (`compose.reactive.yml`) | 3001 |
| `order-service-vt` | 8084 | 8094 (`compose.vt.yml`) | 3002 |

```bash
mvn -pl load-tests gatling:test \
  -Dgatling.simulationClass=com.kholodilin.outbox.loadtests.CreateOrderVtSimulation \
  -DbaseUrl=http://localhost:8084 \
  -Dprofile=warmup -DwarmupRps=50 -DloadRps=100 -DloadSeconds=120
```

### 📈 Benchmark Metrics

The benchmark measures the following characteristics:

| Metric | Description |
|--------|-------------|
| Throughput | Requests processed per second (RPS) |
| Average Latency | Mean HTTP response time |
| P95 Latency | Response time for 95% of requests |
| Error Rate | Percentage of failed requests |
| Kafka Publish Rate | Events published to Kafka |
| Recovery Activity | Events restored by the Recovery Worker |

### 📉 Analyze the Results

During the benchmark you can monitor the system in real time using the built-in observability stack:

- **Grafana** — throughput, latency, queue utilization, JVM and PostgreSQL metrics
- **OpenSearch** — structured application logs
- **Grafana Tempo** — distributed traces
- **Gatling HTML Report** — detailed benchmark statistics

> 💡 **Tip**
>
> Running the benchmark while observing Grafana dashboards provides the clearest picture of how the Transactional Outbox pipeline behaves under load.

## 📚 Docs

- [spring-boot-outbox-starter](https://github.com/KHolodilin/spring-boot-outbox-starter)
- [spring-boot-idempotency-starter](https://github.com/KHolodilin/spring-boot-idempotency-starter)
- [Technical Specification v2](docs/spring-transactional-outbox-kafka-Technical-Specification-v2.md)
- [Distributed Tracing Spec](docs/spring-transactional-outbox-kafka-Distributed-Tracing-Spec.md)
- [OpenSearch Logging Spec](docs/spring-transactional-outbox-kafka-OpenSearch-Logging-Spec.md)
- [Logging guide](docs/logging.md)
