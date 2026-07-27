# Servlet vs Reactive vs Virtual Threads A/B load comparison

Compare three peer order services **sequentially** on the same machine:

| Peer | Stack | Port | DB |
|------|-------|------|-----|
| `order-service` | Tomcat + platform threads + JDBC | `:8080` | `outbox` |
| `order-service-reactive` | WebFlux + R2DBC | `:8083` | `outbox_reactive` |
| `order-service-vt` | Tomcat + Virtual Threads + JDBC | `:8084` | `outbox_vt` |

Do not run more than one service under load at once if you want a clean CPU/IO comparison. Kafka topic `orders.events` is shared.

## Prerequisites

```bash
docker compose up -d postgres kafka
docker compose up --abort-on-container-exit --exit-code-from kafka-init kafka-init
```

If Postgres volume already existed before peer DBs were added:

```bash
docker compose exec postgres psql -U outbox -d postgres -c "CREATE DATABASE outbox_reactive;"
docker compose exec postgres psql -U outbox -d postgres -c "CREATE DATABASE outbox_vt;"
```

Recommended fair profile: warmup 50 RPS → pause → 100 RPS × 2 min (`-Dprofile=warmup`).

## 1. Servlet run

```bash
mvn -pl order-service -am package -DskipTests
java -jar order-service/target/order-service-*.jar --spring.profiles.active=dev
```

```bash
mvn -pl load-tests gatling:test \
  -Dgatling.simulationClass=com.kholodilin.outbox.loadtests.CreateOrderSimulation \
  -DbaseUrl=http://localhost:8080 \
  -Dprofile=warmup -DwarmupRps=50 -DloadRps=100 -DloadSeconds=120
```

Stop the servlet JVM before continuing. Save the Gatling report path.

## 2. Reactive run

```bash
mvn -pl order-service-reactive -am package -DskipTests
java -jar order-service-reactive/target/order-service-reactive-*.jar --spring.profiles.active=dev
```

```bash
mvn -pl load-tests gatling:test \
  -Dgatling.simulationClass=com.kholodilin.outbox.loadtests.CreateOrderReactiveSimulation \
  -DbaseUrl=http://localhost:8083 \
  -Dprofile=warmup -DwarmupRps=50 -DloadRps=100 -DloadSeconds=120
```

Stop the reactive JVM before continuing.

## 3. Virtual Threads run

```bash
mvn -pl order-service-vt -am package -DskipTests
java -jar order-service-vt/target/order-service-vt-*.jar --spring.profiles.active=dev
```

```bash
mvn -pl load-tests gatling:test \
  -Dgatling.simulationClass=com.kholodilin.outbox.loadtests.CreateOrderVtSimulation \
  -DbaseUrl=http://localhost:8084 \
  -Dprofile=warmup -DwarmupRps=50 -DloadRps=100 -DloadSeconds=120
```

## 4. Compare

| Metric | Servlet (`:8080`) | Reactive (`:8083`) | VT (`:8084`) |
|--------|-------------------|--------------------|--------------|
| Mean throughput (RPS) | | | |
| P50 / P95 / P99 (ms) | | | |
| Failed % | | | |
| Queue pressure / pool saturation | Grafana Orders Technical | Grafana Orders Technical (Reactive) | Grafana Orders Technical (Virtual Threads) |

Dashboards:

- Grafana: **Orders Technical** / **Orders Technical (Reactive)** / **Orders Technical (Virtual Threads)**
- OpenSearch: filter by `service.name` (`order-service`, `order-service-reactive`, `order-service-vt`)
