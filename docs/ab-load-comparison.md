# Servlet vs Reactive A/B load comparison

Compare `order-service` (Tomcat/JDBC, `:8080`) and `order-service-reactive` (WebFlux/R2DBC, `:8083`) **sequentially** on the same machine. Databases are separate (`outbox` vs `outbox_reactive`) on the same Postgres server; do not run both services under load at once if you want a clean CPU/IO comparison.

## Prerequisites

```bash
docker compose up -d postgres kafka
docker compose up --abort-on-container-exit --exit-code-from kafka-init kafka-init
```

If Postgres volume already existed before `outbox_reactive` was added:

```bash
docker compose exec postgres psql -U outbox -d postgres -c "CREATE DATABASE outbox_reactive;"
```

## 1. Servlet run

```bash
mvn -pl order-service -am package -DskipTests
java -jar order-service/target/order-service-*.jar --spring.profiles.active=dev
```

```bash
mvn -pl load-tests gatling:test \
  -Dgatling.simulationClass=com.kholodilin.outbox.loadtests.CreateOrderSimulation \
  -DbaseUrl=http://localhost:8080 \
  -Drps1=100 -Drps2=100 -Drps3=100 -Drps4=100 \
  -DstageDurationSeconds=60 -DrampSeconds=15
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
  -Drps1=100 -Drps2=100 -Drps3=100 -Drps4=100 \
  -DstageDurationSeconds=60 -DrampSeconds=15
```

## 3. Compare

| Metric | Servlet (`:8080`) | Reactive (`:8083`) |
|--------|-------------------|--------------------|
| Mean throughput (RPS) | | |
| P50 / P95 / P99 (ms) | | |
| Failed % | | |
| Queue pressure / pool saturation | Grafana Orders Technical | Grafana Orders Technical (Reactive) |

Dashboards:

- Grafana: **Orders Technical** vs **Orders Technical (Reactive)**
- OpenSearch: **Orders Technical (Reactive) Logs** (`service.name: order-service-reactive`)
