# Servlet vs Reactive vs Virtual Threads A/B load comparison

Compare three peer order services **sequentially** on the same machine:

| Peer | Stack | Port | DB |
|------|-------|------|-----|
| `order-service` | Tomcat + platform threads + JDBC | `:8080` | `outbox` |
| `order-service-reactive` | WebFlux + R2DBC | `:8083` | `outbox_reactive` |
| `order-service-vt` | Tomcat + Virtual Threads + JDBC | `:8084` | `outbox_vt` |

Do not run more than one service under load at once if you want a clean CPU/IO comparison. Kafka topic `orders.events` is shared.

## Docker flavors (no Maven)

Each stack is a separate compose file with its own ports, so all three can stay up. Load one at a time for a fair CPU/IO comparison. Grafana on each stack shows only that flavor's Orders Technical dashboard.

| Peer | Order | Stub | Grafana |
|------|-------|------|---------|
| servlet | `:8090` | `:8091` | `:3000` |
| reactive | `:8092` | `:8093` | `:3001` |
| vt | `:8094` | `:8095` | `:3002` |

```bash
docker compose -f docker/compose.servlet.yml --profile observability up -d
mvn -pl load-tests gatling:test \
  -Dgatling.simulationClass=com.kholodilin.outbox.loadtests.CreateOrderSimulation \
  -DbaseUrl=http://localhost:8090 \
  -Dprofile=warmup -DwarmupRps=50 -DloadRps=100 -DloadSeconds=120

docker compose -f docker/compose.reactive.yml --profile observability up -d
mvn -pl load-tests gatling:test \
  -Dgatling.simulationClass=com.kholodilin.outbox.loadtests.CreateOrderReactiveSimulation \
  -DbaseUrl=http://localhost:8092 \
  -Dprofile=warmup -DwarmupRps=50 -DloadRps=100 -DloadSeconds=120

docker compose -f docker/compose.vt.yml --profile observability up -d
mvn -pl load-tests gatling:test \
  -Dgatling.simulationClass=com.kholodilin.outbox.loadtests.CreateOrderVtSimulation \
  -DbaseUrl=http://localhost:8094 \
  -Dprofile=warmup -DwarmupRps=50 -DloadRps=100 -DloadSeconds=120
```

## Prerequisites (Maven on the host)

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

## 5. Local A/B results (with create bulkhead)

All peers use the same create concurrency bulkhead (`max-concurrent-creates: 55`), short pool acquire timeout (2s), and map pool exhaustion to HTTP 429. Gatling profile: warmup 50 RPS → pause → target RPS × 120s. KO is almost entirely intentional **429** (Gatling expects 200/201).

Where multiple runs exist at 100 RPS (servlet ×3, VT ×2, reactive ×2), values are **averages**.

| RPS | Peer | OK | KO % | P50 | P95 | P99 | mean OK RPS |
|-----|------|---:|-----:|----:|----:|----:|------------:|
| **100** | Servlet *(avg 3)* | 14,227 | 0.16% | 13 | 105 | 299 | ~77 |
| | VT *(avg 2)* | 14,218 | 0.22% | 14 | 129 | 393 | ~77 |
| | Reactive *(avg 2)* | 14,217 | 0.24% | 15 | **121** | 363 | ~77 |
| **200** | Servlet | 26,308 | 1.7% | 14 | 167 | 433 | ~141 |
| | VT | 26,510 | 0.9% | 17 | 171 | 324 | ~143 |
| | Reactive | **26,742** | **0.03%** | 18 | **89** | **255** | ~145 |
| **300** | Servlet | 38,013 | 3.2% | 26 | 210 | 443 | ~204 |
| | VT | 38,143 | 2.8% | 25 | **187** | 422 | ~206 |
| | Reactive | 38,147 | 2.8% | 50 | 203 | **336** | ~206 |
| **500** | Servlet | **55,587** | **13.5%** | **45** | **188** | **305** | **~299** |
| | VT | 49,439 | 23% | 83 | 255 | 337 | ~267 |
| | Reactive | 38,590 | 40% | 90 | 309 | 515 | ~207 |
| **1000** | Servlet | **44,643** | 65%* | 130 | **271** | 572 | **~240** |
| | VT | 42,999 | 66% | 137 | 313 | 545 | ~231 |
| | Reactive | 36,461 | 71% | 159 | 410 | 864 | ~196 |

\* Servlet @1000: almost all KO are 429; plus 183× `Connection refused` during JVM start race.

**Takeaways**

- At **100 RPS** the three peers are effectively equal; rare 429s are noise under the same bulkhead.
- At **200 RPS** reactive is best on reject rate and P95.
- Around **300 RPS** successful throughput converges (~205 OK RPS).
- From **500+** servlet leads on successful create throughput; reactive is the tightest at the bulkhead ceiling.
