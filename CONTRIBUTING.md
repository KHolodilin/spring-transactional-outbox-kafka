# Contributing

Thank you for your interest in contributing to **spring-transactional-outbox-kafka**.

## Before you start

* Read the [README](README.md) and the docs in [`docs/`](docs/).
* Check [open issues](https://github.com/KHolodilin/spring-transactional-outbox-kafka/issues) to avoid duplicate work.
* For large changes, open an issue first to discuss the approach.

## Development setup

### Prerequisites

* Java 21
* Maven 3.9+
* Docker (for PostgreSQL, Kafka, Prometheus, Grafana, and integration tests)

### Build and test

```bash
# Start infrastructure
docker compose up -d

# Full build with tests
mvn clean verify

# Build without tests (when Docker is unavailable)
mvn clean package -DskipTests
```

### Run locally

```bash
mvn -pl order-service spring-boot:run -Dspring-boot.run.profiles=dev
mvn -pl notification-stub spring-boot:run -Dspring-boot.run.profiles=dev
```

Metrics: Prometheus http://localhost:9090, Grafana http://localhost:3000 — see [README Observability](README.md#observability).

### Distributed tracing (local)

1. `docker compose up -d` — starts Tempo (OTLP HTTP `:4318`, query `:3200`).
2. Run services with profile `dev` (100% sampling, OTLP to `localhost:4318`).
3. Open Grafana → **Distributed Tracing** dashboard after creating an order.
4. Logs include `traceId` and `spanId` via `logback-spring.xml`.

Environment variables:

| Variable | Default (dev) | Purpose |
|----------|---------------|---------|
| `TRACING_ENABLED` | `true` | Kill switch (`false` disables export) |
| `TRACING_SAMPLING` | `1.0` / `0.1` prod | Trace sampling probability |
| `OTEL_EXPORTER_OTLP_ENDPOINT` | `http://localhost:4318/v1/traces` | Tempo OTLP HTTP endpoint |

Integration tests run with `management.tracing.enabled=false` — no Tempo required in CI.

### Code coverage (JaCoCo)

After `mvn clean verify`, JaCoCo XML reports are written per module:

```text
outbox-events-contract/target/site/jacoco/jacoco.xml
order-service/target/site/jacoco/jacoco.xml
notification-stub/target/site/jacoco/jacoco.xml
```

CI uploads all module reports to Codecov (merged on the Codecov side).

## Continuous integration and Codecov

Every push to `main` and every pull request runs [GitHub Actions CI](.github/workflows/ci.yml):

1. JDK 21 (Temurin), Maven cache
2. `mvn -B clean verify` (Docker required for Testcontainers integration tests)
3. Upload of per-module JaCoCo XML reports to [Codecov](https://codecov.io/gh/KHolodilin/spring-transactional-outbox-kafka)

PRs receive a Codecov comment with patch coverage. The README badge reflects the latest `main` upload.

### Repository maintainer setup (one-time)

1. Sign in at [codecov.io](https://about.codecov.io/) with GitHub and add this repository.
2. Install the **Codecov GitHub App** (PR comments and status checks).
3. Copy the repository upload token from Codecov → repo Settings.
4. Add a GitHub Actions secret named `CODECOV_TOKEN` (repo Settings → Secrets → Actions).

Without `CODECOV_TOKEN`, the CI upload step fails (`fail_ci_if_error: true`).

## Project structure

| Module | Purpose |
|--------|---------|
| `outbox-events-contract` | Shared event DTOs and enums |
| `order-service` | REST API, transactional outbox, Kafka publisher |
| `notification-stub` | Demo downstream consumer |

Package base: `com.kholodilin.outbox`.

## Coding guidelines

* Match existing code style and naming conventions.
* Keep changes focused — one logical change per pull request.
* Prefer clear, self-explanatory code over excessive comments.
* Add or update tests when behavior changes.
* Integration tests use Testcontainers (`application-test.yml`) and Embedded Kafka.

## Pull requests

1. Fork the repository and create a branch from `main`.
2. Make your changes with clear commit messages.
3. Ensure `mvn clean verify` passes (or explain why tests were skipped).
4. Open a pull request using the [PR template](.github/pull_request_template.md).
5. Link related issues (e.g. `Fixes #123`).

## Reporting bugs

Use the [bug report template](.github/ISSUE_TEMPLATE/bug_report.yml) and include:

* Steps to reproduce
* Expected vs actual behavior
* Java/Maven/Docker versions
* Relevant logs or stack traces

## Security issues

Do **not** open public issues for security vulnerabilities. See [SECURITY.md](SECURITY.md).

## Code of conduct

This project follows the [Contributor Covenant](CODE_OF_CONDUCT.md). Please be respectful and constructive.
