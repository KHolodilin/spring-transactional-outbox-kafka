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
* Docker (for PostgreSQL, Kafka, and integration tests)

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
