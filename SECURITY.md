# Security Policy

## Supported Versions

Security fixes are provided for the latest release on the `main` branch.

| Version | Supported |
| ------- | --------- |
| latest `main` | :white_check_mark: |
| older tags / forks | :x: |

## Reporting a Vulnerability

**Please do not report security vulnerabilities through public GitHub issues.**

If you discover a security issue, report it privately using one of the following:

1. **GitHub Security Advisories** (preferred):  
   [Report a vulnerability](https://github.com/KHolodilin/spring-transactional-outbox-kafka/security/advisories/new)

2. **Direct contact**: open a minimal issue asking for a private security contact channel if GitHub advisories are unavailable.

### What to include

* Description of the vulnerability and potential impact
* Steps to reproduce (proof of concept if possible)
* Affected module (`order-service`, `notification-stub`, infrastructure)
* Your environment (Java version, deployment mode)

### What to expect

* **Acknowledgement** within 7 days
* **Status update** on investigation progress
* **Coordinated disclosure** — we will agree on a timeline before any public disclosure

## Scope

In scope:

* Authentication, authorization, and input validation flaws in project services
* SQL injection, deserialization, or injection issues in application code
* Misconfigurations documented as insecure defaults in this repository
* Dependency vulnerabilities directly affecting this project's attack surface

Out of scope:

* Denial-of-service against demo/local deployments without a realistic production impact
* Issues in third-party services (PostgreSQL, Kafka, Docker) unless introduced by this project's configuration
* Social engineering or physical attacks

## Secure development

Contributors should:

* Never commit secrets, credentials, or production connection strings
* Run `mvn clean verify` before submitting changes
* Follow [CONTRIBUTING.md](CONTRIBUTING.md) for dependency and configuration updates

Thank you for helping keep this project secure.
