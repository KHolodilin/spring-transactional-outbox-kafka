## Summary

<!-- What does this PR change and why? -->

## Related issues

<!-- Link issues: Fixes #123, Relates to #456 -->

## Type of change

- [ ] Bug fix (non-breaking change that fixes an issue)
- [ ] New feature (non-breaking change that adds functionality)
- [ ] Breaking change (fix or feature that would cause existing behavior to change)
- [ ] Documentation update
- [ ] Refactoring / chore

## Affected modules

- [ ] `outbox-events-contract`
- [ ] `order-service`
- [ ] `notification-stub`
- [ ] `docker-compose` / infrastructure
- [ ] docs / CI / other

## Test plan

<!-- How was this tested? -->

- [ ] `mvn clean verify`
- [ ] Manual testing (describe below)

**Manual steps (if any):**

```bash
# e.g. docker compose up -d && mvn -pl order-service spring-boot:run
```

## Checklist

- [ ] Code follows existing project conventions
- [ ] Tests added or updated where appropriate
- [ ] Documentation updated (if user-facing behavior changed)
- [ ] No secrets or environment-specific credentials committed
- [ ] Liquibase/schema changes documented (if applicable)
