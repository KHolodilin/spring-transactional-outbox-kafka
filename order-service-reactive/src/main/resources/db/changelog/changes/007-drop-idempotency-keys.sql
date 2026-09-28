--liquibase formatted sql

--changeset outbox:007-drop-idempotency-keys
DROP TABLE IF EXISTS idempotency_keys CASCADE;
