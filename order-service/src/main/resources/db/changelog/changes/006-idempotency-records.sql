--liquibase formatted sql

--changeset outbox:006-drop-idempotency-keys
DROP TABLE IF EXISTS idempotency_keys CASCADE;

--changeset outbox:006-idempotency-records
CREATE TABLE idempotency_records (
    operation        VARCHAR(128)  NOT NULL,
    idempotency_key  VARCHAR(255)  NOT NULL,
    request_hash     VARCHAR(128)  NOT NULL,
    status           VARCHAR(32)   NOT NULL,
    result_type      VARCHAR(255),
    result_payload   JSONB,
    error_code       VARCHAR(128),
    created_at       TIMESTAMPTZ   NOT NULL,
    completed_at     TIMESTAMPTZ,
    expires_at       TIMESTAMPTZ,
    PRIMARY KEY (operation, idempotency_key)
);

CREATE INDEX idx_idempotency_records_expires_at ON idempotency_records (expires_at);
