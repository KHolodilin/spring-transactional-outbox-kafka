--liquibase formatted sql

--changeset outbox:005-outbox-trace-parent
ALTER TABLE outbox_events ADD COLUMN trace_parent VARCHAR(55);
