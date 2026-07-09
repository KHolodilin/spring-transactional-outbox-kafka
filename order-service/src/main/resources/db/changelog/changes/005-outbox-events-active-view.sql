--liquibase formatted sql

--changeset outbox:005-outbox-events-active-view
CREATE OR REPLACE VIEW outbox_events_active AS
SELECT * FROM outbox_events WHERE partition_state = 'ACTIVE';
