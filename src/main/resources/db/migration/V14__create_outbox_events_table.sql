-- =============================================================
-- V14: Transactional outbox for Kafka events.
--
-- An event row is inserted in the SAME transaction as the business change (e.g. the ride
-- becoming COMPLETED), so either both are committed or neither is. A background publisher
-- (OutboxPublisher) sends PENDING rows to Kafka and marks them PUBLISHED only after the
-- broker acknowledges the write. Failed sends stay PENDING with a later next_attempt_at.
--
-- Delivery is at-least-once: a crash after the Kafka ack but before the row is marked
-- PUBLISHED re-sends the event, so consumers must be idempotent (payments are: one per ride).
-- =============================================================

CREATE TABLE outbox_events (
    id              BIGSERIAL       PRIMARY KEY,
    topic           VARCHAR(100)    NOT NULL,
    event_key       VARCHAR(100)    NOT NULL,
    payload_type    VARCHAR(255)    NOT NULL,   -- Java class of the payload (JSON contract on the topic)
    payload         TEXT            NOT NULL,   -- JSON
    status          VARCHAR(20)     NOT NULL DEFAULT 'PENDING',   -- PENDING, PUBLISHED
    attempts        INT             NOT NULL DEFAULT 0,
    next_attempt_at TIMESTAMP       NOT NULL DEFAULT NOW(),
    last_error      VARCHAR(1000),
    created_at      TIMESTAMP       NOT NULL DEFAULT NOW(),
    published_at    TIMESTAMP
);

-- The publisher only ever scans due PENDING rows
CREATE INDEX idx_outbox_events_pending ON outbox_events (next_attempt_at, id) WHERE status = 'PENDING';
