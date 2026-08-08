CREATE TABLE events (
    id            BIGSERIAL PRIMARY KEY,
    event_id      UUID        NOT NULL UNIQUE,     -- client-generated, idempotency key
    event_type    VARCHAR(50) NOT NULL,             -- order.created, user.clicked etc
    payload       JSONB       NOT NULL,             -- flexible event data
    source        VARCHAR(50) NOT NULL,             -- which service sent this
    status        VARCHAR(20) NOT NULL DEFAULT 'processed',
    created_at    TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    processed_at  TIMESTAMPTZ
);

CREATE INDEX idx_events_event_type ON events (event_type);
CREATE INDEX idx_events_created_at ON events (created_at DESC);
CREATE INDEX idx_events_source     ON events (source);
