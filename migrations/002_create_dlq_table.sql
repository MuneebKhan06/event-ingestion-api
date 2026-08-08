CREATE TABLE dlq_events (
    id            BIGSERIAL PRIMARY KEY,
    event_id      UUID,                             -- may be null if payload was unparseable
    event_type    VARCHAR(50),
    raw_payload   JSONB       NOT NULL,              -- original payload, preserved as-is
    source        VARCHAR(50),
    error_reason  TEXT        NOT NULL,
    failed_at     TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX idx_dlq_events_failed_at ON dlq_events (failed_at DESC);
