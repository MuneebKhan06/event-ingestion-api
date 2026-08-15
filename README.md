# Event Ingestion API with Stream Processing

[![CI](https://github.com/MuneebKhan06/event-ingestion-api/actions/workflows/ci.yml/badge.svg)](https://github.com/MuneebKhan06/event-ingestion-api/actions/workflows/ci.yml)

A production-grade event ingestion system built with FastAPI and Apache Kafka.
Accepts high-throughput events via REST API, publishes to Kafka topics, and
processes them through consumers that persist to PostgreSQL with idempotency guarantees.

> **Domain:** E-commerce order and user activity events
> **Stack:** FastAPI · Apache Kafka · PostgreSQL · Docker Compose · Pydantic · Locust
> **Focus:** Backend + Data Engineering

---

## Table of Contents

- [System Overview](#system-overview)
- [Architecture](#architecture)
- [Project Structure](#project-structure)
- [Architecture & Design Decisions](#architecture--design-decisions)
- [Getting Started](#getting-started)
- [API Reference](#api-reference)
- [Development](#development)
- [Load Test Results](#load-test-results)
- [How I Would Scale This](#how-i-would-scale-this)
- [What I Would Do Differently](#what-i-would-do-differently)

---

## System Overview

```
Client
  |
  | HTTP POST /events
  v
FastAPI (Ingestion API)
  |
  | Pydantic validation
  | Kafka Producer
  v
Kafka Topic: events.raw  (3 partitions)
  |
  | Consumer Group: event-processors
  v
PostgreSQL (events table)
  |
  | Dead Letter Queue
  v
Kafka Topic: events.dlq  +  dlq_events table
```

---

## Architecture

### Components

| Component | Role |
|---|---|
| FastAPI | REST API, request validation, Kafka producer |
| Apache Kafka | Event streaming, decoupling producer from consumer |
| Zookeeper | Kafka cluster coordination |
| PostgreSQL | Persistent event storage |
| Kafka Consumer | Reads from Kafka, writes to PostgreSQL with idempotency |
| Dead Letter Queue | Captures failed events for inspection and replay |
| Kafka UI | Web console for browsing topics and messages (`:8080`) |
| Ingest poller | Feeds the API from public APIs so the stack runs on real data |
| Locust | Load testing — measures events/sec throughput |

---

## Project Structure

```
event-ingestion-api/
|
|-- .github/
|   |-- workflows/
|       |-- ci.yml               # Lint + tests on Python 3.10 and 3.11
|
|-- app/
|   |-- __init__.py
|   |-- main.py                  # FastAPI app entry point + lifespan wiring
|   |-- config.py                # Environment config (pydantic-settings)
|   |
|   |-- api/
|   |   |-- __init__.py
|   |   |-- routes/
|   |       |-- __init__.py
|   |       |-- events.py        # POST /events, GET /events, GET /events/{id}
|   |       |-- dlq.py           # GET /dlq
|   |       |-- metrics.py       # GET /metrics
|   |       |-- stream.py        # GET /events/stream (SSE)
|   |       |-- health.py        # GET /health
|   |   |-- middleware.py        # Request correlation IDs
|   |
|   |-- schemas/
|   |   |-- __init__.py
|   |   |-- events.py            # Pydantic models + payload size guard
|   |
|   |-- kafka/
|   |   |-- __init__.py
|   |   |-- producer.py          # Kafka producer (async, configurable acks)
|   |   |-- consumer.py          # Kafka consumer, manual offset commits
|   |   |-- topics.py            # Topic names as constants
|   |
|   |-- db/
|   |   |-- __init__.py
|   |   |-- connection.py        # Async engine, session factory, disposal
|   |   |-- models.py            # SQLAlchemy models
|   |   |-- repository.py        # DB queries (insert, get, list, idempotency)
|   |
|   |-- core/
|       |-- __init__.py
|       |-- idempotency.py       # Duplicate event detection
|       |-- metrics.py           # Prometheus counters
|       |-- dlq.py               # Dead letter queue handler
|
|-- ingest/
|   |-- __init__.py
|   |-- main.py                  # Poll loop + graceful shutdown
|   |-- sources.py               # Public API sources and parsers
|   |-- metrics.py               # Ingest-side Prometheus counters
|   |-- backoff.py               # Per source backoff after failures
|
|-- consumer/
|   |-- __init__.py
|   |-- main.py                  # Consumer entry point + graceful shutdown
|   |-- processor.py             # Event processing logic
|   |-- retry.py                 # Exponential backoff retry logic
|   |-- metrics.py               # Consumer-side Prometheus counters
|
|-- alembic/
|   |-- env.py                   # Async migration environment
|   |-- versions/
|       |-- 0001_initial_schema.py
|
|-- tests/
|   |-- __init__.py
|   |-- test_api.py              # API endpoint tests
|   |-- test_producer.py         # Kafka producer tests
|   |-- test_consumer.py         # Consumer + DLQ routing tests
|   |-- test_core.py             # DLQ handler + idempotency check tests
|   |-- test_connection.py       # Lazy engine construction tests
|   |-- test_consumer_loop.py    # Consume-loop offset-commit guarantees
|   |-- test_dlq_api.py          # GET /dlq inspection endpoint tests
|   |-- test_metrics.py          # API Prometheus counter tests
|   |-- test_consumer_metrics.py # Consumer counter tests
|   |-- test_replay.py           # DLQ replay selection tests
|   |-- test_simulate.py         # Duplicate-check CLI logic tests
|   |-- test_request_id.py       # Correlation ID middleware tests
|   |-- test_ingest.py           # Public API source parsing tests
|   |-- test_stream.py           # SSE stream and route order tests
|   |-- test_backoff.py          # Ingest backoff and rate limit tests
|   |-- test_integration.py      # End-to-end vs real Kafka + Postgres
|   |-- test_schemas.py          # Pydantic schema validation tests
|
|-- load_tests/
|   |-- locustfile.py            # Locust load test scenarios
|
|-- scripts/
|   |-- simulate_events.py       # CLI to post synthetic events
|   |-- replay_dlq.py            # CLI to replay failed events from the DLQ
|
|-- docker/
|   |-- Dockerfile.api           # FastAPI app image (with HEALTHCHECK)
|   |-- Dockerfile.consumer      # Consumer image
|   |-- Dockerfile.ingest        # Public API poller image
|
|-- docker-compose.yml           # Full local stack
|-- docker-compose.test.yml      # Infra-only stack for tests
|-- .dockerignore
|-- .env.example                 # Environment variable template
|-- alembic.ini                  # Migration configuration
|-- pyproject.toml               # Ruff configuration
|-- pytest.ini                   # Pytest configuration (asyncio auto mode)
|-- requirements.txt
|-- requirements-dev.txt
|-- DEVLOG.md                    # Build log
|-- README.md
```

---

## Architecture & Design Decisions

### Decision 1: Why Kafka over direct PostgreSQL insert

**Context:** The API needs to accept events at high throughput. The simplest approach
is to insert directly into PostgreSQL on every request.

**Options considered:**
- Option A: Direct PostgreSQL insert on every API call
- Option B: In-memory queue (Python Queue) with background workers
- Option C: Kafka as the event backbone

**Decision:** Kafka (Option C)

**Reasoning:**
Direct DB inserts couple the API's response time to database write latency. Under
load, this creates a bottleneck: slow DB = slow API = dropped requests. Kafka
decouples the producer (API) from the consumer (DB writer), meaning the API
responds as fast as Kafka acknowledgment (~2-5ms) regardless of DB write speed.
An in-memory queue (Option B) loses all events if the process crashes — no durability.

**Tradeoffs accepted:**
- Added operational complexity (Kafka + Zookeeper to run locally)
- Events are not immediately queryable from PostgreSQL (slight delay)
- At-least-once delivery means duplicate handling is required

---

### Decision 2: Kafka topic partition strategy

**Context:** How many partitions should the `events.raw` topic have?

**Options considered:**
- Option A: 1 partition (simplest, strict ordering)
- Option B: 3 partitions (moderate parallelism)
- Option C: 12 partitions (high parallelism, matches future consumer count)

**Decision:** 3 partitions for local development, with notes on scaling to 12

**Reasoning:**
Partition count determines maximum consumer parallelism — you cannot have more
active consumers in a group than partitions. 1 partition is a bottleneck.
12 partitions is over-provisioned for a single-node local setup. 3 partitions
allows 3 parallel consumers and is easy to reason about during development.
In production, I would set this to match the number of consumer instances.

Topic auto-creation is disabled in `docker-compose.yml` and a `kafka-init` job
creates the topics explicitly — auto-created topics default to 1 partition,
which would silently cap parallelism.

**Tradeoffs accepted:**
- Ordering is guaranteed per partition, not globally across topics
- Partition count cannot be reduced after creation (only increased)

---

### Decision 3: Synchronous vs async Kafka producer

**Context:** Should the Kafka producer wait for broker acknowledgment before
returning a response to the API caller?

**Options considered:**
- Option A: Fire-and-forget (no ack, lowest latency, risk of silent data loss)
- Option B: `acks=1` (leader broker acks, balanced latency vs durability)
- Option C: `acks=all` (all replicas ack, highest durability, highest latency)

**Decision:** `acks=1` for this project, with notes on when to use `acks=all`

**Reasoning:**
For an event ingestion system where occasional loss is acceptable (analytics,
clickstream), `acks=1` gives a good balance. For financial transactions or
mission-critical events, `acks=all` is the correct choice despite the latency cost.
Fire-and-forget is never appropriate for production event systems.

The value is configurable via `KAFKA_PRODUCER_ACKS` rather than hardcoded.

**Tradeoffs accepted:**
- Small risk of event loss if the leader broker fails before replication
- Slightly higher latency than fire-and-forget

---

### Decision 4: Handling duplicate events (at-least-once delivery)

**Context:** Kafka guarantees at-least-once delivery, meaning a consumer may
process the same message more than once (e.g. on consumer restart). This can
cause duplicate rows in PostgreSQL.

**Options considered:**
- Option A: Ignore duplicates (simplest, inaccurate data)
- Option B: Unique constraint on `event_id` in PostgreSQL with `ON CONFLICT DO NOTHING`
- Option C: Redis-based deduplication cache with TTL

**Decision:** PostgreSQL unique constraint on `event_id` (Option B)

**Reasoning:**
Each event carries a client-generated UUID (`event_id`). A unique constraint on
this column with `ON CONFLICT DO NOTHING` is atomic, requires no extra
infrastructure, and is the simplest correct solution. Redis (Option C) would be
faster but introduces another service dependency. For this throughput level,
PostgreSQL handles it cleanly.

The API also performs a **best-effort** pre-publish lookup so an obvious duplicate
returns `409` without a Kafka round trip. That check is deliberately not the
source of truth: two concurrent requests with the same `event_id` can both pass it,
and the unique constraint in the consumer is what actually guarantees correctness.

**That check is optional** (`ENABLE_DUPLICATE_PRECHECK`, default `true`). It
costs one database read per request, which puts PostgreSQL back on the request
path — precisely what Decision 1 set out to avoid — and load testing showed API
latency tracking database load as a result. Because the check was never the
correctness mechanism, disabling it is safe:

| | Check enabled (default) | Check disabled |
|---|---|---|
| Known duplicate | `409`, no Kafka round trip | `202`, deduplicated by the consumer |
| Database reads per request | 1 | 0 |
| Rows stored for a duplicate | 1 | 1 |

The last row is the important one: the outcome in the database is identical.
What changes is how quickly a client learns it sent a duplicate, and whether the
API's latency is exposed to database load.

**Tradeoffs accepted:**
- Slightly slower inserts due to uniqueness check
- `event_id` must be generated client-side (documented in API spec)

---

### Decision 5: PostgreSQL schema design for events

**Context:** How should the events table be structured for time-series event data?

**Schema decided:**

```sql
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
```

**Reasoning:**
JSONB payload keeps the schema flexible as event shapes evolve without
migrations. Indexes on `event_type`, `created_at`, and `source` cover the most
common query patterns (filter by type, time range queries, filter by source).
The `created_at DESC` index matches the most common access pattern: recent events first.

---

### Decision 6: Dead Letter Queue design

**Context:** What happens when a consumer fails to process an event after retries?

**Decision:** Separate Kafka topic (`events.dlq`) + `dlq_events` table in PostgreSQL

**Reasoning:**
Failed events are published to a dedicated DLQ topic instead of being discarded.
This allows: (1) inspection of what failed and why, (2) replay after fixing the
bug, (3) alerting on DLQ growth. The error reason and original payload are
preserved in full.

Transient failures (e.g. a brief DB outage) are retried with exponential backoff
first; only after retries are exhausted does an event go to the DLQ. Permanent
failures — a malformed payload or an invalid `event_id` — skip retries entirely,
since retrying them would never succeed.

**When the DLQ itself cannot be written:** the DLQ records to Postgres before
publishing, so a database outage takes out the fallback as well as the primary
path. The consumer treats that as unrecoverable for the message in hand: it logs
the topic, partition and offset, leaves the offset **uncommitted**, and exits so
the container restarts. Committing would skip the event permanently — an outage
becoming silent data loss — and continuing would strand it behind a later commit.
The `restart: unless-stopped` policy on the consumer is what makes exiting a safe
response rather than a fatal one.

**Inspecting failures:** `GET /dlq` lists what failed and why, newest first, so
this doesn't require a psql session. Its `total` is the value to alert on.

**Replaying failures:** once the underlying bug is fixed, `scripts/replay_dlq.py`
re-publishes stored failures onto `events.raw` so the normal consumer reprocesses
them. Anything that did eventually reach Postgres is deduplicated on insert by
the same unique constraint, so a replay is safe to run more than once.

```bash
python scripts/replay_dlq.py --dry-run                        # preview only
python scripts/replay_dlq.py --event-type order.created --limit 50
```

Payloads that cannot pass validation are reported and skipped rather than
republished — they are the reason the event failed originally, so replaying them
would just send them straight back to the DLQ.

---

## Getting Started

### Prerequisites
- Docker and Docker Compose installed
- Python 3.11+ (only needed for running tests/tools outside Docker)
- Git

### 1. Clone the repository

```bash
git clone https://github.com/MuneebKhan06/event-ingestion-api.git
cd event-ingestion-api
```

### 2. Set up environment variables

```bash
cp .env.example .env
# Edit .env if needed — defaults work out of the box
```

`DATABASE_URL` is derived automatically from the individual `POSTGRES_*` values,
so there is one source of truth. Set it explicitly only to override (for example,
to point at a managed database).

### 3. Start the full stack

```bash
docker-compose up -d
```

This starts Zookeeper, Kafka, PostgreSQL, the FastAPI API, the consumer, and Kafka UI.
The database schema is applied automatically by a one-shot `migrate` service
running `alembic upgrade head`; the API and consumer wait for it to complete, so
they never start against a missing or half-upgraded schema. Re-running is a
no-op when the database is already current, so restarts are safe.

### 4. Verify everything is running

```bash
docker-compose ps
curl http://localhost:8000/health
```

The API image ships with its own `HEALTHCHECK`, so `docker-compose ps` reports a
real health status rather than just "running". The API and consumer wait for
Kafka and Postgres to report healthy before starting, so a cold `up` does not
produce a burst of connection errors.

Every long-running service has `restart: unless-stopped`, so a crashed
dependency comes back on its own. This is load-bearing rather than decorative:
the consumer deliberately exits when the database is unreachable, which is only
a sound strategy if both it *and* the database restart. The two one-shot jobs
(`kafka-init`, `migrate`) have no restart policy on purpose — they are meant to
exit 0, and restarting them would break the completion gate the app services
wait on.

Note that `docker kill` will *not* demonstrate this: Docker treats manual
`kill`/`stop` as operator intent and suppresses the restart policy until the
container is started again. A crash the daemon didn't request — the process
dying on its own — is what the policy responds to.

| Service | URL |
|---|---|
| API | http://localhost:8000 |
| API docs (Swagger) | http://localhost:8000/docs |
| Kafka UI | http://localhost:8080 |
| PostgreSQL | `localhost:5432` |
| Kafka (host listener) | `localhost:9092` |

### 5. Send a test event

```bash
curl -X POST http://localhost:8000/events \
  -H "Content-Type: application/json" \
  -d '{
    "event_id": "550e8400-e29b-41d4-a716-446655440000",
    "event_type": "order.created",
    "source": "order-service",
    "payload": {
      "order_id": "ORD-001",
      "user_id": "USR-123",
      "amount": 99.99,
      "currency": "USD"
    }
  }'
```

Then read it back once the consumer has persisted it:

```bash
curl http://localhost:8000/events/550e8400-e29b-41d4-a716-446655440000
```

### 6. Real data, automatically

`docker-compose up -d` also starts an `ingest` service that polls public APIs
and posts what it finds to `POST /events`, so the pipeline has a continuous
supply of real traffic without anyone driving it.

| Source | API | Event type |
|---|---|---|
| `usgs` | USGS earthquake feed (last hour) | `quake.detected` |
| `github` | GitHub public events | `github.<action>` |
| `weather` | Open-Meteo current conditions | `weather.sampled` |

All three are public and need no credentials. Choose which run with
`INGEST_SOURCES` (default `usgs,weather`) and how often with
`INGEST_INTERVAL_SECONDS`.

A source that fails is skipped for a growing number of polls (2, 4, 8, capped at
32) and returns to normal on its first success. Rate limiting is treated
separately from ordinary failure: `429`, and the `403` with an exhausted
`X-RateLimit-Remaining` that GitHub uses, are recognised as such, and a
`Retry-After` in seconds is honoured over the local guess. Without this a 30
second interval would keep hitting an exhausted GitHub quota around 120 times an
hour to no purpose, so `github` is safe to enable but still worth a longer
interval.

Events within a poll are published concurrently up to
`INGEST_PUBLISH_CONCURRENCY` (default 8). Order does not matter, since events
are independent and Kafka partitions by `event_id`, but the bound does: a
30 record feed would otherwise mean 30 sequential round trips, and an unbounded
one would open a connection per record.

Each event's `event_id` is derived from the upstream record's own identifier, so
polling the same feed repeatedly re-sends unchanged records. Those arrive as
duplicates and are dropped by the unique constraint, which means the idempotency
guarantee is exercised continuously by real data rather than only in tests.
Watch it happen:

```bash
docker-compose logs -f ingest
curl "http://localhost:8000/events?event_type=quake.detected&limit=5"
```

### 7. Send synthetic events

```bash
python scripts/simulate_events.py --count 10
python scripts/simulate_events.py --count 5 --event-type order.created
python scripts/simulate_events.py --duplicate   # exercises the 409 path
```

`--duplicate` sends an event, **waits for the consumer to persist it**, then
resends the same `event_id`. The wait is the point: ingestion is asynchronous,
so resending immediately just races the consumer and gets another `202`, which
demonstrates nothing. If the first copy never lands within `--duplicate-timeout`
(default 10s) it says so and exits non-zero rather than reporting a pass.

---

## API Reference

### `POST /events`
Ingest a new event.

**Request body:**
```json
{
  "event_id": "uuid-v4",
  "event_type": "order.created",
  "source": "order-service",
  "payload": {}
}
```

Validation rules: `event_id` must be a valid UUID, `event_type` must be in
`<domain>.<action>` form, and `payload` must serialise to under 32 KB.

**Responses:**
- `202 Accepted` — event published to Kafka
- `409 Conflict` — duplicate `event_id`
- `422 Unprocessable Entity` — validation error
- `503 Service Unavailable` — the event could not be published to Kafka

The `503` matters as a distinct case: `4xx` tells a client its request was
wrong and resending won't help, whereas `503` means the request was fine and the
pipeline was not. Collapsing that into a `500` leaves callers unable to tell
which of the two happened, and retrying is the correct response to only one.

The `409` depends on `ENABLE_DUPLICATE_PRECHECK` (default `true`). With it
disabled, a duplicate receives `202` instead and is deduplicated later by the
consumer. The stored data is the same either way — see Decision 4.

---

### `GET /events`
List stored events, newest first.

**Query parameters:**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `event_type` | string | — | Exact match, e.g. `order.created` |
| `source` | string | — | Exact match, e.g. `order-service` |
| `since` | datetime | — | Only events created at or after this |
| `until` | datetime | — | Only events created at or before this |
| `limit` | int | `50` | Page size, capped at 200 |
| `offset` | int | `0` | Rows to skip |

```bash
curl "http://localhost:8000/events?event_type=order.created&limit=10"
```

**Response:**
```json
{
  "total": 57,
  "limit": 10,
  "offset": 0,
  "events": [ ... ]
}
```

The filters map exactly onto the three indexes created in migration 001, and
results are ordered `created_at DESC` to match `idx_events_created_at` rather
than forcing a sort. `limit` is capped so a single request cannot ask for the
whole table.

---

### `GET /events/{event_id}`
Retrieve a processed event by ID.

**Responses:**
- `200 OK` — event found
- `404 Not Found` — event not found or not yet processed

Ingestion is asynchronous, so an event may legitimately return `404` for a short
window after it is accepted.

---

### `GET /events/stream`
Server-Sent Events feed of events as they are persisted.

```bash
curl -N http://localhost:8000/events/stream
curl -N "http://localhost:8000/events/stream?after=1200"   # resume from an id
```

Starts from the newest event unless `after` is given, so a fresh connection
shows what is arriving now rather than replaying history. Frames are
`data: {json}` with the same shape as `GET /events`; idle connections receive a
`: keepalive` comment so proxies do not drop them.

Paging is by `id` rather than `created_at`: ids are unique and strictly
increasing, so a cursor cannot skip or repeat a row, whereas two events sharing
a timestamp would make a time cursor ambiguous.

It reads what the consumer has already written rather than tapping Kafka, so
what a client sees is exactly what is durably stored. Each poll uses a
short-lived database session rather than holding one open for the life of the
connection, which would tie up a pooled connection per viewer.

Concurrent streams are capped by `STREAM_MAX_CLIENTS` (default 20), and a
client over the limit gets `503` before the response starts rather than a
stream that opens and dies. The cap exists because every open connection polls
the database once a second, so streams multiply database load rather than just
consuming sockets.

That poll interval carries a small random jitter. Without it every client
settles into the same rhythm and their queries arrive together in bursts with
idle gaps between, which gets worse with each client added.

---

### `GET /dlq`
List failed events, newest first.

**Query parameters:** `event_type`, `source`, `limit` (default `50`, max 200),
`offset` (default `0`).

```bash
curl "http://localhost:8000/dlq?limit=10"
```

**Response:**
```json
{
  "total": 12,
  "limit": 10,
  "offset": 0,
  "events": [
    {
      "id": 1,
      "event_id": "550e8400-e29b-41d4-a716-446655440000",
      "event_type": "order.created",
      "source": "order-service",
      "raw_payload": { "order_id": "ORD-001" },
      "error_reason": "DB insert failed after retries",
      "failed_at": "2026-08-11T09:14:02Z"
    }
  ]
}
```

Newest first, because the question being asked is usually "what is failing right
now". Rows whose payload was unparseable have `null` for `event_id`,
`event_type` and `source` — the original payload is still preserved in full.
`total` is the figure to alert on for DLQ growth.

---

### `GET /metrics`
Prometheus exposition for the API process.

```bash
curl http://localhost:8000/metrics
```

| Metric | Meaning |
|---|---|
| `events_accepted_total` | Events validated and published to Kafka (202) |
| `events_duplicates_total` | Events rejected by the duplicate check (409) |
| `events_publish_failures_total` | Events that could not be published to Kafka (503) |

Plus the standard `process_*` and `python_*` metrics.

Two things this endpoint deliberately does **not** do:

- **No labels.** Breaking these down by `event_type` or `source` is the obvious
  next step, and it's a trap: both are client-supplied, `event_type` is any
  dotted string, and Prometheus allocates a time series per label combination.
  A buggy or hostile caller could mint unbounded series. That breakdown belongs
  in a query over the events table.
- **No consumer metrics here.** The API and consumer are separate processes
  with separate registries, so what the consumer does is exposed on its own
  endpoint (below) rather than this one.

The endpoint reads only in-memory counters, so a scrape can't fail because Kafka
or PostgreSQL is unhealthy — which is when metrics matter most. Dependency
liveness is `/health`'s job.

#### Request correlation IDs

Every API response carries an `X-Request-ID`, and every log line emitted while
handling that request is prefixed with it:

```
INFO:app.api.routes.events:[trace-me-abc] Published event 47d96ce5-... to events.raw
```

Send your own to correlate across services; otherwise one is generated. The
ingest poller does this: every request in one poll carries the same
`ingest-<source>-<hex>` id, so a poll and the API log lines it produced join
with one grep. Note that the duplicate path (409) logs nothing by design, since
a steady state poller re-sending unchanged records would otherwise flood the
log; the accepted and failed paths both carry the id.

```bash
curl -H "X-Request-ID: my-trace-001" http://localhost:8000/events/<id>
```

A supplied ID must match `[A-Za-z0-9._:-]{1,64}` — it goes straight into the
logs, so a newline would let a caller forge log lines and an unbounded value
would bloat every record. Anything else is replaced with a generated ID rather
than quietly rewritten, so the value echoed back is always one the caller
either sent or can recognise as not theirs.

Metrics and correlation IDs answer different questions: `/metrics` tells you
*how many* requests are failing, an ID lets you follow *one*.

This covers the API process only. The consumer runs separately with no request
context, so its logs carry no ID. Threading one through would mean adding it to
the Kafka message — a schema change, and a deliberate decision rather than
something to slip in here.

#### Ingest metrics (`:9200/metrics`)

| Metric | Meaning |
|---|---|
| `ingest_events_fetched_total{source}` | Records parsed from an upstream response |
| `ingest_events_accepted_total{source}` | Accepted by the API (202) |
| `ingest_events_duplicate_total{source}` | Already seen (409) |
| `ingest_events_rejected_total{source}` | Refused for any other reason |
| `ingest_failures_total{source}` | Fetches or publishes that raised |
| `ingest_backoff_skips_total{source}` | Polls skipped while backing off |

Labelled by `source`, which is safe here because the names come from a fixed
dict in this repository rather than from anything upstream sends. Duplicates get
their own counter instead of being folded into failures: feeds repeat unchanged
records between polls, so for this process duplicates are the healthy steady
state and counting them as errors would hide the rate that matters.

#### Consumer metrics (`:9100/metrics`)

The consumer serves its own exposition, since it is a separate process:

```bash
curl http://localhost:9100/metrics
```

| Metric | Meaning |
|---|---|
| `events_persisted_total` | Events inserted into PostgreSQL |
| `events_duplicates_skipped_total` | Redeliveries dropped by the unique constraint |
| `dlq_writes_total{reason}` | Events routed to the DLQ, by reason |

`dlq_writes_total` **is** labelled, unlike the API's counters, and the
difference is deliberate: `reason` comes from a fixed set defined in code
(`missing_fields`, `invalid_event_id`, `retry_exhausted`), whereas `event_type`
would be attacker-controlled. Labels are fine when the value space is yours. All
three series are initialised at zero so alerting expressions referencing them
don't break by only existing after the first failure.

Duplicates are counted separately from persists rather than as errors:
at-least-once delivery makes redeliveries normal, so a rising duplicate rate is
information, not breakage.

If the metrics port is already bound the consumer logs the failure and carries
on without it — losing metrics should cost observability, not availability.

---

### `GET /health`
Health check endpoint.

**Response:**
```json
{
  "status": "healthy",
  "kafka": "connected",
  "database": "connected"
}
```

**Responses:**
- `200 OK` — all dependencies reachable
- `503 Service Unavailable` — something is degraded; the body says what

| Field | Values |
|---|---|
| `status` | `healthy`, `degraded` |
| `kafka` | `connected`, `disconnected` |
| `database` | `connected`, `unreachable`, `query_failed` |

The status code matters as much as the body: the image's `HEALTHCHECK`, load
balancers and orchestrator probes all decide on the code alone, so a degraded
instance has to answer non-2xx to be taken out of rotation.

The `database` field separates two failures that need different responses.
`unreachable` means the connection itself failed — check host, port,
credentials. `query_failed` means the connection succeeded but the query did
not, which points at schema or permissions instead. Reporting both as a single
value sends whoever is on call to the wrong half of the system.

The `kafka` field is a weaker signal than the database one, deliberately: it
reports whether the producer was started, not whether the broker is reachable
right now, because a real broker round trip on every scrape would make the
probe expensive and flappy. The tradeoff is that a broker which died after
startup still reads as `connected` here — `events_publish_failures_total` on
`/metrics` is the honest signal for that.

---

## Development

Set up a local environment for tests and tooling:

```bash
python -m venv .venv
source .venv/bin/activate
pip install -r requirements-dev.txt
```

### Run tests

```bash
pytest tests/
```

138 unit tests covering schema validation, the Kafka producer, consumer processing and
DLQ routing, the DLQ handler and idempotency check, DLQ replay selection, and the API endpoints
(including the degraded-health path). They use mocks throughout, so no running
Kafka or PostgreSQL is required.

### Integration tests

A further 9 tests exercise real Kafka and PostgreSQL rather than mocks —
`docker-compose.test.yml` brings up the infrastructure only, on shifted ports
(`9093`/`5433`) so it can run alongside the dev stack:

```bash
docker-compose -f docker-compose.test.yml up -d
pytest tests/ -m integration
docker-compose -f docker-compose.test.yml down -v
```

They skip automatically when the stack isn't reachable, so the default `pytest
tests/` and CI stay green without Docker.

These cover what mocks structurally cannot: the JSONB payload round trip, the
producer and consumer actually agreeing on serialization across a real broker,
`events.raw` really having 3 partitions, and — most importantly — the
`ON CONFLICT DO NOTHING` idempotency guarantee hitting the real unique
constraint, where a mock could only ever confirm that we called it.

### Lint

```bash
ruff check app/ consumer/ ingest/ tests/ load_tests/ scripts/ alembic/
```

### Database migrations

Schema changes go through Alembic. `alembic/versions/` is the single source of
truth — there is no separate SQL to keep in sync.

```bash
alembic upgrade head          # apply everything (what the migrate service runs)
alembic revision -m "add x"   # new revision; --autogenerate to diff the models
alembic downgrade -1          # step back one
```

`alembic/env.py` takes its URL from the same `app.config` settings the
application uses, so migrations and the app cannot be pointed at different
databases by editing one and forgetting the other. It also sets
`compare_type=True`, so autogenerate reports column-type drift rather than
silently ignoring it.

The schema was previously seeded by SQL files mounted into Postgres's
`/docker-entrypoint-initdb.d`. That only runs against an *empty* data
directory, so it could create a schema but never change one — evolving an
existing database would have meant destroying its data.

### Continuous integration

`.github/workflows/ci.yml` runs the lint and test steps above on every push and
pull request, across Python 3.10 and 3.11. The 3.10 entry is not padding: the
codebase intentionally avoids 3.11-only idioms, and without a 3.10 job that
compatibility would quietly rot.

Ruff's `target-version` is pinned to `py310` for the same reason — it is the
supported floor, not the version development happens on. Pointing it at 3.11
made ruff offer to rewrite code into 3.11-only forms that the 3.10 job rejects.

### Run load tests

```bash
locust -f load_tests/locustfile.py --host=http://localhost:8000
# Open http://localhost:8089 to configure and start the test
```

---

## Load Test Results

> Measured against the full `docker-compose.yml` stack. **Indicative, not a
> benchmark** — see the caveats below before drawing conclusions from these.
>
> Setup: 8-core Intel i7-1165G7, 31 GB RAM (~10 GB free), Linux 6.8, Docker
> Compose single-node. Locust ran on the *same machine* as the stack, so client
> and server contend for the same CPUs. 60-second runs, one run per level.

| Concurrent Users | Requests/sec | Avg Latency | P95 Latency | Error Rate |
|---|---|---|---|---|
| 10 | 30.5 | 18 ms | 42 ms | 0.00% |
| 50 | 62.5 | 476 ms | 840 ms | 0.00% |
| 100 | 162.9 | 296 ms | 590 ms | 0.00% |
| 200 | 165.1 | 876 ms | 1500 ms | 0.00% |

**No failed requests at any level** — 25,134 requests total, zero errors, zero
DLQ entries, zero publish failures.

**Throughput saturates near 165 req/s.** Going from 100 to 200 users moved
throughput by under 2% (162.9 → 165.1) while average latency tripled (296 ms →
876 ms). That is the signature of a saturated system: the extra load queues
rather than getting served, so latency absorbs it.

**The 50-user row is anomalous and I am not going to smooth it over.** It shows
*worse* latency than the 100-user run at a third of the throughput, which is not
what a well-behaved curve does. With one 60-second run per level on a contended
machine, it may simply be noise. A plausible mechanism, unverified: `POST
/events` does a duplicate-check `SELECT` against Postgres on every request, so
API latency is coupled to database load — and during that run the consumer was
still draining the backlog from the previous one, hammering the same database.
That coupling partially undercuts Decision 1's goal of decoupling API response
time from database work. Confirming it would need repeated runs with the
consumer stopped, which I have not done.

**End-to-end integrity held under load.** The API accepted 24,193 events and
PostgreSQL finished with 24,192 rows. The difference is exactly one: a
deliberate duplicate sent during the smoke test, where both copies were accepted
(the pre-publish check is best-effort by design) and `ON CONFLICT DO NOTHING`
collapsed them into a single row — Decision 4 working as documented, under real
load. The consumer trailed the API by roughly 4,700 events at peak and drained
in about 30 seconds afterwards; that lag is the asynchrony Decision 1 buys, not
loss.

---

## How I Would Scale This

**Current:** Single Kafka broker, single consumer, single PostgreSQL node.
Measured at roughly **165 requests/sec** before latency starts absorbing
additional load (see above).

**To 10x throughput:**
- Increase Kafka partitions from 3 to 12
- Run 12 consumer instances (one per partition)
- Add PostgreSQL connection pooling via PgBouncer
- Reason: bottleneck shifts from Kafka to consumer parallelism at this scale

**To 100x throughput:**
- Kafka cluster: 3 brokers with replication factor 3
- Consumer instances autoscaled via Kubernetes HPA on consumer lag metric
- PostgreSQL: switch to batch inserts (`COPY`) instead of row-by-row
- Consider partitioning the events table by `created_at` (monthly partitions)
- Reason: single PostgreSQL node becomes the bottleneck at this point

**Identified bottleneck at scale:**
PostgreSQL write throughput. At very high scale, the events table would need
to be replaced with a columnar store (ClickHouse) or a lakehouse (Iceberg on MinIO)
for analytics queries, keeping PostgreSQL only for operational lookups.

---

## What I Would Do Differently

If starting this project over, I would implement a schema registry (Confluent Schema
Registry or AWS Glue) from day one. Currently, event schemas are validated only at
the API layer with Pydantic. If a producer sends a new event shape that the consumer
does not expect, the consumer fails into the DLQ. A schema registry enforces
contracts between producers and consumers at the Kafka level, preventing this class
of bug entirely. I did not include it here to keep the local setup simple, but in a
production system it would be non-negotiable.

See [DEVLOG.md](DEVLOG.md) for a chronological build log.
