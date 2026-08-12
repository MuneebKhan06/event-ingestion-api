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
|   |       |-- health.py        # GET /health
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
|-- consumer/
|   |-- __init__.py
|   |-- main.py                  # Consumer entry point + graceful shutdown
|   |-- processor.py             # Event processing logic
|   |-- retry.py                 # Exponential backoff retry logic
|
|-- migrations/
|   |-- 001_create_events_table.sql
|   |-- 002_create_dlq_table.sql
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
|   |-- test_metrics.py          # Prometheus counter tests
|   |-- test_replay.py           # DLQ replay selection tests
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
|
|-- docker-compose.yml           # Full local stack
|-- docker-compose.test.yml      # Infra-only stack for tests
|-- .dockerignore
|-- .env.example                 # Environment variable template
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
The database schema is applied automatically on first start — the `migrations/`
directory is mounted into the Postgres init directory and runs in filename order.

### 4. Verify everything is running

```bash
docker-compose ps
curl http://localhost:8000/health
```

The API image ships with its own `HEALTHCHECK`, so `docker-compose ps` reports a
real health status rather than just "running". The API and consumer wait for
Kafka and Postgres to report healthy before starting, so a cold `up` does not
produce a burst of connection errors.

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

### 6. Send synthetic events

```bash
python scripts/simulate_events.py --count 10
python scripts/simulate_events.py --count 5 --event-type order.created
python scripts/simulate_events.py --duplicate   # exercises the 409 path
```

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

Plus the standard `process_*` and `python_*` metrics.

Two things this endpoint deliberately does **not** do:

- **No labels.** Breaking these down by `event_type` or `source` is the obvious
  next step, and it's a trap: both are client-supplied, `event_type` is any
  dotted string, and Prometheus allocates a time series per label combination.
  A buggy or hostile caller could mint unbounded series. That breakdown belongs
  in a query over the events table.
- **No consumer metrics.** The API and consumer are separate processes, so
  events persisted, duplicates skipped at insert, and DLQ writes all happen
  elsewhere and are not visible here. Exposing them means giving the consumer
  its own exposition endpoint. Until then, DLQ depth is available as `total`
  from `GET /dlq`.

The endpoint reads only in-memory counters, so a scrape can't fail because Kafka
or PostgreSQL is unhealthy — which is when metrics matter most. Dependency
liveness is `/health`'s job.

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
- `503 Service Unavailable` — Kafka or the database is unreachable; the body
  reports `"status": "degraded"` and which dependency is down

The status code matters as much as the body: the image's `HEALTHCHECK`, load
balancers and orchestrator probes all decide on the code alone, so a degraded
instance has to answer non-2xx to be taken out of rotation.

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

51 unit tests covering schema validation, the Kafka producer, consumer processing and
DLQ routing, the DLQ handler and idempotency check, DLQ replay selection, and the API endpoints
(including the degraded-health path). They use mocks throughout, so no running
Kafka or PostgreSQL is required.

### Integration tests

A further 6 tests exercise real Kafka and PostgreSQL rather than mocks —
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
ruff check app/ consumer/ tests/ load_tests/ scripts/
```

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

> **Not yet measured.** The Locust scenarios are written and verified to parse,
> but no run has been executed against a live stack, so there are no real numbers
> to report yet. The table below is the intended shape of the results and will be
> filled in once the stack has actually been driven under load.

| Concurrent Users | Requests/sec | Avg Latency | P95 Latency | Error Rate |
|---|---|---|---|---|
| 10 | not measured | not measured | not measured | not measured |
| 50 | not measured | not measured | not measured | not measured |
| 100 | not measured | not measured | not measured | not measured |
| 200 | not measured | not measured | not measured | not measured |

---

## How I Would Scale This

**Current:** Single Kafka broker, single consumer, single PostgreSQL node.

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
