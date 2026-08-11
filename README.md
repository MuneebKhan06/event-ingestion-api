# Event Ingestion API with Stream Processing

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
|   |-- test_schemas.py          # Pydantic schema validation tests
|
|-- load_tests/
|   |-- locustfile.py            # Locust load test scenarios
|
|-- scripts/
|   |-- simulate_events.py       # CLI to post synthetic events
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

28 tests covering schema validation, the Kafka producer, consumer processing and
DLQ routing, the DLQ handler and idempotency check, and the API endpoints
(including the degraded-health path). They use mocks throughout, so no running
Kafka or PostgreSQL is required.

For integration work against real infrastructure, `docker-compose.test.yml`
brings up Kafka and Postgres only (on shifted ports `9093`/`5433`, so it can run
alongside the dev stack):

```bash
docker-compose -f docker-compose.test.yml up -d
```

### Lint

```bash
ruff check app/ consumer/ tests/ load_tests/ scripts/
```

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
