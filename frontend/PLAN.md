# Frontend Plan

A web console for the event ingestion stack. The goal is a frontend that
explains the system while it shows it working: every screen maps to one part of
the pipeline (API, Kafka, consumer, Postgres, DLQ, ingest poller) and says what
that part is doing and why it was built that way.

## Stack

| Choice | Why |
|---|---|
| Vite + React + TypeScript | Fast dev loop, typed models that mirror the Pydantic schemas |
| Plain CSS with design tokens | No framework to learn for a reviewer, light and dark themes |
| Vite dev proxy | One browser origin for API, consumer and ingest metrics, so the backend needs no CORS change |
| Native `EventSource` | `GET /events/stream` is already SSE, no websocket layer needed |

## Screens

1. **Overview**: health of Kafka and Postgres, headline counters, the live feed.
2. **Live stream**: events as the consumer persists them, via SSE.
3. **Events explorer**: `GET /events` with filters, pagination and a detail view.
4. **Send event**: a composer for `POST /events` that demonstrates 202, 409 and 422.
5. **Dead letter queue**: `GET /dlq` with error reasons and raw payloads.
6. **Metrics**: Prometheus counters from the API, consumer and ingest poller.
7. **How it works**: an annotated pipeline diagram linking each stage to its screen.

## Seven day schedule

### Day 1: Foundations (done)
- Scaffold Vite, React and TypeScript under `frontend/`
- Typed API client that mirrors `app/schemas/events.py` and captures `X-Request-ID`
- App shell with sidebar navigation and theme tokens
- Health panel polling `GET /health`
- Live stream hook and feed on `GET /events/stream`

### Day 2: Events explorer (done)
- Table over `GET /events` with `event_type`, `source`, `since`, `until` filters
- Offset pagination driven by `total`, `limit`, `offset`
- Detail drawer for `GET /events/{event_id}` with formatted payload
- Filters kept in the URL so a view can be shared

### Day 3: Send event (done)
- Composer with presets (order, user activity, cart, payment) and a JSON payload editor
- "Break one rule" presets that each trigger a single 422 from the server
- Responses explained in place: 202 accepted, 409 duplicate, 422 validation, 503 unavailable
- "Send again" button that reuses the `event_id` to show the duplicate path
- Race demo: concurrent copies all get 202, the unique constraint keeps one row
- Follow an accepted event until it appears in Postgres

### Day 4: Dead letter queue (done)
- `scripts/seed_dlq.sh` publishes one message per consumer failure path, so the DLQ has real data
- Fixed a consumer crash loop the seed exposed: failed writes now roll back, and overlong fields fit the DLQ row
- `GET /dlq` table with source and type filters
- Error reason badges and raw payload viewer
- Replay verdict per event, matching what `replay_dlq.py` would skip
- Panel explaining the retry policy and the `scripts/replay_dlq.py` replay flow

### Day 5: Metrics (done)
- Small Prometheus text parser
- Counters from API (`/metrics`), consumer (`:9100`) and ingest (`:9200`)
- Rates computed from successive scrapes, sparklines per counter
- Per source breakdown for the ingest poller

### Day 6: How it works
- Pipeline diagram (API, Kafka, consumer, Postgres, DLQ) with live counts on each edge
- Guided demo mode that walks a viewer through sending, duplicating and failing an event
- Short explanations of the design decisions from the README next to each stage

### Day 7: Ship
- Docker image (nginx serving the build and proxying the backends) and a compose service
- Empty, loading and error states reviewed on every screen
- Responsive layout pass for narrow screens
- README section with screenshots and run instructions

## Running

```bash
cd frontend
npm install
npm run dev        # http://localhost:5173, expects the compose stack on its default ports
```

If a backend is published on a different port, point the proxy at it:

```bash
API_URL=http://localhost:8001 npm run dev
```

`CONSUMER_METRICS_URL` and `INGEST_METRICS_URL` do the same for `:9100` and `:9200`.
