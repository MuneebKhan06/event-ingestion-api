import type { Route } from "../router";
import type { StageId } from "./pipeline";

export interface StageDetail {
  does: string;
  // Concrete behaviour, each traceable to the code that implements it.
  facts: string[];
  // Design decisions from the README this stage embodies.
  decisions: number[];
  screen: { route: Route; label: string };
  external?: { href: string; label: string };
}

export const STAGE_DETAILS: Record<StageId, StageDetail> = {
  ingest: {
    does: "Polls public APIs on a timer and posts what it finds to the API, so the stack always runs on real traffic.",
    facts: [
      "Every 30 s by default (INGEST_INTERVAL_SECONDS).",
      "event_id is a uuid5 of the upstream record's own ID, so a record seen again is a natural duplicate, answered 409.",
      "Per source backoff after failures or rate limits: skip 2 polls, then 4, 8, 16, up to a cap.",
      "Publishes concurrently, bounded by a semaphore (INGEST_PUBLISH_CONCURRENCY, 8).",
      "Sends its own X-Request-ID per poll, so its requests are easy to find in the API's logs.",
    ],
    decisions: [4],
    screen: { route: "metrics", label: "Ingest metrics" },
  },
  client: {
    does: "Anything that sends events over HTTP: this console, a service, a load test.",
    facts: [
      "Generates the event_id itself. That ID is the idempotency key the whole pipeline deduplicates on.",
      "Gets an X-Request-ID back on every response, matching the API's log lines for that request.",
      "Reads stored events back through GET /events, or live through the SSE stream.",
    ],
    decisions: [4],
    screen: { route: "send", label: "Send an event" },
  },
  api: {
    does: "Validates each event and publishes it to Kafka, answering as soon as the broker has it.",
    facts: [
      "Pydantic checks the body: a UUID event_id, a dotted <domain>.<action> type, a payload under 32 KB. Failures are 422.",
      "A best effort pre-check answers 409 for an event_id already stored. Optional (ENABLE_DUPLICATE_PRECHECK).",
      "Waits for the broker's acknowledgement (acks=1) before answering 202, so an accepted event is really in Kafka.",
      "Answers 503, not a bare 500, when Kafka is unreachable. Never writes to Postgres itself.",
    ],
    decisions: [1, 3, 4],
    screen: { route: "send", label: "Send an event" },
  },
  kafka: {
    does: "The durable buffer between accepting an event and storing it, so a slow database never slows the API.",
    facts: [
      "events.raw has 3 partitions, created by a kafka-init job; topic auto creation is off so nothing gets 1 partition by accident.",
      "Messages are keyed by event_id, so copies of one event land on the same partition.",
      "Delivery is at least once: a consumer restart can redeliver, which is why storage has to be idempotent.",
    ],
    decisions: [1, 2],
    screen: { route: "stream", label: "Watch events arrive" },
    external: { href: "http://localhost:8080", label: "Kafka UI on :8080" },
  },
  consumer: {
    does: "Reads events.raw and stores each event exactly once, routing anything it cannot store to the DLQ.",
    facts: [
      "Commits a Kafka offset only after the event is durably handled: stored, skipped as a duplicate, or recorded in the DLQ.",
      "Retries database failures with exponential backoff (1 s, 2 s, 4 s); malformed messages go straight to the DLQ.",
      "Rolls back a failed insert before retrying, so one rejected row cannot wedge the consumer.",
      "Serves its own Prometheus metrics on :9100, since it is a separate process from the API.",
    ],
    decisions: [4, 6],
    screen: { route: "metrics", label: "Consumer metrics" },
  },
  postgres: {
    does: "The system of record: one row per event, whatever happened upstream.",
    facts: [
      "UNIQUE on event_id with ON CONFLICT DO NOTHING. This, not the API's pre-check, is what guarantees no duplicates.",
      "payload is JSONB, so event shapes can evolve without migrations.",
      "Indexed on event_type, source and created_at DESC, matching the filters on the explorer.",
      "Schema managed by Alembic migrations, run by a one shot compose service before the app starts.",
    ],
    decisions: [4, 5],
    screen: { route: "events", label: "Explore stored events" },
  },
  dlq: {
    does: "Keeps every event the consumer gave up on, with the reason, so a failure can be inspected and replayed instead of lost.",
    facts: [
      "Written twice: the dlq_events table for inspection and the events.dlq topic for alerting.",
      "Stores the raw payload exactly as received, which is what makes replay possible.",
      "scripts/replay_dlq.py republishes valid ones to events.raw and skips any that would fail again.",
    ],
    decisions: [6],
    screen: { route: "dlq", label: "Open the DLQ" },
  },
};
