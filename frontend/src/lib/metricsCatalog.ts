import type { ProcessId } from "../hooks/useMetrics";

export interface CounterSpec {
  name: string;
  label: string;
  // When a rising value means something is wrong. Only these get the warning
  // treatment, so a busy pipeline never looks like a failing one.
  bad?: boolean;
}

export interface ProcessSpec {
  title: string;
  role: string;
  note: string;
  counters: CounterSpec[];
}

// The counters that tell the pipeline's story, per process, in reading order.
// Labelled series (dlq_writes_total by reason, ingest_* by source) are summed
// here; the breakdown screen shows them split.
export const CATALOG: Record<ProcessId, ProcessSpec> = {
  api: {
    title: "API",
    role: "Validates and publishes",
    note: "Unlabelled on purpose: event_type and source come from the request body, and labelling on them would let any caller create unbounded time series.",
    counters: [
      { name: "events_accepted_total", label: "Accepted (202)" },
      { name: "events_duplicates_total", label: "Duplicates refused (409)" },
      { name: "events_publish_failures_total", label: "Publish failures (503)", bad: true },
    ],
  },
  consumer: {
    title: "Consumer",
    role: "Stores to Postgres",
    note: "Its own endpoint because it is its own process. Events stored, duplicates dropped at insert and DLQ writes all happen here, so the API cannot count them.",
    counters: [
      { name: "events_persisted_total", label: "Stored" },
      { name: "events_duplicates_skipped_total", label: "Redeliveries dropped" },
      { name: "dlq_writes_total", label: "Sent to DLQ", bad: true },
    ],
  },
  ingest: {
    title: "Ingest poller",
    role: "Feeds real data in",
    note: "Duplicates are the normal steady state here: feeds repeat unchanged records between polls, and the API's 409 is what keeps them from being stored twice.",
    counters: [
      { name: "ingest_events_fetched_total", label: "Fetched upstream" },
      { name: "ingest_events_accepted_total", label: "Accepted by the API" },
      { name: "ingest_events_duplicate_total", label: "Already seen (409)" },
      { name: "ingest_failures_total", label: "Upstream failures", bad: true },
    ],
  },
};
