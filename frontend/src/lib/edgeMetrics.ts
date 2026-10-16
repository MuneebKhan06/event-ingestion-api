import { sumLatest, sumRatePerMinute, type MetricsState, type ProcessId } from "../hooks/useMetrics";
import type { EdgeId } from "./pipeline";

// Which counter measures each edge. Every edge is counted by the process at
// its far end, because that is the process that knows the event arrived.
// Two edges have no counter of their own: client requests are not counted
// apart from ingest ones, and SSE frames are not counted at all.
const EDGE_COUNTERS: Partial<Record<EdgeId, { process: ProcessId; metrics: string[] }>> = {
  "ingest-api": { process: "ingest", metrics: ["ingest_events_fetched_total"] },
  "api-kafka": { process: "api", metrics: ["events_accepted_total"] },
  // Everything the consumer finished with, whichever way it went.
  "kafka-consumer": {
    process: "consumer",
    metrics: ["events_persisted_total", "events_duplicates_skipped_total", "dlq_writes_total"],
  },
  "consumer-postgres": { process: "consumer", metrics: ["events_persisted_total"] },
  "consumer-dlq": { process: "consumer", metrics: ["dlq_writes_total"] },
};

export interface EdgeCount {
  total: number;
  perMinute: number | null;
}

export function edgeCounts(state: MetricsState): Partial<Record<EdgeId, EdgeCount>> {
  const out: Partial<Record<EdgeId, EdgeCount>> = {};
  for (const [edge, spec] of Object.entries(EDGE_COUNTERS) as [EdgeId, { process: ProcessId; metrics: string[] }][]) {
    const process = state[spec.process];
    if (!process.up) continue;
    const totals = spec.metrics.map((m) => sumLatest(process, m));
    if (totals.some((t) => t === undefined)) continue;
    const rates = spec.metrics.map((m) => sumRatePerMinute(process, m));
    out[edge] = {
      total: totals.reduce((a, b) => a! + b!, 0)!,
      perMinute: rates.some((r) => r === null) ? null : rates.reduce((a, b) => a! + b!, 0),
    };
  }
  return out;
}

// Two short lines rather than one long one: the gaps between stages are only
// 80 units wide, and a single "36/min · 8,069 total" ran under the boxes.
export function formatEdge({ total, perMinute }: EdgeCount): string[] {
  const rate =
    perMinute === null ? "…" : perMinute === 0 ? "idle" : `${perMinute < 10 ? perMinute.toFixed(1) : Math.round(perMinute)}/min`;
  return [rate, `${total.toLocaleString()} total`];
}
