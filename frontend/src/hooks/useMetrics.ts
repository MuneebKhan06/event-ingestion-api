import { useEffect, useState } from "react";
import { API_BASE, CONSUMER_BASE, fetchMetricsText, INGEST_BASE } from "../api/client";
import { isProjectCounter, parseHelp, parsePrometheus, seriesKey } from "../lib/prometheus";

export type ProcessId = "api" | "consumer" | "ingest";

// Three processes, three registries, three endpoints. Each counts only what
// it does itself (see app/core/metrics.py), so the console scrapes all of
// them rather than expecting one place to have everything.
export const PROCESSES: { id: ProcessId; base: string; label: string; port: number }[] = [
  { id: "api", base: API_BASE, label: "API", port: 8000 },
  { id: "consumer", base: CONSUMER_BASE, label: "Consumer", port: 9100 },
  { id: "ingest", base: INGEST_BASE, label: "Ingest poller", port: 9200 },
];

export const SCRAPE_MS = 5000;
// Two minutes of history at one scrape per 5 s: enough for a sparkline to show
// a burst arriving and draining.
const HISTORY = 24;

export interface Snapshot {
  at: number;
  values: Record<string, number>;
}

export interface ProcessMetrics {
  up: boolean | null;
  error: string | null;
  help: Record<string, string>;
  history: Snapshot[];
}

export type MetricsState = Record<ProcessId, ProcessMetrics>;

const empty = (): ProcessMetrics => ({ up: null, error: null, help: {}, history: [] });

/**
 * Change in a counter between two snapshots.
 *
 * A counter only goes down when its process restarted and began again from
 * zero. Reading that as a negative rate would be wrong, so the new value is
 * taken as the increase since the restart, which is what Prometheus's own
 * rate() does.
 */
export function increase(previous: number | undefined, current: number | undefined): number {
  if (current === undefined) return 0;
  if (previous === undefined) return 0;
  return current >= previous ? current - previous : current;
}

/** Per minute rate of one series over the whole history window. */
export function ratePerMinute(history: Snapshot[], key: string): number | null {
  if (history.length < 2) return null;
  let total = 0;
  for (let i = 1; i < history.length; i++) total += increase(history[i - 1].values[key], history[i].values[key]);
  const minutes = (history[history.length - 1].at - history[0].at) / 60_000;
  return minutes > 0 ? total / minutes : null;
}

/** Increase per scrape interval, oldest first, for a sparkline. */
export function increments(history: Snapshot[], key: string): number[] {
  return history.slice(1).map((snap, i) => increase(history[i].values[key], snap.values[key]));
}

export function latest(metrics: ProcessMetrics, key: string): number | undefined {
  return metrics.history[metrics.history.length - 1]?.values[key];
}

export function useMetrics(): MetricsState {
  const [state, setState] = useState<MetricsState>({ api: empty(), consumer: empty(), ingest: empty() });

  useEffect(() => {
    let cancelled = false;

    const scrape = async () => {
      const results = await Promise.all(
        PROCESSES.map(async ({ id, base }) => {
          try {
            const text = await fetchMetricsText(base);
            const values: Record<string, number> = {};
            for (const sample of parsePrometheus(text)) {
              if (isProjectCounter(sample.name)) values[seriesKey(sample)] = sample.value;
            }
            return { id, ok: true as const, help: parseHelp(text), values };
          } catch (err) {
            return { id, ok: false as const, error: err instanceof Error ? err.message : String(err) };
          }
        }),
      );
      if (cancelled) return;

      const now = Date.now();
      setState((prev) => {
        const next = { ...prev };
        for (const result of results) {
          const current = prev[result.id];
          next[result.id] = result.ok
            ? {
                up: true,
                error: null,
                help: result.help,
                history: [...current.history, { at: now, values: result.values }].slice(-HISTORY),
              }
            : // Keep the history on a failed scrape so a brief outage shows as a
              // gap in freshness, not as the counters vanishing.
              { ...current, up: false, error: result.error };
        }
        return next;
      });
    };

    scrape();
    const timer = window.setInterval(() => {
      if (document.visibilityState === "visible") scrape();
    }, SCRAPE_MS);
    return () => {
      cancelled = true;
      window.clearInterval(timer);
    };
  }, []);

  return state;
}
