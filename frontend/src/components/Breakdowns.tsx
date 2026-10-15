import { latest, ratePerMinute, type ProcessMetrics } from "../hooks/useMetrics";
import { REASONS, type Reason } from "../lib/dlqReasons";
import { href } from "../router";
import { StatusBadge } from "./StatusBadge";
import "./Breakdowns.css";

// Matches SOURCES in ingest/sources.py. The ingest metrics declare every
// source at zero, polled or not, so an all zero row is how a disabled source
// shows up.
const SOURCES = ["usgs", "weather", "github"];

const COLUMNS: { metric: string; label: string }[] = [
  { metric: "ingest_events_fetched_total", label: "Fetched" },
  { metric: "ingest_events_accepted_total", label: "Accepted" },
  { metric: "ingest_events_duplicate_total", label: "Already seen" },
  { metric: "ingest_events_rejected_total", label: "Rejected" },
  { metric: "ingest_failures_total", label: "Failures" },
  { metric: "ingest_backoff_skips_total", label: "Backoff skips" },
];

const key = (metric: string, label: string, value: string) => `${metric}{${label}="${value}"}`;

export function SourceBreakdown({ ingest }: { ingest: ProcessMetrics }) {
  return (
    <section className="card">
      <div className="card-head">
        <h2>Ingest poller by source</h2>
        <span className="muted">Each public feed, counted since the poller started</span>
      </div>

      <div className="table-wrap">
        <table className="event-table breakdown-table">
          <thead>
            <tr>
              <th>Source</th>
              {COLUMNS.map((c) => (
                <th key={c.metric} className="num">
                  {c.label}
                </th>
              ))}
              <th className="num">New</th>
            </tr>
          </thead>
          <tbody>
            {SOURCES.map((source) => {
              const values = COLUMNS.map((c) => latest(ingest, key(c.metric, "source", source)) ?? 0);
              const [fetched, accepted] = values;
              const polled = values.some((v) => v > 0);
              const fetchRate = ratePerMinute(ingest.history, key(COLUMNS[0].metric, "source", source));
              return (
                <tr key={source}>
                  <td>
                    <strong>{source}</strong>
                    {!polled ? (
                      <span className="muted breakdown-sub">not polled</span>
                    ) : fetchRate ? (
                      <span className="muted breakdown-sub">{fetchRate.toFixed(0)} fetched / min</span>
                    ) : null}
                  </td>
                  {values.map((value, i) => (
                    <td key={COLUMNS[i].metric} className={`num${value === 0 ? " zero" : ""}`}>
                      {value.toLocaleString()}
                    </td>
                  ))}
                  <td className="num">{fetched > 0 ? `${((accepted / fetched) * 100).toFixed(1)}%` : "n/a"}</td>
                </tr>
              );
            })}
          </tbody>
        </table>
      </div>

      <p className="muted breakdown-note">
        <strong>New</strong> is the share of fetched records the API accepted. It is low by design: each poll fetches
        the whole feed again, and records seen before carry the same uuid5 event ID, so the API answers 409 and nothing
        is stored twice. <strong>Backoff skips</strong> are polls a source sat out after failing or being rate
        limited, so a failing upstream is not hammered every interval. A source with all zeros is not in <code>INGEST_SOURCES</code>. The{" "}
        <code>weather</code> source stores its events as <code>open-meteo</code>.
      </p>
    </section>
  );
}

const DLQ_REASONS: Reason[] = ["missing_fields", "invalid_event_id", "retry_exhausted"];

export function DLQBreakdown({ consumer }: { consumer: ProcessMetrics }) {
  return (
    <section className="card">
      <div className="card-head">
        <h2>DLQ writes by reason</h2>
        <span className="muted">Consumer, since it started</span>
        <a className="request-meta" href={href("dlq")}>
          Open the DLQ →
        </a>
      </div>
      <div className="breakdown-reasons">
        {DLQ_REASONS.map((reason) => {
          const value = latest(consumer, key("dlq_writes_total", "reason", reason));
          return (
            <div key={reason} className="breakdown-reason">
              <StatusBadge tone={REASONS[reason].tone}>{REASONS[reason].label}</StatusBadge>
              <span className="breakdown-count">{value ?? "n/a"}</span>
              <code className="muted">reason="{reason}"</code>
            </div>
          );
        })}
      </div>
      <p className="muted breakdown-note">
        Labelled by reason, unlike the API's counters, because the reasons are a fixed set defined in the consumer, so
        they cannot multiply into unbounded series. Every reason is declared at zero on startup, so alerts on a rate
        work before the first failure. These reset when the consumer restarts; the DLQ table keeps the full history.
      </p>
    </section>
  );
}
