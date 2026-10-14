import { useEffect, useState } from "react";
import { api } from "../api/client";
import { classify, REASONS, type Reason } from "../lib/dlqReasons";
import { StatusBadge } from "./StatusBadge";
import "./ReasonSummary.css";

// GET /dlq caps a page at 200 and has no group-by, so the breakdown is taken
// over the newest 200 failures. Plenty for a demo or an incident; the card
// says so when the queue is bigger.
const SAMPLE = 200;
const SHOWN: Reason[] = ["missing_fields", "invalid_event_id", "retry_exhausted"];

export function ReasonBadge({ errorReason }: { errorReason: string }) {
  const info = REASONS[classify(errorReason)];
  return <StatusBadge tone={info.tone}>{info.label}</StatusBadge>;
}

export function ReasonSummary({ refreshKey }: { refreshKey: unknown }) {
  const [counts, setCounts] = useState<Record<Reason, number> | null>(null);
  const [total, setTotal] = useState(0);

  useEffect(() => {
    let cancelled = false;
    api
      .listDLQ({ limit: SAMPLE })
      .then((res) => {
        if (cancelled) return;
        const next: Record<Reason, number> = { missing_fields: 0, invalid_event_id: 0, retry_exhausted: 0, unknown: 0 };
        for (const event of res.data.events) next[classify(event.error_reason)] += 1;
        setCounts(next);
        setTotal(res.data.total);
      })
      .catch(() => !cancelled && setCounts(null));
    return () => {
      cancelled = true;
    };
  }, [refreshKey]);

  return (
    <section className="reason-summary">
      {SHOWN.map((reason) => {
        const info = REASONS[reason];
        return (
          <article key={reason} className="card reason-card">
            <div className="reason-head">
              <StatusBadge tone={info.tone}>{info.label}</StatusBadge>
              <span className="reason-count">{counts ? counts[reason] : "…"}</span>
            </div>
            <p className="muted">{info.explanation}</p>
            <span className="reason-retry">{info.retried ? "Retried with backoff" : "Sent straight to the DLQ"}</span>
          </article>
        );
      })}
      {total > SAMPLE && <p className="muted reason-sample">Counts cover the newest {SAMPLE} of {total} failures.</p>}
    </section>
  );
}
