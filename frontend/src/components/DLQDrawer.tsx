import { useEffect } from "react";
import type { DLQEvent } from "../api/types";
import { classify, REASONS, rootCause } from "../lib/dlqReasons";
import { fullTime } from "../lib/format";
import { replayVerdict } from "../lib/replay";
import { TypeChip } from "./EventTable";
import { ReasonBadge } from "./ReasonSummary";
import { StatusBadge } from "./StatusBadge";
import "./EventDrawer.css";
import "../pages/DLQ.css";

interface Props {
  event: DLQEvent;
  onClose: () => void;
}

/**
 * Detail view for one failed event.
 *
 * Shows raw_payload exactly as the consumer received it, not a normalised
 * version, because the point of keeping it (Decision 6) is to see what was
 * actually sent. There is no GET /dlq/{id}, so the row comes from the page.
 */
export function DLQDrawer({ event, onClose }: Props) {
  useEffect(() => {
    const onKey = (e: KeyboardEvent) => e.key === "Escape" && onClose();
    window.addEventListener("keydown", onKey);
    return () => window.removeEventListener("keydown", onKey);
  }, [onClose]);

  const reason = classify(event.error_reason);
  const verdict = replayVerdict(event.raw_payload);

  return (
    <div className="drawer-backdrop" onClick={onClose}>
      <aside className="drawer" role="dialog" aria-label="Failed event" onClick={(e) => e.stopPropagation()}>
        <header className="drawer-head">
          <div>
            <span className="stage-tag">dlq_events #{event.id}</span>
            <h2>{event.event_type ? <TypeChip eventType={event.event_type} /> : "Failed event"}</h2>
          </div>
          <button className="btn" onClick={onClose}>
            Close
          </button>
        </header>

        <dl className="drawer-fields">
          <dt>Reason</dt>
          <dd>
            <ReasonBadge errorReason={event.error_reason} />
          </dd>
          <dt>Cause</dt>
          <dd>{reason === "retry_exhausted" ? rootCause(event.error_reason) : event.error_reason}</dd>
          <dt>Event ID</dt>
          <dd>{event.event_id ? <code>{event.event_id}</code> : "missing"}</dd>
          <dt>Source</dt>
          <dd>{event.source ?? "missing"}</dd>
          <dt>Failed</dt>
          <dd>{fullTime(event.failed_at)}</dd>
        </dl>

        <p className="drawer-note">{REASONS[reason].explanation}</p>

        <div className="drawer-payload-head">
          <h3>Raw payload, as received from Kafka</h3>
        </div>
        <pre className="drawer-payload">{JSON.stringify(event.raw_payload, null, 2)}</pre>

        <div className="drawer-payload-head">
          <h3>Replay</h3>
          <StatusBadge tone={verdict.replayable ? "ok" : "warn"}>
            {verdict.replayable ? "replayable" : "would be skipped"}
          </StatusBadge>
        </div>
        <p className="muted drawer-text">
          {verdict.replayable
            ? "The payload is a valid event, so replay_dlq.py would republish it to events.raw once the underlying fault is fixed. If it was in fact stored meanwhile, the unique constraint deduplicates it."
            : `replay_dlq.py checks the payload against the API's schema first and skips this one (${verdict.reason}). Replaying it would only fail again, so it needs fixing at the producer that sent it.`}
        </p>

        {event.event_type && event.event_type.length === 50 && (
          <p className="muted drawer-text">
            The event type column holds 50 characters, so a longer value is trimmed here. The raw payload above keeps the
            original.
          </p>
        )}

        <footer className="drawer-foot">
          The full error text, as stored: <code>{event.error_reason}</code>
        </footer>
      </aside>
    </div>
  );
}
