import { useEffect, useState } from "react";
import { api, ApiError, type ApiResult } from "../api/client";
import type { StoredEvent } from "../api/types";
import { fullTime } from "../lib/format";
import { TypeChip } from "./EventTable";
import "./EventDrawer.css";

interface Props {
  eventId: string;
  onClose: () => void;
}

function CopyButton({ text }: { text: string }) {
  const [copied, setCopied] = useState(false);
  return (
    <button
      className="btn btn-quiet copy-btn"
      onClick={() => {
        navigator.clipboard?.writeText(text).then(() => {
          setCopied(true);
          window.setTimeout(() => setCopied(false), 1200);
        });
      }}
    >
      {copied ? "Copied" : "Copy"}
    </button>
  );
}

/**
 * Detail view for one stored event.
 *
 * Fetches GET /events/{event_id} instead of reusing the table row, so the
 * drawer shows that endpoint working and also opens correctly from a shared
 * link (#/events?event=<uuid>) when the row is not on the current page.
 */
export function EventDrawer({ eventId, onClose }: Props) {
  const [result, setResult] = useState<ApiResult<StoredEvent> | null>(null);
  const [error, setError] = useState<ApiError | null>(null);

  useEffect(() => {
    let cancelled = false;
    setResult(null);
    setError(null);
    api
      .getEvent(eventId)
      .then((res) => !cancelled && setResult(res))
      .catch((err: ApiError) => !cancelled && setError(err));
    return () => {
      cancelled = true;
    };
  }, [eventId]);

  useEffect(() => {
    const onKey = (e: KeyboardEvent) => e.key === "Escape" && onClose();
    window.addEventListener("keydown", onKey);
    return () => window.removeEventListener("keydown", onKey);
  }, [onClose]);

  const event = result?.data;
  const payloadText = event ? JSON.stringify(event.payload, null, 2) : "";

  return (
    <div className="drawer-backdrop" onClick={onClose}>
      <aside className="drawer" role="dialog" aria-label="Event detail" onClick={(e) => e.stopPropagation()}>
        <header className="drawer-head">
          <div>
            <span className="stage-tag">GET /events/{"{event_id}"}</span>
            <h2>{event ? <TypeChip eventType={event.event_type} /> : "Event"}</h2>
          </div>
          <button className="btn" onClick={onClose} aria-label="Close">
            Close
          </button>
        </header>

        {error ? (
          <div className="empty">
            {error.status === 404
              ? "No stored event has this id. It may still be in Kafka, or it went to the dead letter queue."
              : error.message}
          </div>
        ) : !event ? (
          <div className="empty">Loading…</div>
        ) : (
          <>
            <dl className="drawer-fields">
              <dt>Event ID</dt>
              <dd>
                <code>{event.event_id}</code>
                <CopyButton text={event.event_id} />
              </dd>
              <dt>Row id</dt>
              <dd>#{event.id}</dd>
              <dt>Source</dt>
              <dd>{event.source}</dd>
              <dt>Status</dt>
              <dd>{event.status}</dd>
              <dt>Stored</dt>
              <dd>{fullTime(event.created_at)}</dd>
              <dt>Processed</dt>
              {/* The column exists in the schema but the consumer does not set
                  it yet, so it is normally empty. */}
              <dd>{event.processed_at ? fullTime(event.processed_at) : "not recorded"}</dd>
            </dl>

            <div className="drawer-payload-head">
              <h3>Payload</h3>
              <CopyButton text={payloadText} />
            </div>
            <pre className="drawer-payload">{payloadText}</pre>

            <p className="drawer-note">
              The event ID is the idempotency key. Sending this same ID to POST /events again is answered with
              409, and even if two copies reach Kafka, the unique constraint keeps exactly one row.
            </p>

            <footer className="drawer-foot">
              HTTP {result.status} in {result.elapsedMs} ms · X-Request-ID <code>{result.requestId}</code>
            </footer>
          </>
        )}
      </aside>
    </div>
  );
}
