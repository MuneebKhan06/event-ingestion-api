import { useState } from "react";
import type { StoredEvent } from "../api/types";
import { useEventStream, type StreamState } from "../hooks/useEventStream";
import { StatusBadge, type Tone } from "./StatusBadge";
import "./LiveFeed.css";

const STATE_BADGE: Record<StreamState, { tone: Tone; label: string }> = {
  connecting: { tone: "idle", label: "connecting" },
  open: { tone: "ok", label: "live" },
  paused: { tone: "warn", label: "paused" },
  retrying: { tone: "bad", label: "reconnecting" },
};

// The domain half of "<domain>.<action>" picks the colour, so order, user,
// quake and github events are easy to tell apart while they scroll past.
const DOMAIN_HUES = [212, 145, 32, 280, 350, 180];

function domainHue(eventType: string): number {
  const domain = eventType.split(".")[0];
  let hash = 0;
  for (const char of domain) hash = (hash * 31 + char.charCodeAt(0)) >>> 0;
  return DOMAIN_HUES[hash % DOMAIN_HUES.length];
}

function EventRow({ event }: { event: StoredEvent }) {
  const [open, setOpen] = useState(false);
  const hue = domainHue(event.event_type);

  return (
    <li className="feed-row">
      <button className="feed-summary" onClick={() => setOpen(!open)} aria-expanded={open}>
        <span className="feed-type" style={{ "--hue": hue } as React.CSSProperties}>
          {event.event_type}
        </span>
        <span className="feed-source">{event.source}</span>
        <code className="feed-id">{event.event_id.slice(0, 8)}</code>
        <span className="feed-time">#{event.id} · {new Date(event.created_at).toLocaleTimeString()}</span>
      </button>
      {open && <pre className="feed-payload">{JSON.stringify(event.payload, null, 2)}</pre>}
    </li>
  );
}

interface Props {
  // The overview shows a short tail, the stream screen a long one.
  limit?: number;
  compact?: boolean;
}

export function LiveFeed({ limit = 200, compact = false }: Props) {
  const stream = useEventStream(limit);
  const badge = STATE_BADGE[stream.state];

  return (
    <section className="card feed">
      <div className="feed-head">
        <h2>Live events</h2>
        <StatusBadge tone={badge.tone}>{badge.label}</StatusBadge>
        <span className="feed-count">{stream.received} received</span>
        <div className="feed-actions">
          {stream.state === "paused" ? (
            <button className="btn" onClick={stream.resume}>
              Resume
            </button>
          ) : (
            <button className="btn" onClick={stream.pause}>
              Pause
            </button>
          )}
          {!compact && (
            <button className="btn btn-quiet" onClick={stream.clear}>
              Clear
            </button>
          )}
        </div>
      </div>

      {!compact && (
        <p className="feed-hint">
          Pausing closes the connection. Resuming reopens it with{" "}
          <code>?after={stream.lastId ?? "latest"}</code>, so events stored in the meantime still arrive
          and none are skipped.
        </p>
      )}

      {stream.events.length === 0 ? (
        <div className="empty">
          {stream.state === "retrying"
            ? "The stream is unavailable. Retrying every few seconds."
            : "Waiting for events. The ingest poller publishes every 30 seconds, or send one yourself."}
        </div>
      ) : (
        <ul className="feed-list">
          {stream.events.map((event) => (
            <EventRow key={event.id} event={event} />
          ))}
        </ul>
      )}
    </section>
  );
}
