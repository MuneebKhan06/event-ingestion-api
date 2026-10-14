import { useState } from "react";
import type { DLQEvent } from "../api/types";
import { replayVerdict } from "../lib/replay";
import "./ReplayGuide.css";

// CONSUMER_MAX_RETRIES and CONSUMER_RETRY_BACKOFF_BASE_SECONDS defaults from
// app/config.py. The console cannot read the consumer's settings, so these
// are the shipped values; a deployment that overrides them will differ.
const MAX_RETRIES = 3;
const BACKOFF_BASE_S = 1;

// Attempt 1 runs at once, then each retry waits base * 2^n (consumer/retry.py).
const ATTEMPTS = Array.from({ length: MAX_RETRIES + 1 }, (_, i) => ({
  attempt: i + 1,
  at: i === 0 ? 0 : Array.from({ length: i }, (_, n) => BACKOFF_BASE_S * 2 ** n).reduce((a, b) => a + b, 0),
}));
const GIVE_UP_S = ATTEMPTS[ATTEMPTS.length - 1].at;

interface Props {
  events: DLQEvent[];
  eventType: string;
  source: string;
}

function command(eventType: string, source: string, dryRun: boolean): string {
  const parts = ["python scripts/replay_dlq.py"];
  if (dryRun) parts.push("--dry-run");
  if (eventType) parts.push(`--event-type ${eventType}`);
  if (source) parts.push(`--source ${source}`);
  return parts.join(" ");
}

function Command({ text }: { text: string }) {
  const [copied, setCopied] = useState(false);
  return (
    <div className="replay-command">
      <code>{text}</code>
      <button
        className="btn btn-quiet"
        onClick={() =>
          navigator.clipboard?.writeText(text).then(() => {
            setCopied(true);
            window.setTimeout(() => setCopied(false), 1200);
          })
        }
      >
        {copied ? "Copied" : "Copy"}
      </button>
    </div>
  );
}

export function ReplayGuide({ events, eventType, source }: Props) {
  const replayable = events.filter((event) => replayVerdict(event.raw_payload).replayable).length;

  return (
    <section className="replay-guide">
      <article className="card">
        <h2>How an event gets here</h2>
        <ol className="retry-steps">
          {ATTEMPTS.map(({ attempt, at }) => (
            <li key={attempt}>
              <span className="retry-at">{at === 0 ? "0 s" : `+${at} s`}</span>
              <span>
                Insert attempt {attempt}
                {attempt === ATTEMPTS.length && ", then give up"}
              </span>
            </li>
          ))}
        </ol>
        <p className="muted replay-text">
          Only database failures are retried, with exponential backoff, about {GIVE_UP_S} s in all. A malformed
          message goes straight here, since retrying it cannot help. Either way the failure is written to the{" "}
          <code>dlq_events</code> table for inspection and to the <code>events.dlq</code> topic for alerting, and the
          offset is committed so the partition keeps moving.
        </p>
      </article>

      <article className="card">
        <h2>Replaying</h2>
        <p className="muted replay-text">
          Replay is a command line step on purpose: it republishes to <code>events.raw</code>, and that should be a
          deliberate act after the cause is fixed, not a button. Run a dry run first; it lists what would be republished
          and what would be skipped. Commands below follow the current filters.
        </p>
        <Command text={command(eventType, source, true)} />
        <Command text={command(eventType, source, false)} />
        <p className="replay-count">
          {events.length === 0
            ? "Nothing on this page to replay."
            : `${replayable} of ${events.length} on this page would be republished; the rest fail validation and are skipped.`}
        </p>
        <p className="muted replay-text">
          Replayed events go through the normal consumer, so the usual idempotency applies: one that did land in
          Postgres in the meantime is deduplicated on insert, never stored twice. To try it on real failures, seed some
          with <code>./scripts/seed_dlq.sh</code>.
        </p>
      </article>
    </section>
  );
}
