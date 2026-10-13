import { useState } from "react";
import { api, ApiError, CONSUMER_BASE, fetchMetricsText } from "../api/client";
import { parsePrometheus, total } from "../lib/prometheus";
import { newEventId, toRequestBody, type Draft } from "../lib/validate";
import "./DuplicateDemo.css";

const COPIES = 3;
// The consumer needs a moment to read the copies off Kafka and attempt the
// inserts before its counters reflect them.
const SETTLE_MS = 4000;

interface Props {
  draft: Draft;
  // Re-sends the current draft through the page, so its answer lands in the
  // main response panel like any other send.
  onResend: () => void;
  canResend: boolean;
}

interface RaceResult {
  eventId: string;
  statuses: number[];
  skipped: number | null;
  rowFound: boolean;
}

async function consumerCounters(): Promise<{ skipped: number } | null> {
  try {
    const samples = parsePrometheus(await fetchMetricsText(CONSUMER_BASE));
    return { skipped: total(samples, "events_duplicates_skipped_total") };
  } catch {
    return null;
  }
}

/**
 * Shows the two layers of deduplication from Decision 4.
 *
 * "Send again" hits the API's pre-check: the event is already stored, so the
 * answer is a fast 409. "Race" sends several copies of a brand new event at
 * once. None of them is stored yet when the others are checked, so all of
 * them pass the pre-check and are accepted, and the unique constraint in the
 * consumer is what keeps exactly one row. The consumer's own counters are
 * read before and after as proof.
 */
export function DuplicateDemo({ draft, onResend, canResend }: Props) {
  const [racing, setRacing] = useState(false);
  const [race, setRace] = useState<RaceResult | null>(null);

  async function runRace() {
    setRacing(true);
    setRace(null);
    const eventId = newEventId();
    const body = toRequestBody({ ...draft, event_id: eventId });
    const before = await consumerCounters();

    const statuses = await Promise.all(
      Array.from({ length: COPIES }, () =>
        api.sendEvent(body).then(
          (res) => res.status,
          (err: ApiError) => err.status,
        ),
      ),
    );

    await new Promise((resolve) => window.setTimeout(resolve, SETTLE_MS));
    const after = await consumerCounters();
    const rowFound = await api.getEvent(eventId).then(
      () => true,
      () => false,
    );

    setRace({
      eventId,
      statuses,
      skipped: before && after ? after.skipped - before.skipped : null,
      rowFound,
    });
    setRacing(false);
  }

  return (
    <section className="card">
      <div className="card-head">
        <h2>Duplicates</h2>
        <span className="muted">The event ID is the idempotency key</span>
      </div>

      <div className="dup-actions">
        <div className="dup-action">
          <button type="button" className="btn" disabled={!canResend} onClick={onResend}>
            Send again
          </button>
          <p className="muted">
            Same event ID as the last send. Once it is stored, the API's pre-check answers <strong>409</strong> without
            publishing.
          </p>
        </div>
        <div className="dup-action">
          <button type="button" className="btn" disabled={racing || !canResend} onClick={runRace}>
            {racing ? "Racing…" : `Race ${COPIES} copies`}
          </button>
          <p className="muted">
            A new event sent {COPIES} times at once. All pass the pre-check, so expect {COPIES}× <strong>202</strong>,
            and still only one stored row.
          </p>
        </div>
      </div>

      {race && (
        <div className="dup-result">
          <div className="dup-step">
            <span className="dup-label">API answers</span>
            <span>
              {race.statuses.map((status, i) => (
                <code key={i} className={`dup-code dup-${status}`}>
                  {status}
                </code>
              ))}
            </span>
          </div>
          <div className="dup-step">
            <span className="dup-label">Skipped by unique constraint</span>
            <span>{race.skipped ?? "?"}</span>
          </div>
          <div className="dup-step">
            <span className="dup-label">Rows for this event ID</span>
            <span>{race.rowFound ? "exactly one (GET /events/{id})" : "not stored yet"}</span>
          </div>
          <p className="dup-note">
            {race.skipped === null
              ? "Consumer metrics on :9100 were unreachable, so the counts could not be read."
              : `The skipped count is the change in events_duplicates_skipped_total on the consumer: ${COPIES - 1} of the ${COPIES} accepted copies reached the insert and were dropped by ON CONFLICT DO NOTHING.`}
          </p>
          <code className="dup-id">{race.eventId}</code>
        </div>
      )}
    </section>
  );
}
