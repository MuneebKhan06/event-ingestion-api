import { useEffect, useState } from "react";
import { api } from "../api/client";
import type { StoredEvent } from "../api/types";
import { href } from "../router";
import type { SendOutcome } from "./ResponsePanel";
import "./DeliveryTracker.css";

const POLL_MS = 300;
const GIVE_UP_MS = 15_000;

type StepState = "done" | "active" | "failed" | "skipped";

interface Step {
  title: string;
  where: string;
  state: StepState;
  detail: string;
}

/**
 * Follows one accepted event from the API's 202 to its row in Postgres.
 *
 * The 202 only means "published to Kafka". Whether the consumer then stores
 * it is a separate, asynchronous fact, so this polls GET /events/{id} until
 * the row exists and reports how long the trip through Kafka and the
 * consumer took. That gap is the cost of Decision 1, keeping the database
 * off the request path, made visible.
 */
export function DeliveryTracker({ outcome }: { outcome: SendOutcome | null }) {
  const [stored, setStored] = useState<{ event: StoredEvent; afterMs: number } | null>(null);
  const [gaveUp, setGaveUp] = useState(false);

  const accepted = outcome?.status === 202;

  useEffect(() => {
    setStored(null);
    setGaveUp(false);
    if (!outcome || outcome.status !== 202) return;

    let cancelled = false;
    let timer: number | undefined;
    const started = outcome.sentAt.getTime();

    const poll = async () => {
      const found = await api.getEvent(outcome.eventId).then(
        (res) => res.data,
        () => null,
      );
      if (cancelled) return;
      if (found) {
        setStored({ event: found, afterMs: Date.now() - started });
      } else if (Date.now() - started > GIVE_UP_MS) {
        setGaveUp(true);
      } else {
        timer = window.setTimeout(poll, POLL_MS);
      }
    };
    poll();

    return () => {
      cancelled = true;
      window.clearTimeout(timer);
    };
  }, [outcome]);

  if (!outcome || outcome.status === 0) return null;

  const status = outcome.status;
  const steps: Step[] = [
    {
      title: "Validated",
      where: "FastAPI + Pydantic",
      state: status === 422 ? "failed" : "done",
      detail: status === 422 ? "Rejected here; nothing was published." : "Body matched the EventCreate schema.",
    },
    {
      title: "Published",
      where: "Kafka topic events.raw",
      state: accepted ? "done" : status === 503 ? "failed" : "skipped",
      detail: accepted
        ? `Acknowledged by the broker; the API answered 202 in ${outcome.elapsedMs} ms.`
        : status === 409
          ? "Skipped: the pre-check found the ID already stored."
          : status === 503
            ? "Kafka was unreachable, so the API answered 503."
            : "Not reached.",
    },
    {
      title: "Stored",
      where: "Consumer → PostgreSQL",
      state: !accepted ? "skipped" : stored ? "done" : gaveUp ? "failed" : "active",
      detail: !accepted
        ? status === 409
          ? "Already stored from an earlier send."
          : "Not reached."
        : stored
          ? `Row #${stored.event.id} visible ${stored.afterMs} ms after sending.`
          : gaveUp
            ? `Not stored after ${GIVE_UP_MS / 1000} s. The consumer may be down or retrying, or the event went to the dead letter queue.`
            : "Waiting for the consumer to read it from Kafka and insert it…",
    },
  ];

  return (
    <section className="card">
      <div className="card-head">
        <h2>Delivery</h2>
        <span className="muted">Where this event is in the pipeline</span>
      </div>

      <ol className="timeline">
        {steps.map((step) => (
          <li key={step.title} className={`timeline-step step-${step.state}`}>
            <span className="timeline-dot" aria-hidden />
            <div>
              <div className="timeline-title">
                <strong>{step.title}</strong>
                <span className="muted">{step.where}</span>
              </div>
              <p className="timeline-detail">{step.detail}</p>
            </div>
          </li>
        ))}
      </ol>

      {(stored || (status === 409 && outcome.eventId)) && (
        <a className="timeline-link" href={`${href("events")}?event=${encodeURIComponent(outcome.eventId)}`}>
          Open the stored event →
        </a>
      )}
      {gaveUp && (
        <a className="timeline-link" href={href("dlq")}>
          Check the dead letter queue →
        </a>
      )}
    </section>
  );
}
