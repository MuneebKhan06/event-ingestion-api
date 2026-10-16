import { api, ApiError, CONSUMER_BASE, fetchMetricsText } from "../api/client";
import type { StageId } from "./pipeline";
import { parsePrometheus, total } from "./prometheus";
import { newEventId } from "./validate";

export interface TourContext {
  // The event the tour sent in step 1, reused by the duplicate step.
  eventId: string | null;
}

export interface StepResult {
  ok: boolean;
  text: string;
}

export interface TourStep {
  title: string;
  explain: string;
  stages: StageId[];
  action: string;
  run: (ctx: TourContext) => Promise<StepResult>;
}

const body = (eventId: string, eventType = "order.created") => ({
  event_id: eventId,
  event_type: eventType,
  source: "guided-tour",
  payload: { order_id: `ORD-${Math.floor(1000 + Math.random() * 9000)}`, amount: 42.5, currency: "USD" },
});

const sleep = (ms: number) => new Promise((resolve) => window.setTimeout(resolve, ms));

async function skippedCount(): Promise<number | null> {
  try {
    return total(parsePrometheus(await fetchMetricsText(CONSUMER_BASE)), "events_duplicates_skipped_total");
  } catch {
    return null;
  }
}

const statusOf = (err: unknown) => (err instanceof ApiError ? err.status : 0);

// Each step does one real thing against the running stack and reports what
// actually came back, so the tour can never show a result that did not happen.
export const TOUR: TourStep[] = [
  {
    title: "Send an event",
    explain:
      "A client posts an order.created event. The API validates it and publishes it to Kafka, then answers. It does not wait for the database.",
    stages: ["client", "api", "kafka"],
    action: "Send it",
    run: async (ctx) => {
      ctx.eventId = newEventId();
      const res = await api.sendEvent(body(ctx.eventId));
      return { ok: true, text: `${res.status} Accepted in ${res.elapsedMs} ms. The event is in Kafka; Postgres has not seen it yet.` };
    },
  },
  {
    title: "Watch it get stored",
    explain:
      "The consumer reads the event from Kafka and inserts it. This polls GET /events/{id} until the row exists, which shows the short gap Decision 1 accepts.",
    stages: ["kafka", "consumer", "postgres"],
    action: "Find the row",
    run: async (ctx) => {
      if (!ctx.eventId) return { ok: false, text: "Run step 1 first, so there is an event to look for." };
      const started = performance.now();
      while (performance.now() - started < 10_000) {
        try {
          const res = await api.getEvent(ctx.eventId);
          return {
            ok: true,
            text: `Stored as row #${res.data.id}, found ${Math.round(performance.now() - started)} ms after looking.`,
          };
        } catch {
          await sleep(250);
        }
      }
      return { ok: false, text: "Not stored within 10 s. Is the consumer running?" };
    },
  },
  {
    title: "Send it again",
    explain:
      "Kafka delivers at least once and clients retry, so duplicates are normal. Sending the same event_id again hits the API's pre-check.",
    stages: ["client", "api", "postgres"],
    action: "Resend",
    run: async (ctx) => {
      if (!ctx.eventId) return { ok: false, text: "Run step 1 first." };
      try {
        const res = await api.sendEvent(body(ctx.eventId));
        return {
          ok: true,
          text: `${res.status}: the pre-check is switched off here, so the consumer's unique constraint will drop the copy instead.`,
        };
      } catch (err) {
        return statusOf(err) === 409
          ? { ok: true, text: "409 Conflict. The ID is already stored, so nothing was published to Kafka." }
          : { ok: false, text: `Unexpected answer: ${statusOf(err)}.` };
      }
    },
  },
  {
    title: "Race three copies",
    explain:
      "The pre-check is best effort. Three copies of a new event sent at once all pass it, because none is stored yet. The unique constraint in Postgres is the real guarantee.",
    stages: ["api", "consumer", "postgres"],
    action: "Race",
    run: async () => {
      const eventId = newEventId();
      const before = await skippedCount();
      const statuses = await Promise.all(
        [0, 1, 2].map(() =>
          api.sendEvent(body(eventId)).then(
            (res) => res.status,
            (err) => statusOf(err),
          ),
        ),
      );
      await sleep(4000);
      const after = await skippedCount();
      const skipped = before !== null && after !== null ? after - before : null;
      // GET /events/{id} can only ever return one row, so finding it proves
      // the copies collapsed into one rather than being lost.
      const stored = await api.getEvent(eventId).then(
        () => true,
        () => false,
      );
      return {
        ok: stored,
        text: `API answered ${statuses.join(", ")}. ${
          skipped === null ? "Consumer metrics were unreachable." : `The consumer dropped ${skipped} as duplicates.`
        } ${stored ? "One row is stored for this event_id." : "No row is stored yet; try again in a moment."}`,
      };
    },
  },
  {
    title: "Break validation",
    explain:
      "An event_type without a dot breaks the <domain>.<action> rule. Pydantic rejects it before it gets anywhere near Kafka.",
    stages: ["client", "api"],
    action: "Send a bad event",
    run: async () => {
      try {
        await api.sendEvent(body(newEventId(), "ordercreated"));
        return { ok: false, text: "The API accepted it, which it should not have." };
      } catch (err) {
        const detail = err instanceof ApiError ? err.message.replace(/^Value error, /, "") : "";
        return statusOf(err) === 422
          ? { ok: true, text: `422 Unprocessable: ${detail}` }
          : { ok: false, text: `Unexpected answer: ${statusOf(err)}.` };
      }
    },
  },
  {
    title: "Where failures go",
    explain:
      "Events that pass the API but cannot be stored, such as a malformed message written straight to Kafka, go to the dead letter queue with their reason, ready to replay.",
    stages: ["consumer", "dlq"],
    action: "Check the DLQ",
    run: async () => {
      const res = await api.listDLQ({ limit: 1 });
      return {
        ok: true,
        text:
          res.data.total > 0
            ? `${res.data.total} failed event(s) are waiting in the DLQ. Open the Dead letter queue screen to inspect them.`
            : "The DLQ is empty. Run ./scripts/seed_dlq.sh to publish one message per failure path and watch them arrive.",
      };
    },
  },
];
