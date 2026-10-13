import type { ValidationIssue } from "../api/types";
import { StatusBadge, type Tone } from "./StatusBadge";
import "./ResponsePanel.css";

export interface SendOutcome {
  status: number;
  body: unknown;
  requestId: string | null;
  elapsedMs: number;
  eventId: string;
  sentAt: Date;
}

interface Explanation {
  tone: Tone;
  title: string;
  text: string;
}

// What each answer from POST /events means for the event, in the terms of
// the design: where it went, and which mechanism produced the answer.
function explain(status: number): Explanation {
  switch (status) {
    case 202:
      return {
        tone: "ok",
        title: "202 Accepted",
        text: "Validated and published to the events.raw Kafka topic. The API has not touched Postgres; the consumer will store it asynchronously.",
      };
    case 409:
      return {
        tone: "warn",
        title: "409 Conflict",
        text: "An event with this ID is already stored. The API's duplicate pre-check found it before publishing, so nothing was sent to Kafka.",
      };
    case 422:
      return {
        tone: "warn",
        title: "422 Unprocessable",
        text: "Pydantic rejected the body before it went anywhere near Kafka. Each problem below names the exact field.",
      };
    case 503:
      return {
        tone: "bad",
        title: "503 Unavailable",
        text: "The event was valid but could not be published because Kafka is unreachable. The client should retry; nothing was lost on the server.",
      };
    case 0:
      return {
        tone: "bad",
        title: "No response",
        text: "The API did not answer at all. Check that the compose stack is running.",
      };
    default:
      return { tone: "bad", title: `HTTP ${status}`, text: "An unexpected answer from the API." };
  }
}

function issues(body: unknown): ValidationIssue[] {
  const detail = (body as { detail?: unknown } | null)?.detail;
  return Array.isArray(detail) ? (detail as ValidationIssue[]) : [];
}

export function ResponsePanel({ outcome }: { outcome: SendOutcome | null }) {
  if (!outcome) {
    return (
      <section className="card">
        <h2>Response</h2>
        <div className="empty">Send the event to see how the API answers and why.</div>
      </section>
    );
  }

  const meaning = explain(outcome.status);
  const problems = issues(outcome.body);

  return (
    <section className="card response">
      <div className="card-head">
        <h2>Response</h2>
        <StatusBadge tone={meaning.tone}>{meaning.title}</StatusBadge>
        <span className="muted request-meta">
          {outcome.elapsedMs} ms · {outcome.sentAt.toLocaleTimeString()}
        </span>
      </div>

      <p className="response-text">{meaning.text}</p>

      {problems.length > 0 && (
        <ul className="response-issues">
          {problems.map((issue, i) => (
            <li key={i}>
              <code>{issue.loc.filter((part) => part !== "body").join(".")}</code>
              <span>{issue.msg.replace(/^Value error, /, "")}</span>
            </li>
          ))}
        </ul>
      )}

      <details className="response-raw">
        <summary>Raw response body</summary>
        <pre>{JSON.stringify(outcome.body, null, 2)?.slice(0, 3000) ?? "(empty)"}</pre>
      </details>

      {outcome.requestId && (
        <p className="response-foot">
          X-Request-ID <code>{outcome.requestId}</code> is the ID the API logged this request under.
        </p>
      )}
    </section>
  );
}
