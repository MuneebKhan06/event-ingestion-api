import type { Tone } from "../components/StatusBadge";

// The three routes into the DLQ in consumer/processor.py, named as the
// consumer's dlq_writes_total metric labels them.
export type Reason = "missing_fields" | "invalid_event_id" | "retry_exhausted" | "unknown";

export interface ReasonInfo {
  label: string;
  tone: Tone;
  // Whether the consumer retried before giving up. The first two are
  // permanent problems with the message itself, so retrying is pointless and
  // they go straight to the DLQ.
  retried: boolean;
  explanation: string;
}

export const REASONS: Record<Reason, ReasonInfo> = {
  missing_fields: {
    label: "Missing fields",
    tone: "bad",
    retried: false,
    explanation:
      "The Kafka message lacked event_id, event_type, source or payload. A permanent problem with the message, so it was not retried.",
  },
  invalid_event_id: {
    label: "Invalid event ID",
    tone: "bad",
    retried: false,
    explanation:
      "event_id was present but not a UUID, so there is no idempotency key to deduplicate on. Not retried: it would fail the same way.",
  },
  retry_exhausted: {
    label: "Retries exhausted",
    tone: "warn",
    retried: true,
    explanation:
      "The message was well formed, but the database insert kept failing. The consumer retried with exponential backoff before giving up.",
  },
  unknown: {
    label: "Other",
    tone: "idle",
    retried: false,
    explanation: "A reason this console does not recognise.",
  },
};

/** Which route a DLQ row took, read from the error_reason the consumer wrote. */
export function classify(errorReason: string): Reason {
  if (errorReason.startsWith("Missing required field")) return "missing_fields";
  if (errorReason.startsWith("Invalid event_id")) return "invalid_event_id";
  if (errorReason.startsWith("DB insert failed after retries")) return "retry_exhausted";
  return "unknown";
}

/**
 * The database's own words from a retry_exhausted reason.
 *
 * The consumer stores the full SQLAlchemy exception text, which buries the
 * useful part ("value too long for type character varying(50)") between a
 * driver class name and the echoed SQL. Pulls that part out; falls back to
 * the whole text when the shape is not recognised.
 */
export function rootCause(errorReason: string): string {
  const afterClass = errorReason.match(/'>:\s*([^\n[]+)/);
  if (afterClass) return afterClass[1].trim();
  return errorReason.replace(/^DB insert failed after retries:\s*/, "");
}
