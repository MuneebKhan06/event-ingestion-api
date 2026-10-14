import { validateDraft } from "./validate";

export type ReplayVerdict = { replayable: true } | { replayable: false; reason: string };

const FIELD_LABEL: Record<string, string> = {
  event_id: "event_id",
  event_type: "event_type",
  source: "source",
  payloadText: "payload",
};

/**
 * Whether scripts/replay_dlq.py would republish this payload.
 *
 * Mirrors its classify(): the stored raw_payload must itself be a valid
 * EventCreate, because a payload that fails validation is why it landed here
 * in the first place and replaying it would only send it straight back. The
 * script reports those instead of republishing them, and so does this.
 */
export function replayVerdict(rawPayload: unknown): ReplayVerdict {
  if (rawPayload === null || typeof rawPayload !== "object" || Array.isArray(rawPayload)) {
    return { replayable: false, reason: "raw_payload is not an object" };
  }
  const raw = rawPayload as Record<string, unknown>;

  for (const field of ["event_id", "event_type", "source", "payload"]) {
    if (raw[field] === undefined || raw[field] === null) {
      return { replayable: false, reason: `${field}: Field required` };
    }
  }

  const errors = validateDraft({
    event_id: String(raw.event_id),
    event_type: String(raw.event_type),
    source: String(raw.source),
    payloadText: JSON.stringify(raw.payload),
  });
  const first = Object.entries(errors)[0];
  return first ? { replayable: false, reason: `${FIELD_LABEL[first[0]]}: ${first[1]}` } : { replayable: true };
}
