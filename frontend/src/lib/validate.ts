import type { EventCreate } from "../api/types";

// The same rules as EventCreate in app/schemas/events.py. Checking them here
// gives instant feedback while typing; the API still enforces them, and the
// composer can deliberately skip these checks to show the server's own 422.
export const MAX_FIELD_LENGTH = 50;
export const MAX_PAYLOAD_BYTES = 32_768;

const UUID = /^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$/i;

export interface Draft {
  event_id: string;
  event_type: string;
  source: string;
  payloadText: string;
}

export type DraftErrors = Partial<Record<keyof Draft, string>>;

export function newEventId(): string {
  return crypto.randomUUID();
}

/**
 * The size the server measures, len(json.dumps(payload)).
 *
 * Python's defaults differ from JSON.stringify: a space after every "," and
 * ":", and non ASCII escaped to \uXXXX. Matching both keeps the browser check
 * agreeing with the API at the boundary. Floats can still differ by a digit
 * (Python writes 1.0 where JS writes 1), which only matters within a few
 * bytes of the limit.
 */
export function payloadBytes(payload: unknown): number {
  return pythonDumps(payload).length;
}

function pythonDumps(value: unknown): string {
  if (Array.isArray(value)) return `[${value.map(pythonDumps).join(", ")}]`;
  if (value !== null && typeof value === "object") {
    const entries = Object.entries(value).map(([k, v]) => `${pythonDumps(k)}: ${pythonDumps(v)}`);
    return `{${entries.join(", ")}}`;
  }
  if (typeof value === "string") return JSON.stringify(value).replace(/[^\x00-\x7f]/g, "\\uXXXX");
  return JSON.stringify(value);
}

export function parsePayload(text: string): { value?: Record<string, unknown>; error?: string } {
  let value: unknown;
  try {
    value = JSON.parse(text);
  } catch (err) {
    return { error: `Not valid JSON: ${(err as Error).message}` };
  }
  if (value === null || typeof value !== "object" || Array.isArray(value)) {
    return { error: "The payload must be a JSON object, e.g. {\"order_id\": \"ORD-001\"}." };
  }
  return { value: value as Record<string, unknown> };
}

export function validateDraft(draft: Draft): DraftErrors {
  const errors: DraftErrors = {};

  if (!UUID.test(draft.event_id.trim())) errors.event_id = "Must be a UUID.";

  const type = draft.event_type.trim();
  if (!type) errors.event_type = "Required.";
  else if (type.length > MAX_FIELD_LENGTH) errors.event_type = `At most ${MAX_FIELD_LENGTH} characters.`;
  else if (!type.includes(".")) errors.event_type = "Use <domain>.<action>, e.g. order.created.";

  const source = draft.source.trim();
  if (!source) errors.source = "Required.";
  else if (source.length > MAX_FIELD_LENGTH) errors.source = `At most ${MAX_FIELD_LENGTH} characters.`;

  const parsed = parsePayload(draft.payloadText);
  if (parsed.error) errors.payloadText = parsed.error;
  else if (payloadBytes(parsed.value) > MAX_PAYLOAD_BYTES)
    errors.payloadText = `Payload is ${payloadBytes(parsed.value).toLocaleString()} bytes; the limit is ${MAX_PAYLOAD_BYTES.toLocaleString()}.`;

  return errors;
}

/**
 * The request body exactly as it will be sent.
 *
 * An unparseable payload is sent as its raw text rather than refused, so that
 * with browser checks off the server is the one to reject it.
 */
export function toRequestBody(draft: Draft): EventCreate | Record<string, unknown> {
  const parsed = parsePayload(draft.payloadText);
  return {
    event_id: draft.event_id.trim(),
    event_type: draft.event_type.trim(),
    source: draft.source.trim(),
    payload: parsed.value ?? draft.payloadText,
  };
}
