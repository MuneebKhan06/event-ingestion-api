import type {
  DLQPage,
  ErrorDetail,
  EventAccepted,
  EventCreate,
  EventFilters,
  EventPage,
  Health,
  StoredEvent,
} from "./types";

// Every path goes through the Vite proxy (see vite.config.ts), so these are
// same origin and the browser never makes a cross origin request.
export const API_BASE = "/api";
export const CONSUMER_BASE = "/consumer";
export const INGEST_BASE = "/ingest";

/**
 * A response the UI should explain rather than just report.
 *
 * Carries the status code, FastAPI's detail and the request ID the middleware
 * echoed back, so a screen can say "409 because this event_id was already
 * stored" and show the ID that matches the server's log line.
 */
export class ApiError extends Error {
  constructor(
    readonly status: number,
    readonly detail: ErrorDetail,
    readonly requestId: string | null,
  ) {
    super(typeof detail === "string" ? detail : detail.map((d) => d.msg).join("; "));
    this.name = "ApiError";
  }
}

export interface ApiResult<T> {
  data: T;
  status: number;
  requestId: string | null;
  elapsedMs: number;
}

interface RequestOptions extends RequestInit {
  // /health answers 503 with a full body when degraded, and that body is the
  // useful part, so a caller that expects a status can accept it as data.
  allowStatus?: number[];
}

async function request<T>(path: string, options: RequestOptions = {}): Promise<ApiResult<T>> {
  const { allowStatus = [], ...init } = options;
  const started = performance.now();
  let response: Response;
  try {
    response = await fetch(`${API_BASE}${path}`, {
      ...init,
      headers: { Accept: "application/json", ...init.headers },
    });
  } catch {
    // fetch only rejects when nothing answered at all: the API is down or the
    // proxy cannot reach it. Status 0 lets screens tell that apart from a 5xx.
    throw new ApiError(0, "The API could not be reached. Is the compose stack running?", null);
  }

  const requestId = response.headers.get("X-Request-ID");
  const body = await response.json().catch(() => null);

  if (!response.ok && !allowStatus.includes(response.status)) {
    throw new ApiError(response.status, body?.detail ?? response.statusText, requestId);
  }

  return {
    data: body as T,
    status: response.status,
    requestId,
    elapsedMs: Math.round(performance.now() - started),
  };
}

function query(params: object): string {
  const search = new URLSearchParams();
  for (const [key, value] of Object.entries(params)) {
    if (value !== undefined && value !== null && value !== "") search.set(key, String(value));
  }
  const text = search.toString();
  return text ? `?${text}` : "";
}

export const api = {
  health: () => request<Health>("/health", { allowStatus: [503] }),

  listEvents: (filters: EventFilters = {}) => request<EventPage>(`/events${query(filters)}`),

  getEvent: (eventId: string) => request<StoredEvent>(`/events/${encodeURIComponent(eventId)}`),

  // Accepts any body, not only a valid EventCreate: the composer can turn its
  // own checks off to let the server reject a malformed event itself.
  sendEvent: (event: EventCreate | Record<string, unknown>) =>
    request<EventAccepted>("/events", {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify(event),
    }),

  listDLQ: (filters: Pick<EventFilters, "event_type" | "source" | "limit" | "offset"> = {}) =>
    request<DLQPage>(`/dlq${query(filters)}`),

  streamUrl: (after?: number) => `${API_BASE}/events/stream${query({ after })}`,
};

/** Raw Prometheus text from one of the three processes. */
export async function fetchMetricsText(base: string): Promise<string> {
  const response = await fetch(`${base}/metrics`);
  if (!response.ok) throw new ApiError(response.status, response.statusText, null);
  return response.text();
}
