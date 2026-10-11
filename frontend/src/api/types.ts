// Mirrors app/schemas/events.py. Kept by hand rather than generated from the
// OpenAPI document because the surface is small and these names are what the
// rest of the UI reads; a field renamed on the backend should break the build
// here, not fail quietly at runtime.

export interface EventCreate {
  event_id: string;
  event_type: string;
  source: string;
  payload: Record<string, unknown>;
}

export interface EventAccepted {
  event_id: string;
  status: "accepted";
}

export interface StoredEvent {
  id: number;
  event_id: string;
  event_type: string;
  source: string;
  payload: Record<string, unknown>;
  status: string;
  created_at: string;
  processed_at: string | null;
}

export interface Page<T> {
  total: number;
  limit: number;
  offset: number;
  events: T[];
}

export type EventPage = Page<StoredEvent>;

export interface DLQEvent {
  id: number;
  event_id: string | null;
  event_type: string | null;
  source: string | null;
  raw_payload: Record<string, unknown>;
  error_reason: string;
  failed_at: string;
}

export type DLQPage = Page<DLQEvent>;

export type DependencyStatus = "connected" | "disconnected" | "unreachable" | "query_failed";

export interface Health {
  status: "healthy" | "degraded";
  kafka: DependencyStatus;
  database: DependencyStatus;
}

export interface EventFilters {
  event_type?: string;
  source?: string;
  since?: string;
  until?: string;
  limit?: number;
  offset?: number;
}

// FastAPI answers HTTPException with a string detail and validation failures
// with a list of located errors. Both shapes reach the UI.
export interface ValidationIssue {
  loc: (string | number)[];
  msg: string;
  type: string;
}

export type ErrorDetail = string | ValidationIssue[];
