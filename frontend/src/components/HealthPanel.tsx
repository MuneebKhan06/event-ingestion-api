import { api, type ApiResult } from "../api/client";
import type { DependencyStatus, Health } from "../api/types";
import { usePolling } from "../hooks/usePolling";
import { StatusBadge, type Tone } from "./StatusBadge";
import "./HealthPanel.css";

const POLL_MS = 5000;

// What each value from app/api/routes/health.py actually means. The endpoint
// separates "cannot connect" from "connected but the query failed" because
// they have different fixes, so the console keeps that distinction too.
const MEANING: Record<DependencyStatus, { tone: Tone; text: string }> = {
  connected: { tone: "ok", text: "Reachable and answering." },
  disconnected: {
    tone: "bad",
    text: "The API's Kafka producer is not started, so POST /events would answer 503.",
  },
  unreachable: {
    tone: "bad",
    text: "No connection at all: wrong host, refused connection or bad credentials.",
  },
  query_failed: {
    tone: "warn",
    text: "Connected, but SELECT 1 failed: missing schema or revoked permissions.",
  },
};

interface DependencyProps {
  name: string;
  status: DependencyStatus | undefined;
  note: string;
  // What "connected" proves differs per dependency, so it can be restated.
  connectedText?: string;
}

function Dependency({ name, status, note, connectedText }: DependencyProps) {
  const meaning = status ? MEANING[status] : null;
  const text = status === "connected" && connectedText ? connectedText : meaning?.text;
  return (
    <div className="dependency">
      <div className="dependency-head">
        <strong>{name}</strong>
        <StatusBadge tone={meaning?.tone ?? "idle"}>{status ?? "unknown"}</StatusBadge>
      </div>
      <p>{text ?? "Waiting for the first health check."}</p>
      <p className="dependency-note">{note}</p>
    </div>
  );
}

export function HealthPanel() {
  const { data, error, lastUpdated } = usePolling<ApiResult<Health>>(api.health, POLL_MS);
  const health = data?.data;

  // Status 0 means nothing answered, which is different from the API
  // answering that it is degraded.
  const apiDown = error?.status === 0;
  const overall: { tone: Tone; label: string } = apiDown
    ? { tone: "bad", label: "API unreachable" }
    : !health
      ? { tone: "idle", label: "checking" }
      : health.status === "healthy"
        ? { tone: "ok", label: "healthy" }
        : { tone: "bad", label: "degraded" };

  return (
    <section className="card health">
      <div className="health-head">
        <h2>Pipeline health</h2>
        <StatusBadge tone={overall.tone}>{overall.label}</StatusBadge>
      </div>

      {apiDown && (
        <p className="health-error">
          {error.message} Start it with <code>docker compose up -d</code>.
        </p>
      )}

      <div className="dependencies">
        <Dependency
          name="Kafka"
          status={apiDown ? undefined : health?.kafka}
          connectedText="The API's producer is started and ready to publish."
          note="Reports whether the producer started, not a live broker round trip. A real round trip on every check would make the probe slow and flappy."
        />
        <Dependency
          name="PostgreSQL"
          status={apiDown ? undefined : health?.database}
          note="Probed for real: connect, then SELECT 1, so the two failure kinds can be told apart."
        />
      </div>

      <footer className="health-foot">
        <span>
          HTTP {data?.status ?? "n/a"}
          {data && ` in ${data.elapsedMs} ms`}
        </span>
        {data?.requestId && (
          <span title="Echoed by the API's correlation middleware; matches its log lines">
            X-Request-ID <code>{data.requestId.slice(0, 8)}</code>
          </span>
        )}
        <span>{lastUpdated ? `checked ${lastUpdated.toLocaleTimeString()}` : "not checked yet"}</span>
      </footer>
    </section>
  );
}
