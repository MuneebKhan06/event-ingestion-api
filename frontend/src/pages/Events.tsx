import { useEffect, useState } from "react";
import { api, ApiError, type ApiResult } from "../api/client";
import type { EventPage } from "../api/types";
import { EventFilters, type FilterValues } from "../components/EventFilters";
import { EventTable } from "../components/EventTable";
import { useHashParams } from "../router";

const PAGE_SIZE = 25;
const FILTER_KEYS = ["event_type", "source", "since", "until"] as const;

// Seeded with what the bundled sources and the README examples produce, then
// extended with whatever the results contain.
const SEED_TYPES = ["order.created", "quake.detected", "weather.sampled"];
const SEED_SOURCES = ["order-service", "usgs", "open-meteo", "github"];

export function Events() {
  const [params, setParams] = useHashParams();
  const filters: FilterValues = Object.fromEntries(
    FILTER_KEYS.map((key) => [key, params.get(key) ?? undefined]),
  );
  const filterKey = FILTER_KEYS.map((key) => params.get(key) ?? "").join("|");

  const [result, setResult] = useState<ApiResult<EventPage> | null>(null);
  const [error, setError] = useState<ApiError | null>(null);
  const [loading, setLoading] = useState(true);
  const [knownTypes, setKnownTypes] = useState(new Set(SEED_TYPES));
  const [knownSources, setKnownSources] = useState(new Set(SEED_SOURCES));

  useEffect(() => {
    let cancelled = false;
    setLoading(true);
    api
      .listEvents({ ...filters, limit: PAGE_SIZE })
      .then((res) => {
        if (cancelled) return;
        setResult(res);
        setError(null);
        setKnownTypes((prev) => new Set([...prev, ...res.data.events.map((e) => e.event_type)]));
        setKnownSources((prev) => new Set([...prev, ...res.data.events.map((e) => e.source)]));
      })
      .catch((err: ApiError) => !cancelled && setError(err))
      .finally(() => !cancelled && setLoading(false));
    return () => {
      cancelled = true;
    };
    // filterKey stands in for the filters object, which is rebuilt each render.
  }, [filterKey]);

  const page = result?.data;
  const filtered = FILTER_KEYS.some((key) => params.get(key));

  return (
    <section className="card">
      <EventFilters
        values={filters}
        onChange={(next) => setParams(Object.fromEntries(FILTER_KEYS.map((key) => [key, next[key]])))}
        knownTypes={[...knownTypes].sort()}
        knownSources={[...knownSources].sort()}
      />

      <div className="card-head">
        <h2>Stored events</h2>
        {page && (
          <span className="muted">
            {page.total.toLocaleString()} {filtered ? "match these filters" : "in Postgres"}, newest first
          </span>
        )}
        {result && (
          <span className="muted request-meta">
            {result.elapsedMs} ms · X-Request-ID <code>{result.requestId?.slice(0, 8)}</code>
          </span>
        )}
      </div>

      {error ? (
        <div className="empty">
          {error.status === 422 ? `The API rejected these filters: ${error.message}` : error.message}
        </div>
      ) : loading && !page ? (
        <div className="empty">Loading events…</div>
      ) : page && page.events.length > 0 ? (
        <EventTable events={page.events} />
      ) : filtered ? (
        <div className="empty">Nothing matches these filters. Type and source must match exactly.</div>
      ) : (
        <div className="empty">No events stored yet. Send one, or wait for the ingest poller.</div>
      )}
    </section>
  );
}
