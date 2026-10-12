import { useEffect, useState } from "react";
import { api, ApiError, type ApiResult } from "../api/client";
import type { EventPage } from "../api/types";
import { EventFilters, type FilterValues } from "../components/EventFilters";
import { EventTable } from "../components/EventTable";
import { PAGE_SIZES, Pagination } from "../components/Pagination";
import { useHashParams } from "../router";

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
  const requestedLimit = Number(params.get("limit"));
  const limit = PAGE_SIZES.includes(requestedLimit) ? requestedLimit : PAGE_SIZES[0];
  const offset = Math.max(0, Number(params.get("offset")) || 0);
  const queryKey = [...FILTER_KEYS.map((key) => params.get(key) ?? ""), limit, offset].join("|");

  const [result, setResult] = useState<ApiResult<EventPage> | null>(null);
  const [error, setError] = useState<ApiError | null>(null);
  const [loading, setLoading] = useState(true);
  const [knownTypes, setKnownTypes] = useState(new Set(SEED_TYPES));
  const [knownSources, setKnownSources] = useState(new Set(SEED_SOURCES));

  useEffect(() => {
    let cancelled = false;
    setLoading(true);
    api
      .listEvents({ ...filters, limit, offset })
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
    // queryKey stands in for the filters object, which is rebuilt each render.
  }, [queryKey]);

  const page = result?.data;
  const filtered = FILTER_KEYS.some((key) => params.get(key));

  return (
    <section className="card">
      <EventFilters
        values={filters}
        onChange={(next) =>
          // A new filter changes what page 3 even means, so start from the top.
          setParams({ ...Object.fromEntries(FILTER_KEYS.map((key) => [key, next[key]])), offset: undefined })
        }
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
        <>
          <EventTable events={page.events} />
          <Pagination
            total={page.total}
            limit={limit}
            offset={offset}
            onChange={(nextOffset, nextLimit) =>
              setParams({
                offset: nextOffset > 0 ? String(nextOffset) : undefined,
                limit: nextLimit !== PAGE_SIZES[0] ? String(nextLimit) : undefined,
              })
            }
          />
          <p className="muted pagination-note">
            Offset pagination over a newest first list: events that arrive while you browse push older rows
            onto the next page. The live stream avoids this by resuming from an id (<code>?after=</code>)
            instead of a position.
          </p>
        </>
      ) : page && offset > 0 ? (
        <div className="empty">
          This page is past the end of the results.{" "}
          <button className="btn" onClick={() => setParams({ offset: undefined })}>
            Back to page 1
          </button>
        </div>
      ) : filtered ? (
        <div className="empty">Nothing matches these filters. Type and source must match exactly.</div>
      ) : (
        <div className="empty">No events stored yet. Send one, or wait for the ingest poller.</div>
      )}
    </section>
  );
}
