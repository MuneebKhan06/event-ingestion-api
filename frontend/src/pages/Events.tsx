import { useEffect, useState } from "react";
import { api, ApiError, type ApiResult } from "../api/client";
import type { EventPage } from "../api/types";
import { EventTable } from "../components/EventTable";

const PAGE_SIZE = 25;

export function Events() {
  const [result, setResult] = useState<ApiResult<EventPage> | null>(null);
  const [error, setError] = useState<ApiError | null>(null);
  const [loading, setLoading] = useState(true);

  useEffect(() => {
    let cancelled = false;
    setLoading(true);
    api
      .listEvents({ limit: PAGE_SIZE })
      .then((res) => {
        if (cancelled) return;
        setResult(res);
        setError(null);
      })
      .catch((err: ApiError) => !cancelled && setError(err))
      .finally(() => !cancelled && setLoading(false));
    return () => {
      cancelled = true;
    };
  }, []);

  const page = result?.data;

  return (
    <section className="card">
      <div className="card-head">
        <h2>Stored events</h2>
        {page && (
          <span className="muted">
            {page.total.toLocaleString()} in Postgres, newest first
          </span>
        )}
      </div>

      {error ? (
        <div className="empty">{error.message}</div>
      ) : loading && !page ? (
        <div className="empty">Loading events…</div>
      ) : page && page.events.length > 0 ? (
        <EventTable events={page.events} />
      ) : (
        <div className="empty">No events stored yet. Send one, or wait for the ingest poller.</div>
      )}
    </section>
  );
}
