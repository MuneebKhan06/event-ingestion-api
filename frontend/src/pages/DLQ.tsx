import { useEffect, useState } from "react";
import { api, ApiError, type ApiResult } from "../api/client";
import type { DLQPage } from "../api/types";
import { DLQTable } from "../components/DLQTable";
import { PAGE_SIZES, Pagination } from "../components/Pagination";
import { useHashParams } from "../router";
import "./DLQ.css";

const REFRESH_MS = 10_000;

export function DLQ() {
  const [params, setParams] = useHashParams();
  const requestedLimit = Number(params.get("limit"));
  const limit = PAGE_SIZES.includes(requestedLimit) ? requestedLimit : PAGE_SIZES[0];
  const offset = Math.max(0, Number(params.get("offset")) || 0);
  const eventType = params.get("event_type") ?? "";
  const source = params.get("source") ?? "";
  const queryKey = [eventType, source, limit, offset].join("|");

  const [draft, setDraft] = useState({ event_type: eventType, source });
  const [result, setResult] = useState<ApiResult<DLQPage> | null>(null);
  const [error, setError] = useState<ApiError | null>(null);

  useEffect(() => setDraft({ event_type: eventType, source }), [eventType, source]);

  // Refreshed on a timer as well as on filter changes: the question asked of
  // a DLQ is usually "is anything failing right now", so a stale view is the
  // wrong default.
  useEffect(() => {
    let cancelled = false;
    const load = () =>
      api
        .listDLQ({ event_type: eventType || undefined, source: source || undefined, limit, offset })
        .then((res) => {
          if (cancelled) return;
          setResult(res);
          setError(null);
        })
        .catch((err: ApiError) => !cancelled && setError(err));
    load();
    const timer = window.setInterval(load, REFRESH_MS);
    return () => {
      cancelled = true;
      window.clearInterval(timer);
    };
  }, [queryKey]);

  const page = result?.data;
  const filtered = Boolean(eventType || source);

  return (
    <section className="card">
      <form
        className="dlq-filters"
        onSubmit={(e) => {
          e.preventDefault();
          setParams({ event_type: draft.event_type.trim(), source: draft.source.trim(), offset: undefined });
        }}
      >
        <input
          placeholder="Event type, exact"
          value={draft.event_type}
          onChange={(e) => setDraft({ ...draft, event_type: e.target.value })}
        />
        <input
          placeholder="Source, exact"
          value={draft.source}
          onChange={(e) => setDraft({ ...draft, source: e.target.value })}
        />
        <button type="submit" className="btn btn-primary">
          Filter
        </button>
        {filtered && (
          <button
            type="button"
            className="btn btn-quiet"
            onClick={() => setParams({ event_type: undefined, source: undefined, offset: undefined })}
          >
            Reset
          </button>
        )}
      </form>

      <div className="card-head">
        <h2>Failed events</h2>
        {page && (
          <span className="muted">
            {page.total.toLocaleString()} {filtered ? "match" : "in the queue"}, newest first
          </span>
        )}
        <span className="muted request-meta">refreshes every {REFRESH_MS / 1000} s</span>
      </div>

      {error ? (
        <div className="empty">{error.message}</div>
      ) : !page ? (
        <div className="empty">Loading…</div>
      ) : page.events.length === 0 ? (
        <div className="empty">
          {filtered ? (
            "Nothing in the queue matches these filters."
          ) : (
            <>
              The queue is empty: nothing has failed. To see it working, publish some malformed messages with{" "}
              <code>./scripts/seed_dlq.sh</code>.
            </>
          )}
        </div>
      ) : (
        <>
          <DLQTable events={page.events} />
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
        </>
      )}
    </section>
  );
}
