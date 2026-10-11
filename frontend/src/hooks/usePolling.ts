import { useCallback, useEffect, useRef, useState } from "react";
import { ApiError } from "../api/client";

export interface PollState<T> {
  data: T | null;
  error: ApiError | null;
  loading: boolean;
  lastUpdated: Date | null;
  refresh: () => void;
}

/**
 * Calls `load` now and then every `intervalMs`, keeping the last good value.
 *
 * A failed poll keeps the previous data on screen next to the error rather
 * than blanking it, so a brief outage reads as "stale since 13:02" instead of
 * an empty page. Polling pauses while the tab is hidden: a console left open
 * in a background tab should not keep querying the API.
 */
export function usePolling<T>(load: () => Promise<T>, intervalMs: number): PollState<T> {
  const [data, setData] = useState<T | null>(null);
  const [error, setError] = useState<ApiError | null>(null);
  const [loading, setLoading] = useState(true);
  const [lastUpdated, setLastUpdated] = useState<Date | null>(null);

  // Held in a ref so a caller passing an inline arrow does not restart the
  // interval on every render.
  const loadRef = useRef(load);
  loadRef.current = load;

  const run = useCallback(async () => {
    try {
      const value = await loadRef.current();
      setData(value);
      setError(null);
      setLastUpdated(new Date());
    } catch (err) {
      setError(err instanceof ApiError ? err : new ApiError(0, String(err), null));
    } finally {
      setLoading(false);
    }
  }, []);

  useEffect(() => {
    run();
    const timer = window.setInterval(() => {
      if (document.visibilityState === "visible") run();
    }, intervalMs);
    return () => window.clearInterval(timer);
  }, [run, intervalMs]);

  return { data, error, loading, lastUpdated, refresh: run };
}
