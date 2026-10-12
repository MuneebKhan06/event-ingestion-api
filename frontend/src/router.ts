import { useCallback, useEffect, useState } from "react";

// Hash routing keeps the build servable from any static host or a plain nginx
// location with no rewrite rules, which matters for the Day 7 Docker image.

export type Route = "overview" | "stream" | "events" | "send" | "dlq" | "metrics" | "how";

const ROUTES: Route[] = ["overview", "stream", "events", "send", "dlq", "metrics", "how"];

function parse(hash: string): Route {
  const name = hash.replace(/^#\/?/, "").split("?")[0];
  return (ROUTES as string[]).includes(name) ? (name as Route) : "overview";
}

export function useRoute(): Route {
  const [route, setRoute] = useState<Route>(() => parse(window.location.hash));

  useEffect(() => {
    const onChange = () => setRoute(parse(window.location.hash));
    window.addEventListener("hashchange", onChange);
    return () => window.removeEventListener("hashchange", onChange);
  }, []);

  return route;
}

export const href = (route: Route) => `#/${route}`;

const readHashParams = () => new URLSearchParams(window.location.hash.split("?")[1] ?? "");

/**
 * Query parameters carried in the hash, e.g. #/events?source=usgs.
 *
 * Filters live here rather than in component state so a filtered view
 * survives a reload and can be shared as a link during a demo.
 */
export function useHashParams(): [URLSearchParams, (next: Record<string, string | undefined>) => void] {
  const [params, setParams] = useState(readHashParams);

  useEffect(() => {
    const onChange = () => setParams(readHashParams());
    window.addEventListener("hashchange", onChange);
    return () => window.removeEventListener("hashchange", onChange);
  }, []);

  // Stable across renders, so callers can hand it to effects and memoised
  // callbacks without resubscribing every time.
  const update = useCallback((next: Record<string, string | undefined>) => {
    const merged = readHashParams();
    for (const [key, value] of Object.entries(next)) {
      if (value === undefined || value === "") merged.delete(key);
      else merged.set(key, value);
    }
    const route = window.location.hash.replace(/^#\/?/, "").split("?")[0] || "overview";
    const query = merged.toString();
    window.location.hash = `/${route}${query ? `?${query}` : ""}`;
  }, []);

  return [params, update];
}
