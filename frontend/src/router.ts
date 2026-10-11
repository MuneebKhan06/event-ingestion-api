import { useEffect, useState } from "react";

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
