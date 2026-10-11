import type { ReactNode } from "react";
import { href, type Route } from "../router";

interface NavItem {
  route: Route;
  label: string;
  icon: ReactNode;
}

const stroke = {
  fill: "none",
  stroke: "currentColor",
  strokeWidth: 1.8,
  strokeLinecap: "round" as const,
  strokeLinejoin: "round" as const,
};

const NAV: NavItem[] = [
  {
    route: "overview",
    label: "Overview",
    icon: (
      <svg viewBox="0 0 24 24" {...stroke}>
        <rect x="3" y="3" width="7" height="9" rx="1.5" />
        <rect x="14" y="3" width="7" height="5" rx="1.5" />
        <rect x="14" y="12" width="7" height="9" rx="1.5" />
        <rect x="3" y="16" width="7" height="5" rx="1.5" />
      </svg>
    ),
  },
  {
    route: "stream",
    label: "Live stream",
    icon: (
      <svg viewBox="0 0 24 24" {...stroke}>
        <path d="M3 12h4l3 7 4-14 3 7h4" />
      </svg>
    ),
  },
  {
    route: "events",
    label: "Events explorer",
    icon: (
      <svg viewBox="0 0 24 24" {...stroke}>
        <ellipse cx="12" cy="5" rx="8" ry="3" />
        <path d="M4 5v14c0 1.7 3.6 3 8 3s8-1.3 8-3V5M4 12c0 1.7 3.6 3 8 3s8-1.3 8-3" />
      </svg>
    ),
  },
  {
    route: "send",
    label: "Send event",
    icon: (
      <svg viewBox="0 0 24 24" {...stroke}>
        <path d="M22 2 11 13M22 2l-7 20-4-9-9-4 20-7z" />
      </svg>
    ),
  },
  {
    route: "dlq",
    label: "Dead letter queue",
    icon: (
      <svg viewBox="0 0 24 24" {...stroke}>
        <path d="M12 9v4M12 17h.01M10.3 3.9 1.8 18a2 2 0 0 0 1.7 3h17a2 2 0 0 0 1.7-3L13.7 3.9a2 2 0 0 0-3.4 0z" />
      </svg>
    ),
  },
  {
    route: "metrics",
    label: "Metrics",
    icon: (
      <svg viewBox="0 0 24 24" {...stroke}>
        <path d="M3 3v18h18M7 15l4-4 3 3 6-6" />
      </svg>
    ),
  },
  {
    route: "how",
    label: "How it works",
    icon: (
      <svg viewBox="0 0 24 24" {...stroke}>
        <circle cx="12" cy="12" r="9" />
        <path d="M9.1 9a3 3 0 0 1 5.8 1c0 2-3 3-3 3M12 17h.01" />
      </svg>
    ),
  },
];

export function Sidebar({ current }: { current: Route }) {
  return (
    <nav className="sidebar" aria-label="Main">
      <div className="brand">
        <img src="/favicon.svg" alt="" />
        <div>
          Event Ingestion
          <small>FastAPI · Kafka · Postgres</small>
        </div>
      </div>

      {NAV.map((item) => (
        <a
          key={item.route}
          href={href(item.route)}
          className={`nav-link${item.route === current ? " active" : ""}`}
          aria-current={item.route === current ? "page" : undefined}
        >
          {item.icon}
          {item.label}
        </a>
      ))}

      <div className="sidebar-footer">
        Reading the compose stack through the dev proxy on <code>:8000</code>, <code>:9100</code> and{" "}
        <code>:9200</code>.
      </div>
    </nav>
  );
}
