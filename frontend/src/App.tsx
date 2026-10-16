import { PageHeader } from "./components/PageHeader";
import { Sidebar } from "./components/Sidebar";
import { DLQ } from "./pages/DLQ";
import { Events } from "./pages/Events";
import { HowItWorks } from "./pages/HowItWorks";
import { Metrics } from "./pages/Metrics";
import { Overview } from "./pages/Overview";
import { Send } from "./pages/Send";
import { Stream } from "./pages/Stream";
import { useRoute, type Route } from "./router";

interface Screen {
  title: string;
  stage: string;
  intro: string;
}

// Every screen opens by saying which part of the pipeline it shows. The
// console is meant to explain the system to someone seeing it for the first
// time, not only to operate it.
const SCREENS: Record<Route, Screen> = {
  overview: {
    title: "Overview",
    stage: "GET /health",
    intro:
      "The state of the whole pipeline at a glance: whether the API can reach Kafka and Postgres, and what is flowing through it right now.",
  },
  stream: {
    title: "Live stream",
    stage: "GET /events/stream",
    intro:
      "Events appear here once the consumer has written them to Postgres. The feed reads stored rows, not Kafka, so everything shown is durable.",
  },
  events: {
    title: "Events explorer",
    stage: "GET /events",
    intro: "Search everything the consumer has persisted, newest first, by type, source and time window.",
  },
  send: {
    title: "Send event",
    stage: "POST /events",
    intro:
      "Publish an event through the API and see how it answers: accepted, rejected as a duplicate, or refused by validation.",
  },
  dlq: {
    title: "Dead letter queue",
    stage: "GET /dlq",
    intro:
      "Events the consumer could not process are kept here with the reason, so a failure can be inspected and replayed instead of lost.",
  },
  metrics: {
    title: "Metrics",
    stage: "/metrics on :8000, :9100, :9200",
    intro: "Prometheus counters from all three processes: the API, the consumer and the ingest poller.",
  },
  how: {
    title: "How it works",
    stage: "Architecture",
    intro: "The path one event takes from an HTTP request to a row in Postgres, and the decisions behind each step.",
  },
};

export function App() {
  const route = useRoute();
  const screen = SCREENS[route];

  return (
    <div className="layout">
      <Sidebar current={route} />
      <main className="main">
        <PageHeader title={screen.title} stage={screen.stage}>
          {screen.intro}
        </PageHeader>
        {route === "overview" ? (
          <Overview />
        ) : route === "stream" ? (
          <Stream />
        ) : route === "events" ? (
          <Events />
        ) : route === "send" ? (
          <Send />
        ) : route === "dlq" ? (
          <DLQ />
        ) : route === "metrics" ? (
          <Metrics />
        ) : route === "how" ? (
          <HowItWorks />
        ) : (
          <div className="card empty">This screen is on the plan and arrives in a later step.</div>
        )}
      </main>
    </div>
  );
}
