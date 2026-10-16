import { useState } from "react";
import { PipelineDiagram } from "../components/PipelineDiagram";
import { StatusBadge } from "../components/StatusBadge";
import { SCRAPE_MS, useMetrics } from "../hooks/useMetrics";
import { edgeCounts, formatEdge } from "../lib/edgeMetrics";
import type { EdgeId, StageId } from "../lib/pipeline";
import "./HowItWorks.css";

export function HowItWorks() {
  const [selected, setSelected] = useState<StageId | null>(null);
  const metrics = useMetrics();
  const counts = edgeCounts(metrics);
  const extras = Object.fromEntries(
    Object.entries(counts).map(([edge, count]) => [edge, formatEdge(count!)]),
  ) as Partial<Record<EdgeId, string[]>>;
  const live = Object.keys(counts).length > 0;

  return (
    <div className="grid">
      <section className="card">
        <div className="card-head">
          <h2>The pipeline</h2>
          <span className="muted">Click a stage to see what it does and why it was built that way</span>
          <span className="request-meta">
            <StatusBadge tone={live ? "ok" : "idle"}>{live ? "live counts" : "waiting for metrics"}</StatusBadge>
          </span>
        </div>
        <div className="pipeline-wrap">
          <PipelineDiagram
            selected={selected}
            onSelect={(id) => setSelected(id === selected ? null : id)}
            edgeExtras={extras}
          />
        </div>
        <p className="muted pipeline-caption">
          The write path runs left to right and never waits on the database: the API answers as soon as Kafka has
          the event. The read path runs along the bottom, streaming rows back out only once they are stored.
          Edge counts come from each process's Prometheus counters, scraped every {SCRAPE_MS / 1000} s; totals count
          since that process started.
        </p>
      </section>
    </div>
  );
}
