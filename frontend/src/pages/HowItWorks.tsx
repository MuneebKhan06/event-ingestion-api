import { useRef, useState } from "react";
import { DecisionList } from "../components/DecisionList";
import { PipelineDiagram } from "../components/PipelineDiagram";
import { StageDetails } from "../components/StageDetails";
import { StatusBadge } from "../components/StatusBadge";
import { SCRAPE_MS, useMetrics } from "../hooks/useMetrics";
import { edgeCounts, formatEdge } from "../lib/edgeMetrics";
import type { EdgeId, StageId } from "../lib/pipeline";
import "./HowItWorks.css";

export function HowItWorks() {
  const [selected, setSelected] = useState<StageId | null>(null);
  const [focusedDecision, setFocusedDecision] = useState<number | null>(null);
  const diagramRef = useRef<HTMLElement>(null);

  // Stages and decisions link both ways, so a viewer can start from either
  // "what is this box" or "why was it built like this".
  const showDecision = (n: number) => {
    setFocusedDecision(n);
    document.getElementById(`decision-${n}`)?.scrollIntoView({ behavior: "smooth", block: "center" });
  };
  const showStage = (stage: StageId) => {
    setSelected(stage);
    diagramRef.current?.scrollIntoView({ behavior: "smooth", block: "start" });
  };
  const metrics = useMetrics();
  const counts = edgeCounts(metrics);
  const extras = Object.fromEntries(
    Object.entries(counts).map(([edge, count]) => [edge, formatEdge(count!)]),
  ) as Partial<Record<EdgeId, string[]>>;
  const live = Object.keys(counts).length > 0;

  return (
    <div className="grid">
      <section className="card" ref={diagramRef}>
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

      <StageDetails stage={selected} onDecision={showDecision} />

      <DecisionList focused={focusedDecision} onStage={showStage} />
    </div>
  );
}
