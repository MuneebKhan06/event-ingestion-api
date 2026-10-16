import { useRef, useState } from "react";
import { useHashParams } from "../router";
import { DecisionList } from "../components/DecisionList";
import { GuidedTour } from "../components/GuidedTour";
import { PipelineDiagram } from "../components/PipelineDiagram";
import { StageDetails } from "../components/StageDetails";
import { StatusBadge } from "../components/StatusBadge";
import { SCRAPE_MS, useMetrics } from "../hooks/useMetrics";
import { edgeCounts, formatEdge } from "../lib/edgeMetrics";
import { STAGES, type EdgeId, type StageId } from "../lib/pipeline";
import { TOUR } from "../lib/tour";
import "./HowItWorks.css";

export function HowItWorks() {
  // The selected stage and the tour step live in the URL, so a link such as
  // #/how?tour=1 opens straight into the tour, and a reload keeps your place.
  const [params, setParams] = useHashParams();
  const stageParam = params.get("stage");
  const selected = STAGES.some((s) => s.id === stageParam) ? (stageParam as StageId) : null;
  const setSelected = (stage: StageId | null) => setParams({ stage: stage ?? undefined });
  const tourParam = Number(params.get("tour"));
  // null when the tour is not running. Steps are 1 based in the URL.
  const tourStep = tourParam >= 1 && tourParam <= TOUR.length ? tourParam - 1 : null;
  const setTourStep = (step: number | null) => setParams({ tour: step === null ? undefined : String(step + 1) });

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
          <span className="request-meta tour-start">
            <StatusBadge tone={live ? "ok" : "idle"}>{live ? "live counts" : "waiting for metrics"}</StatusBadge>
            {tourStep === null && (
              <button className="btn btn-primary" onClick={() => setTourStep(0)}>
                Start guided tour
              </button>
            )}
          </span>
        </div>
        <div className="pipeline-wrap">
          <PipelineDiagram
            selected={selected}
            highlight={tourStep === null ? [] : TOUR[tourStep].stages}
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

      {tourStep !== null && <GuidedTour step={tourStep} onStep={setTourStep} onClose={() => setTourStep(null)} />}

      <StageDetails stage={selected} onDecision={showDecision} />

      <DecisionList focused={focusedDecision} onStage={showStage} />
    </div>
  );
}
