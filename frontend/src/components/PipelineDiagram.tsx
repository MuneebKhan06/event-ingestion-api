import type { ReactNode } from "react";
import { EDGES, NODE_H, NODE_W, STAGES, VIEW_H, VIEW_W, type EdgeId, type StageId } from "../lib/pipeline";
import "./PipelineDiagram.css";

interface Props {
  selected?: StageId | null;
  // Stages to emphasise, e.g. the one a tour step is about.
  highlight?: StageId[];
  onSelect?: (stage: StageId) => void;
  // Optional content under each edge label, e.g. a live rate.
  edgeExtras?: Partial<Record<EdgeId, ReactNode>>;
}

/**
 * The pipeline as an SVG: where an event goes, from an HTTP request to a row
 * in Postgres and back out over SSE. Stages are buttons; colours come from the
 * theme tokens so the diagram reads the same in light and dark mode.
 */
export function PipelineDiagram({ selected, highlight = [], onSelect, edgeExtras = {} }: Props) {
  return (
    <svg
      className="pipeline"
      viewBox={`0 0 ${VIEW_W} ${VIEW_H}`}
      role="group"
      aria-label="Event pipeline: clients and the ingest poller post to FastAPI, which publishes to Kafka; the consumer stores events in PostgreSQL or routes failures to the dead letter queue; stored events stream back to clients over SSE."
    >
      <defs>
        <marker id="arrow" viewBox="0 0 10 10" refX="9" refY="5" markerWidth="7" markerHeight="7" orient="auto-start-reverse">
          <path d="M0,0 L10,5 L0,10 z" className="pipeline-arrowhead" />
        </marker>
        <marker id="arrow-fail" viewBox="0 0 10 10" refX="9" refY="5" markerWidth="7" markerHeight="7" orient="auto-start-reverse">
          <path d="M0,0 L10,5 L0,10 z" className="pipeline-arrowhead-fail" />
        </marker>
      </defs>

      {EDGES.map((edge) => {
        const failure = edge.id === "consumer-dlq";
        return (
          <g key={edge.id} className={`pipeline-edge${failure ? " pipeline-edge-fail" : ""}`}>
            <path d={edge.path} markerEnd={`url(#${failure ? "arrow-fail" : "arrow"})`} />
            <text x={edge.labelAt[0]} y={edge.labelAt[1]} textAnchor="middle" className="pipeline-edge-label">
              {edge.label}
            </text>
            {edgeExtras[edge.id] && (
              <text x={edge.labelAt[0]} y={edge.labelAt[1] + 14} textAnchor="middle" className="pipeline-edge-extra">
                {edgeExtras[edge.id]}
              </text>
            )}
          </g>
        );
      })}

      {STAGES.map((stage) => {
        const classes = [
          "pipeline-node",
          selected === stage.id && "selected",
          highlight.includes(stage.id) && "highlight",
          stage.id === "dlq" && "pipeline-node-dlq",
        ]
          .filter(Boolean)
          .join(" ");
        return (
          <g
            key={stage.id}
            className={classes}
            role="button"
            tabIndex={0}
            aria-pressed={selected === stage.id}
            aria-label={`${stage.title}: ${stage.subtitle}`}
            onClick={() => onSelect?.(stage.id)}
            onKeyDown={(e) => (e.key === "Enter" || e.key === " ") && (e.preventDefault(), onSelect?.(stage.id))}
          >
            <rect x={stage.x} y={stage.y} width={NODE_W} height={NODE_H} rx={10} />
            <text x={stage.x + 14} y={stage.y + 27} className="pipeline-node-title">
              {stage.title}
            </text>
            <text x={stage.x + 14} y={stage.y + 46} className="pipeline-node-sub">
              {stage.subtitle}
            </text>
          </g>
        );
      })}
    </svg>
  );
}
