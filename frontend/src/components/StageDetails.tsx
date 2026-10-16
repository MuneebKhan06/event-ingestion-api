import { STAGES, type StageId } from "../lib/pipeline";
import { STAGE_DETAILS } from "../lib/stageDetails";
import { href } from "../router";
import "./StageDetails.css";

interface Props {
  stage: StageId | null;
  onDecision?: (decision: number) => void;
}

export function StageDetails({ stage, onDecision }: Props) {
  if (!stage) {
    return (
      <section className="card stage-details stage-empty">
        <p className="muted">
          Select a stage in the diagram to see what it does, how it behaves, and which design decisions shaped it.
        </p>
      </section>
    );
  }

  const meta = STAGES.find((s) => s.id === stage)!;
  const detail = STAGE_DETAILS[stage];

  return (
    <section className="card stage-details" aria-live="polite">
      <div className="card-head">
        <h2>{meta.title}</h2>
        <span className="muted">{meta.subtitle}</span>
      </div>
      <p className="stage-does">{detail.does}</p>
      <ul className="stage-facts">
        {detail.facts.map((fact) => (
          <li key={fact}>{fact}</li>
        ))}
      </ul>
      <div className="stage-links">
        {detail.decisions.map((n) => (
          <button key={n} className="chip" onClick={() => onDecision?.(n)}>
            Decision {n}
          </button>
        ))}
        <a className="stage-screen" href={href(detail.screen.route)}>
          {detail.screen.label} →
        </a>
        {detail.external && (
          <a className="stage-screen" href={detail.external.href} target="_blank" rel="noreferrer">
            {detail.external.label} ↗
          </a>
        )}
      </div>
    </section>
  );
}
