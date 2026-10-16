import { DECISIONS } from "../lib/decisions";
import { STAGES, type StageId } from "../lib/pipeline";
import "./DecisionList.css";

interface Props {
  // The decision to draw attention to, e.g. one picked from a stage.
  focused: number | null;
  onStage?: (stage: StageId) => void;
}

const stageTitle = (id: StageId) => STAGES.find((s) => s.id === id)!.title;

export function DecisionList({ focused, onStage }: Props) {
  return (
    <section className="decisions">
      <div className="decisions-head">
        <h2>Design decisions</h2>
        <span className="muted">Each one names what it costs. The README has the full reasoning and the options rejected.</span>
      </div>
      <div className="decision-grid">
        {DECISIONS.map((d) => (
          <article
            key={d.n}
            id={`decision-${d.n}`}
            className={`card decision${focused === d.n ? " focused" : ""}`}
          >
            <span className="decision-n">Decision {d.n}</span>
            <h3>{d.title}</h3>
            <p className="decision-q">{d.question}</p>
            <dl>
              <dt>Chose</dt>
              <dd>{d.chose}</dd>
              <dt>Why</dt>
              <dd>{d.why}</dd>
              <dt>Cost</dt>
              <dd>{d.cost}</dd>
            </dl>
            <div className="decision-stages">
              {d.stages.map((s) => (
                <button key={s} className="chip" onClick={() => onStage?.(s)}>
                  {stageTitle(s)}
                </button>
              ))}
            </div>
          </article>
        ))}
      </div>
    </section>
  );
}
