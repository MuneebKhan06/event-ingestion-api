import { useRef, useState } from "react";
import { TOUR, type StepResult, type TourContext } from "../lib/tour";
import "./GuidedTour.css";

interface Props {
  step: number;
  onStep: (step: number) => void;
  onClose: () => void;
}

/**
 * A walkthrough of the pipeline, one real request per step.
 *
 * The page owns the current step so the diagram can highlight the stages it
 * involves. Results are kept per step, so going back shows what happened.
 */
export function GuidedTour({ step, onStep, onClose }: Props) {
  const ctx = useRef<TourContext>({ eventId: null });
  const [results, setResults] = useState<Record<number, StepResult>>({});
  const [running, setRunning] = useState(false);
  const current = TOUR[step];
  const result = results[step];

  async function run() {
    setRunning(true);
    try {
      const outcome = await current.run(ctx.current);
      setResults((prev) => ({ ...prev, [step]: outcome }));
    } catch (err) {
      setResults((prev) => ({
        ...prev,
        [step]: { ok: false, text: err instanceof Error ? err.message : "The request failed." },
      }));
    } finally {
      setRunning(false);
    }
  }

  return (
    <section className="card tour" aria-live="polite">
      <div className="tour-head">
        <span className="tour-count">
          Step {step + 1} of {TOUR.length}
        </span>
        <ol className="tour-dots" aria-hidden>
          {TOUR.map((s, i) => (
            <li key={s.title} className={i === step ? "now" : results[i] ? "done" : ""} />
          ))}
        </ol>
        <button className="btn btn-quiet" onClick={onClose}>
          End tour
        </button>
      </div>

      <h2>{current.title}</h2>
      <p className="tour-explain">{current.explain}</p>

      <div className="tour-run">
        <button className="btn btn-primary" onClick={run} disabled={running}>
          {running ? "Running…" : result ? "Run again" : current.action}
        </button>
        {result && <p className={`tour-result ${result.ok ? "ok" : "bad"}`}>{result.text}</p>}
      </div>

      <div className="tour-nav">
        <button className="btn" disabled={step === 0} onClick={() => onStep(step - 1)}>
          Back
        </button>
        {step < TOUR.length - 1 ? (
          <button className="btn" onClick={() => onStep(step + 1)}>
            Next
          </button>
        ) : (
          <button className="btn" onClick={onClose}>
            Finish
          </button>
        )}
      </div>
    </section>
  );
}
