import { useState } from "react";
import { PipelineDiagram } from "../components/PipelineDiagram";
import type { StageId } from "../lib/pipeline";
import "./HowItWorks.css";

export function HowItWorks() {
  const [selected, setSelected] = useState<StageId | null>(null);

  return (
    <div className="grid">
      <section className="card">
        <div className="card-head">
          <h2>The pipeline</h2>
          <span className="muted">Click a stage to see what it does and why it was built that way</span>
        </div>
        <div className="pipeline-wrap">
          <PipelineDiagram selected={selected} onSelect={(id) => setSelected(id === selected ? null : id)} />
        </div>
        <p className="muted pipeline-caption">
          The write path runs left to right and never waits on the database: the API answers as soon as Kafka has
          the event. The read path runs along the bottom, streaming rows back out only once they are stored.
        </p>
      </section>
    </div>
  );
}
