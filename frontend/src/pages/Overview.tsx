import { HealthPanel } from "../components/HealthPanel";
import { LiveFeed } from "../components/LiveFeed";
import { href } from "../router";
import "./Overview.css";

// A first time viewer lands here, so the page says what the system is before
// it shows any numbers.
const STEPS = [
  {
    title: "Accept",
    text: "POST /events validates the body with Pydantic and publishes it to Kafka, answering 202 without touching the database.",
    link: href("send"),
    cta: "Send an event",
  },
  {
    title: "Process",
    text: "A consumer group reads events.raw, retries with backoff and writes each event once, enforced by a unique constraint.",
    link: href("stream"),
    cta: "Watch them land",
  },
  {
    title: "Recover",
    text: "Anything that cannot be processed goes to the dead letter queue with its reason, ready to inspect and replay.",
    link: href("dlq"),
    cta: "Open the DLQ",
  },
];

export function Overview() {
  return (
    <div className="grid">
      <section className="steps">
        {STEPS.map((step, index) => (
          <article key={step.title} className="card step">
            <span className="step-number">{index + 1}</span>
            <h2>{step.title}</h2>
            <p>{step.text}</p>
            <a href={step.link}>{step.cta} →</a>
          </article>
        ))}
      </section>

      <HealthPanel />
      <LiveFeed limit={8} compact />
    </div>
  );
}
