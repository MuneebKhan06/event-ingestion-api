import { useState } from "react";
import { Composer } from "../components/Composer";
import { PresetBar } from "../components/PresetBar";
import { newEventId, toRequestBody, validateDraft, type Draft } from "../lib/validate";
import "./Send.css";

const INITIAL: Draft = {
  event_id: newEventId(),
  event_type: "order.created",
  source: "order-service",
  payloadText: JSON.stringify({ order_id: "ORD-001", user_id: "USR-123", amount: 99.99, currency: "USD" }, null, 2),
};

// Long enough to read any realistic event, short enough that the oversized
// preset does not turn the preview into 32 KB of x.
const PREVIEW_CHARS = 3000;

function preview(body: unknown): string {
  const text = JSON.stringify(body, null, 2);
  return text.length > PREVIEW_CHARS
    ? `${text.slice(0, PREVIEW_CHARS)}\n… ${(text.length - PREVIEW_CHARS).toLocaleString()} more characters`
    : text;
}

export function Send() {
  const [draft, setDraft] = useState<Draft>(INITIAL);
  const errors = validateDraft(draft);
  const valid = Object.keys(errors).length === 0;

  return (
    <div className="send-layout">
      <section className="card">
        <div className="card-head">
          <h2>Compose</h2>
          <span className="muted">Checked against the same rules as the API's Pydantic model</span>
        </div>
        <PresetBar onPick={(next) => setDraft(next)} />
        <Composer draft={draft} errors={errors} onChange={setDraft} />
      </section>

      <section className="card">
        <div className="card-head">
          <h2>Request</h2>
          <span className={valid ? "muted" : "send-invalid"}>
            {valid ? "Valid, ready to send" : `${Object.keys(errors).length} problem(s) to fix`}
          </span>
        </div>
        <pre className="request-preview">
          <span className="request-line">POST /events</span>
          {"\n"}Content-Type: application/json{"\n\n"}
          {preview(toRequestBody(draft))}
        </pre>
      </section>
    </div>
  );
}
