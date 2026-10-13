import { useState } from "react";
import { Composer } from "../components/Composer";
import { newEventId, toRequestBody, validateDraft, type Draft } from "../lib/validate";
import "./Send.css";

const INITIAL: Draft = {
  event_id: newEventId(),
  event_type: "order.created",
  source: "order-service",
  payloadText: JSON.stringify({ order_id: "ORD-001", user_id: "USR-123", amount: 99.99, currency: "USD" }, null, 2),
};

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
          {JSON.stringify(toRequestBody(draft), null, 2)}
        </pre>
      </section>
    </div>
  );
}
