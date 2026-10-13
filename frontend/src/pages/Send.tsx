import { useState } from "react";
import { api, ApiError } from "../api/client";
import { Composer } from "../components/Composer";
import { DuplicateDemo } from "../components/DuplicateDemo";
import { PresetBar } from "../components/PresetBar";
import { ResponsePanel, type SendOutcome } from "../components/ResponsePanel";
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
  // Off lets an invalid draft through so the server's own 422 can be shown;
  // the browser check is a convenience, the API is the actual gate.
  const [browserChecks, setBrowserChecks] = useState(true);
  const [sending, setSending] = useState(false);
  const [outcome, setOutcome] = useState<SendOutcome | null>(null);

  const errors = validateDraft(draft);
  const valid = Object.keys(errors).length === 0;
  const shownErrors = browserChecks ? errors : {};

  async function send() {
    const body = toRequestBody(draft);
    const sentAt = new Date();
    const started = performance.now();
    setSending(true);
    try {
      const res = await api.sendEvent(body);
      setOutcome({ status: res.status, body: res.data, requestId: res.requestId, elapsedMs: res.elapsedMs, eventId: draft.event_id, sentAt });
    } catch (err) {
      const e = err as ApiError;
      setOutcome({
        status: e.status,
        body: { detail: e.detail },
        requestId: e.requestId,
        elapsedMs: Math.round(performance.now() - started),
        eventId: draft.event_id,
        sentAt,
      });
    } finally {
      setSending(false);
    }
  }

  return (
    <div className="send-layout">
      <section className="card">
        <div className="card-head">
          <h2>Compose</h2>
          <span className="muted">Checked against the same rules as the API's Pydantic model</span>
        </div>
        <PresetBar
          onPick={(next, preset) => {
            setDraft(next);
            // A "break one rule" preset exists to reach the server and be
            // refused there, so it switches the browser checks off.
            setBrowserChecks(preset.expects !== 422);
          }}
        />
        <Composer draft={draft} errors={shownErrors} onChange={setDraft}>
          <div className="send-bar">
            <button
              type="button"
              className="btn btn-primary"
              disabled={sending || (browserChecks && !valid)}
              onClick={send}
            >
              {sending ? "Sending…" : "Send event"}
            </button>
            <label className="toggle">
              <input type="checkbox" checked={browserChecks} onChange={(e) => setBrowserChecks(e.target.checked)} />
              Browser checks
            </label>
            {!browserChecks && (
              <span className="muted">Invalid events go straight to the API, which rejects them itself.</span>
            )}
          </div>
        </Composer>
      </section>

      <div className="send-side">
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
        <ResponsePanel outcome={outcome} />
        <DuplicateDemo draft={draft} onResend={send} canResend={valid && outcome !== null && !sending} />
      </div>
    </div>
  );
}
