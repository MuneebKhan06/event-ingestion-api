import type { ReactNode } from "react";
import {
  MAX_FIELD_LENGTH,
  MAX_PAYLOAD_BYTES,
  newEventId,
  parsePayload,
  payloadBytes,
  type Draft,
  type DraftErrors,
} from "../lib/validate";
import "./Composer.css";

interface Props {
  draft: Draft;
  errors: DraftErrors;
  onChange: (draft: Draft) => void;
  // Rendered under the form; the send controls live with the page.
  children?: ReactNode;
}

function Field({ label, hint, error, children }: { label: string; hint?: string; error?: string; children: ReactNode }) {
  return (
    <label className={`field${error ? " has-error" : ""}`}>
      <span className="field-label">
        {label}
        {hint && <span className="field-hint">{hint}</span>}
      </span>
      {children}
      {error && <span className="field-error">{error}</span>}
    </label>
  );
}

export function Composer({ draft, errors, onChange, children }: Props) {
  const set = (patch: Partial<Draft>) => onChange({ ...draft, ...patch });
  const parsed = parsePayload(draft.payloadText);
  const bytes = parsed.value ? payloadBytes(parsed.value) : null;

  return (
    <div className="composer">
      <Field
        label="Event ID"
        hint="the idempotency key"
        error={errors.event_id}
      >
        <div className="input-row">
          <input
            className="mono"
            value={draft.event_id}
            onChange={(e) => set({ event_id: e.target.value })}
            spellCheck={false}
          />
          <button type="button" className="btn" onClick={() => set({ event_id: newEventId() })}>
            New ID
          </button>
        </div>
      </Field>

      <div className="field-pair">
        <Field label="Event type" hint={`<domain>.<action>, max ${MAX_FIELD_LENGTH}`} error={errors.event_type}>
          <input
            className="mono"
            value={draft.event_type}
            placeholder="order.created"
            onChange={(e) => set({ event_type: e.target.value })}
            spellCheck={false}
          />
        </Field>
        <Field label="Source" hint={`max ${MAX_FIELD_LENGTH}`} error={errors.source}>
          <input
            className="mono"
            value={draft.source}
            placeholder="order-service"
            onChange={(e) => set({ source: e.target.value })}
            spellCheck={false}
          />
        </Field>
      </div>

      <Field
        label="Payload"
        hint={
          bytes !== null
            ? `${bytes.toLocaleString()} of ${MAX_PAYLOAD_BYTES.toLocaleString()} bytes`
            : "any JSON object"
        }
        error={errors.payloadText}
      >
        <textarea
          className="mono"
          rows={10}
          value={draft.payloadText}
          onChange={(e) => set({ payloadText: e.target.value })}
          spellCheck={false}
        />
      </Field>

      {parsed.value && (
        <button
          type="button"
          className="btn btn-quiet format-btn"
          onClick={() => set({ payloadText: JSON.stringify(parsed.value, null, 2) })}
        >
          Format JSON
        </button>
      )}

      {children}
    </div>
  );
}
