import { useEffect, useState } from "react";
import "./EventFilters.css";

export interface FilterValues {
  event_type?: string;
  source?: string;
  since?: string;
  until?: string;
}

interface Props {
  values: FilterValues;
  onChange: (next: FilterValues) => void;
  // Values seen in results so far. The API has no "distinct types" endpoint,
  // so suggestions are learned from what has been loaded.
  knownTypes: string[];
  knownSources: string[];
}

// The API filters on timezone aware datetimes, while a datetime-local input
// speaks local wall clock time with no offset. Converting at this boundary
// keeps the URL holding the exact instant the API receives.
const toInput = (iso?: string) => {
  if (!iso) return "";
  const date = new Date(iso);
  const local = new Date(date.getTime() - date.getTimezoneOffset() * 60_000);
  return local.toISOString().slice(0, 16);
};
const fromInput = (value: string) => (value ? new Date(value).toISOString() : undefined);

const WINDOWS: { label: string; minutes: number }[] = [
  { label: "15 min", minutes: 15 },
  { label: "1 hour", minutes: 60 },
  { label: "24 hours", minutes: 1440 },
];

export function EventFilters({ values, onChange, knownTypes, knownSources }: Props) {
  // Text inputs are applied on submit, not per keystroke: each change is a
  // database query, and a half typed type name would only ever match nothing.
  const [draft, setDraft] = useState({ event_type: values.event_type ?? "", source: values.source ?? "" });

  useEffect(() => {
    setDraft({ event_type: values.event_type ?? "", source: values.source ?? "" });
  }, [values.event_type, values.source]);

  const active = Object.values(values).some(Boolean);

  return (
    <form
      className="filters"
      onSubmit={(e) => {
        e.preventDefault();
        onChange({ ...values, event_type: draft.event_type.trim(), source: draft.source.trim() });
      }}
    >
      <label>
        <span>Event type</span>
        <input
          list="known-types"
          placeholder="e.g. order.created"
          value={draft.event_type}
          onChange={(e) => setDraft({ ...draft, event_type: e.target.value })}
        />
        <datalist id="known-types">
          {knownTypes.map((t) => (
            <option key={t} value={t} />
          ))}
        </datalist>
      </label>

      <label>
        <span>Source</span>
        <input
          list="known-sources"
          placeholder="e.g. usgs"
          value={draft.source}
          onChange={(e) => setDraft({ ...draft, source: e.target.value })}
        />
        <datalist id="known-sources">
          {knownSources.map((s) => (
            <option key={s} value={s} />
          ))}
        </datalist>
      </label>

      <label>
        <span>Since</span>
        <input
          type="datetime-local"
          value={toInput(values.since)}
          onChange={(e) => onChange({ ...values, since: fromInput(e.target.value) })}
        />
      </label>

      <label>
        <span>Until</span>
        <input
          type="datetime-local"
          value={toInput(values.until)}
          onChange={(e) => onChange({ ...values, until: fromInput(e.target.value) })}
        />
      </label>

      <div className="filter-actions">
        <button type="submit" className="btn btn-primary">
          Apply
        </button>
        {active && (
          <button type="button" className="btn btn-quiet" onClick={() => onChange({})}>
            Reset
          </button>
        )}
      </div>

      <div className="filter-windows">
        <span>Last</span>
        {WINDOWS.map((w) => (
          <button
            key={w.label}
            type="button"
            className="chip"
            onClick={() =>
              onChange({
                ...values,
                since: new Date(Date.now() - w.minutes * 60_000).toISOString(),
                until: undefined,
              })
            }
          >
            {w.label}
          </button>
        ))}
        <span className="filter-note">
          Type and source are exact matches, as in the API. Times filter on when the consumer stored the event.
        </span>
      </div>
    </form>
  );
}
