import type { CSSProperties } from "react";
import type { StoredEvent } from "../api/types";
import { domainHue, fullTime, payloadPreview, timeAgo } from "../lib/format";
import "./EventTable.css";

interface Props {
  events: StoredEvent[];
  onSelect?: (event: StoredEvent) => void;
}

export function TypeChip({ eventType }: { eventType: string }) {
  return (
    <span className="type-chip" style={{ "--hue": domainHue(eventType) } as CSSProperties}>
      {eventType}
    </span>
  );
}

export function EventTable({ events, onSelect }: Props) {
  return (
    <div className="table-wrap">
      <table className="event-table">
        <thead>
          <tr>
            <th>#</th>
            <th>Type</th>
            <th>Source</th>
            <th>Payload</th>
            <th>Event ID</th>
            <th>Stored</th>
          </tr>
        </thead>
        <tbody>
          {events.map((event) => (
            <tr key={event.id} onClick={() => onSelect?.(event)} className={onSelect ? "clickable" : undefined}>
              <td className="num">{event.id}</td>
              <td>
                <TypeChip eventType={event.event_type} />
              </td>
              <td>{event.source}</td>
              <td className="preview">{payloadPreview(event.payload)}</td>
              <td>
                <code title={event.event_id}>{event.event_id.slice(0, 8)}</code>
              </td>
              <td className="when" title={fullTime(event.created_at)}>
                {timeAgo(event.created_at)}
              </td>
            </tr>
          ))}
        </tbody>
      </table>
    </div>
  );
}
