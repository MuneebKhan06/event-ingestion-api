import type { DLQEvent } from "../api/types";
import { fullTime, timeAgo } from "../lib/format";
import { TypeChip } from "./EventTable";
import "./EventTable.css";

interface Props {
  events: DLQEvent[];
  onSelect?: (event: DLQEvent) => void;
}

// Every column but the reason can be missing: a message with no event_id or
// no event_type is exactly the kind of thing that ends up here.
const missing = <span className="muted">missing</span>;

export function DLQTable({ events, onSelect }: Props) {
  return (
    <div className="table-wrap">
      <table className="event-table">
        <thead>
          <tr>
            <th>#</th>
            <th>Type</th>
            <th>Source</th>
            <th>Error</th>
            <th>Event ID</th>
            <th>Failed</th>
          </tr>
        </thead>
        <tbody>
          {events.map((event) => (
            <tr key={event.id} onClick={() => onSelect?.(event)} className={onSelect ? "clickable" : undefined}>
              <td className="num">{event.id}</td>
              <td>{event.event_type ? <TypeChip eventType={event.event_type} /> : missing}</td>
              <td>{event.source ?? missing}</td>
              <td className="preview" title={event.error_reason}>
                {event.error_reason}
              </td>
              <td>{event.event_id ? <code title={event.event_id}>{event.event_id.slice(0, 8)}</code> : missing}</td>
              <td className="when" title={fullTime(event.failed_at)}>
                {timeAgo(event.failed_at)}
              </td>
            </tr>
          ))}
        </tbody>
      </table>
    </div>
  );
}
