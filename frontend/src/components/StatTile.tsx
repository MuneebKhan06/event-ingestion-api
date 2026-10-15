import type { ReactNode } from "react";
import "./StatTile.css";

const COMPACT = new Intl.NumberFormat(undefined, { notation: "compact", maximumFractionDigits: 1 });

export function compact(value: number): string {
  return value < 10_000 ? value.toLocaleString() : COMPACT.format(value);
}

interface Props {
  label: string;
  value: number | undefined;
  ratePerMinute: number | null;
  help?: string;
  bad?: boolean;
  trend?: ReactNode;
}

function rateText(rate: number | null): string {
  if (rate === null) return "measuring rate…";
  if (rate === 0) return "idle";
  return `${rate >= 10 ? Math.round(rate) : rate.toFixed(1)} / min`;
}

/**
 * One counter: its total since the process started, and how fast it is moving.
 *
 * The rate is the useful half. A total of 400 says little; 400 that stopped
 * moving two minutes ago says the pipeline is stalled.
 */
export function StatTile({ label, value, ratePerMinute, help, bad, trend }: Props) {
  const alarming = bad && (ratePerMinute ?? 0) > 0;
  return (
    <div className={`stat-tile${alarming ? " stat-alarm" : ""}`} title={help}>
      <span className="stat-label">{label}</span>
      <span className="stat-value">{value === undefined ? "n/a" : compact(value)}</span>
      <span className="stat-rate">
        {alarming && <span aria-hidden>▲ </span>}
        {rateText(ratePerMinute)}
      </span>
      {trend}
    </div>
  );
}
