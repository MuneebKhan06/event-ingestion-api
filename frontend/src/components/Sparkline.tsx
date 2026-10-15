import { useState } from "react";
import "./Sparkline.css";

interface Props {
  // Increase per scrape interval, oldest first.
  values: number[];
  // Seconds per value, for the hover text.
  intervalSeconds: number;
  label: string;
}

const POINTS = 12;
const WIDTH = 120;
const HEIGHT = 28;
const GAP = 2;

/**
 * Column sparkline of a counter's increase per scrape.
 *
 * Columns rather than a line: each value is a discrete count of events in one
 * interval, and a zero should read as an empty slot, not a dip in a curve.
 * History in the de-emphasis tone, the newest interval in the accent. Hover a
 * column for its exact count.
 */
export function Sparkline({ values, intervalSeconds, label }: Props) {
  const [hover, setHover] = useState<number | null>(null);
  const shown = values.slice(-POINTS);
  // Pad from the left so a short history still sits at the right edge, where
  // "now" is, instead of stretching across the tile.
  const slots: (number | null)[] = [...Array(Math.max(0, POINTS - shown.length)).fill(null), ...shown];
  const max = Math.max(1, ...shown);
  const step = WIDTH / POINTS;
  const barWidth = step - GAP;

  const hoverText =
    hover !== null && slots[hover] !== null
      ? `+${slots[hover]} in ${intervalSeconds} s, ${(POINTS - 1 - hover) * intervalSeconds} s ago`
      : null;

  return (
    <div className="sparkline">
      <svg
        viewBox={`0 0 ${WIDTH} ${HEIGHT}`}
        width="100%"
        height={HEIGHT}
        preserveAspectRatio="none"
        role="img"
        aria-label={`${label}: increase per ${intervalSeconds} s over the last ${shown.length} scrapes: ${shown.join(", ")}`}
        onMouseLeave={() => setHover(null)}
      >
        {slots.map((value, i) => {
          const x = i * step;
          const h = value ? Math.max(2, (value / max) * HEIGHT) : 0;
          const newest = i === POINTS - 1;
          return (
            <g key={i} onMouseEnter={() => setHover(i)}>
              {/* Full height hit target, wider than the visible column. */}
              <rect x={x} y={0} width={step} height={HEIGHT} fill="transparent" />
              {value === null ? null : value === 0 ? (
                <rect x={x} y={HEIGHT - 1} width={barWidth} height={1} className="spark-zero" />
              ) : (
                <rect
                  x={x}
                  y={HEIGHT - h}
                  width={barWidth}
                  height={h}
                  rx={1.5}
                  className={newest ? "spark-now" : hover === i ? "spark-hover" : "spark-past"}
                />
              )}
            </g>
          );
        })}
      </svg>
      <span className="spark-caption">{hoverText ?? `last ${POINTS * intervalSeconds} s`}</span>
    </div>
  );
}
