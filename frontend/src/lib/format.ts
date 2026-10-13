const RELATIVE = new Intl.RelativeTimeFormat(undefined, { numeric: "auto" });

const STEPS: [Intl.RelativeTimeFormatUnit, number][] = [
  ["second", 60],
  ["minute", 60],
  ["hour", 24],
  ["day", 30],
  ["month", 12],
  ["year", Infinity],
];

/** "3 minutes ago", for scanning a table; the exact time goes in a tooltip. */
export function timeAgo(iso: string, now = Date.now()): string {
  let value = (new Date(iso).getTime() - now) / 1000;
  for (const [unit, size] of STEPS) {
    if (Math.abs(value) < size) return RELATIVE.format(Math.round(value), unit);
    value /= size;
  }
  return iso;
}

export function fullTime(iso: string): string {
  return new Date(iso).toLocaleString();
}

/** One line preview of a payload, so a row hints at its content. */
export function payloadPreview(payload: Record<string, unknown>, max = 80): string {
  const text = Object.entries(payload)
    .map(([key, value]) => `${key}: ${typeof value === "object" ? JSON.stringify(value) : String(value)}`)
    .join(", ");
  return text.length > max ? `${text.slice(0, max - 1)}…` : text;
}

// The domain half of "<domain>.<action>" picks the colour, so order, user,
// quake and weather events are easy to tell apart at a glance.
const DOMAIN_HUES = [212, 145, 32, 280, 350, 180];

export function domainHue(eventType: string): number {
  const domain = eventType.split(".")[0];
  let hash = 0;
  for (const char of domain) hash = (hash * 31 + char.charCodeAt(0)) >>> 0;
  return DOMAIN_HUES[hash % DOMAIN_HUES.length];
}
