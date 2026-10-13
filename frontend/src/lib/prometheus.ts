export interface Sample {
  name: string;
  labels: Record<string, string>;
  value: number;
}

const LINE = /^([a-zA-Z_:][a-zA-Z0-9_:]*)(?:\{([^}]*)\})?\s+(\S+)/;
const LABEL = /([a-zA-Z_][a-zA-Z0-9_]*)="((?:[^"\\]|\\.)*)"/g;

/**
 * Parses Prometheus text exposition into samples.
 *
 * Enough of the format for what these processes emit: counters and gauges,
 * with or without labels. Comment lines (# HELP, # TYPE) are skipped.
 */
export function parsePrometheus(text: string): Sample[] {
  const samples: Sample[] = [];
  for (const raw of text.split("\n")) {
    const line = raw.trim();
    if (!line || line.startsWith("#")) continue;
    const match = LINE.exec(line);
    if (!match) continue;
    const labels: Record<string, string> = {};
    for (const [, key, value] of (match[2] ?? "").matchAll(LABEL)) labels[key] = value;
    samples.push({ name: match[1], labels, value: Number(match[3]) });
  }
  return samples;
}

/** Sum of every series with this name, across all label values. */
export function total(samples: Sample[], name: string): number {
  return samples.filter((s) => s.name === name).reduce((sum, s) => sum + s.value, 0);
}
