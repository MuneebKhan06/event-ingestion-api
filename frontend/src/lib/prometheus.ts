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

/** HELP text per metric name, so a screen can say what a counter means in the process's own words. */
export function parseHelp(text: string): Record<string, string> {
  const help: Record<string, string> = {};
  for (const line of text.split("\n")) {
    const match = /^# HELP (\S+) (.*)$/.exec(line.trim());
    if (match) help[match[1]] = match[2];
  }
  return help;
}

/** Stable key for one series: the name plus its sorted labels. */
export function seriesKey(sample: Sample): string {
  const labels = Object.entries(sample.labels)
    .sort(([a], [b]) => a.localeCompare(b))
    .map(([k, v]) => `${k}="${v}"`)
    .join(",");
  return labels ? `${sample.name}{${labels}}` : sample.name;
}

/**
 * Counters this project defines, without the client library's own series.
 *
 * prometheus_client adds a *_created gauge per counter (its start time) and
 * process_/python_ runtime metrics. Useful to Prometheus, noise on a screen
 * explaining the pipeline.
 */
export function isProjectCounter(name: string): boolean {
  return name.endsWith("_total") && !/^(process|python|gc)_/.test(name);
}
