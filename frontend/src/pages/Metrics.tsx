import { DLQBreakdown, SourceBreakdown } from "../components/Breakdowns";
import { Sparkline } from "../components/Sparkline";
import { StatTile } from "../components/StatTile";
import { StatusBadge } from "../components/StatusBadge";
import {
  latest,
  PROCESSES,
  SCRAPE_MS,
  sumIncrements,
  sumLatest,
  sumRatePerMinute,
  useMetrics,
} from "../hooks/useMetrics";
import { CATALOG } from "../lib/metricsCatalog";
import "./Metrics.css";

export function Metrics() {
  const metrics = useMetrics();

  return (
    <div className="grid">
      {PROCESSES.map(({ id, port }) => {
        const process = metrics[id];
        const spec = CATALOG[id];
        const last = process.history[process.history.length - 1];
        const keys = last ? Object.keys(last.values).sort() : [];
        return (
          <section key={id} className="card">
            <div className="card-head">
              <h2>{spec.title}</h2>
              <span className="muted">{spec.role}</span>
              <StatusBadge tone={process.up === null ? "idle" : process.up ? "ok" : "bad"}>
                {process.up === null ? "scraping" : process.up ? "up" : "unreachable"}
              </StatusBadge>
              <span className="muted request-meta">:{port}/metrics</span>
            </div>

            {process.up === false && (
              <p className="metrics-error">
                Could not scrape :{port}/metrics ({process.error}). Values below are the last ones seen.
              </p>
            )}

            <div className="stat-tiles">
              {spec.counters.map((counter) => (
                <StatTile
                  key={counter.name}
                  label={counter.label}
                  value={sumLatest(process, counter.name)}
                  ratePerMinute={sumRatePerMinute(process, counter.name)}
                  help={process.help[counter.name]}
                  bad={counter.bad}
                  trend={
                    <Sparkline
                      values={sumIncrements(process, counter.name)}
                      intervalSeconds={SCRAPE_MS / 1000}
                      label={counter.label}
                    />
                  }
                />
              ))}
            </div>

            <p className="muted metrics-note">{spec.note}</p>

            <details className="metrics-details">
              <summary>Raw series ({keys.length})</summary>
              <table className="metrics-raw">
                <tbody>
                  {keys.map((key) => (
                    <tr key={key}>
                      <td>
                        <code>{key}</code>
                      </td>
                      <td className="num">{latest(process, key)?.toLocaleString()}</td>
                    </tr>
                  ))}
                </tbody>
              </table>
            </details>
          </section>
        );
      })}
      <SourceBreakdown ingest={metrics.ingest} />
      <DLQBreakdown consumer={metrics.consumer} />
      <p className="muted metrics-foot">
        Totals count since each process last started; rates cover up to the last two minutes, from when this page started scraping. Scraped every{" "}
        {SCRAPE_MS / 1000} s while this tab is visible. Hover a tile for the counter's own description.
      </p>
    </div>
  );
}
