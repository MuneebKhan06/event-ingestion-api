import { StatusBadge } from "../components/StatusBadge";
import { latest, PROCESSES, SCRAPE_MS, useMetrics } from "../hooks/useMetrics";
import "./Metrics.css";

export function Metrics() {
  const metrics = useMetrics();

  return (
    <div className="grid">
      <section className="metrics-processes">
        {PROCESSES.map(({ id, label, port }) => {
          const process = metrics[id];
          const last = process.history[process.history.length - 1];
          const keys = last ? Object.keys(last.values).sort() : [];
          return (
            <article key={id} className="card">
              <div className="card-head">
                <h2>{label}</h2>
                <StatusBadge tone={process.up === null ? "idle" : process.up ? "ok" : "bad"}>
                  {process.up === null ? "scraping" : process.up ? "up" : "unreachable"}
                </StatusBadge>
                <span className="muted request-meta">:{port}/metrics</span>
              </div>
              {process.up === false && <p className="metrics-error">{process.error}</p>}
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
            </article>
          );
        })}
      </section>
      <p className="muted metrics-foot">Scraped every {SCRAPE_MS / 1000} s while this tab is visible.</p>
    </div>
  );
}
