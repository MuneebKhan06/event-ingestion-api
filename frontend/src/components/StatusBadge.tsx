import "./StatusBadge.css";

export type Tone = "ok" | "warn" | "bad" | "idle";

export function StatusBadge({ tone, children }: { tone: Tone; children: string }) {
  return (
    <span className={`badge badge-${tone}`}>
      <span className="badge-dot" aria-hidden />
      {children}
    </span>
  );
}
