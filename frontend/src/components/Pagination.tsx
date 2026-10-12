import "./Pagination.css";

interface Props {
  total: number;
  limit: number;
  offset: number;
  onChange: (offset: number, limit: number) => void;
}

// The API caps limit at 200 so one request cannot ask for the whole table.
export const PAGE_SIZES = [25, 50, 100, 200];

/** Page numbers to show: always the ends, a window around the current page. */
function pageWindow(current: number, count: number): (number | "gap")[] {
  const pages = new Set([0, count - 1, current - 1, current, current + 1]);
  const sorted = [...pages].filter((p) => p >= 0 && p < count).sort((a, b) => a - b);
  const out: (number | "gap")[] = [];
  sorted.forEach((page, i) => {
    if (i > 0 && page - sorted[i - 1] > 1) out.push("gap");
    out.push(page);
  });
  return out;
}

export function Pagination({ total, limit, offset, onChange }: Props) {
  const count = Math.max(1, Math.ceil(total / limit));
  const current = Math.floor(offset / limit);
  const first = total === 0 ? 0 : offset + 1;
  const last = Math.min(offset + limit, total);

  return (
    <nav className="pagination" aria-label="Pages">
      <span className="pagination-range">
        {first.toLocaleString()} to {last.toLocaleString()} of {total.toLocaleString()}
      </span>

      <div className="pagination-pages">
        <button className="btn" disabled={current === 0} onClick={() => onChange(offset - limit, limit)}>
          Previous
        </button>
        {pageWindow(current, count).map((page, i) =>
          page === "gap" ? (
            <span key={`gap-${i}`} className="pagination-gap">
              …
            </span>
          ) : (
            <button
              key={page}
              className={`btn page-btn${page === current ? " current" : ""}`}
              aria-current={page === current ? "page" : undefined}
              onClick={() => onChange(page * limit, limit)}
            >
              {page + 1}
            </button>
          ),
        )}
        <button className="btn" disabled={current >= count - 1} onClick={() => onChange(offset + limit, limit)}>
          Next
        </button>
      </div>

      <label className="pagination-size">
        Per page
        <select value={limit} onChange={(e) => onChange(0, Number(e.target.value))}>
          {PAGE_SIZES.map((size) => (
            <option key={size} value={size}>
              {size}
            </option>
          ))}
        </select>
      </label>
    </nav>
  );
}
