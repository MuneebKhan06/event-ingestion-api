import { LiveFeed } from "../components/LiveFeed";

// Facts from app/api/routes/stream.py, stated where a viewer watching the
// feed would wonder about them.
const NOTES = [
  {
    title: "Durable, not optimistic",
    text: "The feed polls rows the consumer already committed to Postgres. An event shows up here only once it is safely stored.",
  },
  {
    title: "Short lived sessions",
    text: "Each poll opens and returns its own database session, so an idle browser tab never holds a pooled connection.",
  },
  {
    title: "Capped and jittered",
    text: "At most 20 streams are served; the next is refused with a 503. Poll intervals are jittered so clients never query in sync.",
  },
];

export function Stream() {
  return (
    <div className="grid">
      <LiveFeed />
      <section className="tiles">
        {NOTES.map((note) => (
          <article key={note.title} className="card">
            <h2>{note.title}</h2>
            <p className="muted">{note.text}</p>
          </article>
        ))}
      </section>
    </div>
  );
}
