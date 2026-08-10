# Build Log

Chronological notes on how this project was built, and the problems worth
remembering. Kept factual — the intent is that a future reader (including me)
can see why things are the way they are.

---

## Pass 1 — Foundations

Config, schemas, and the shared Kafka topic constants, then the persistence
layer and the Kafka wrappers.

- `app/config.py` uses pydantic-settings, so every value is overridable by
  environment variable with sane defaults.
- Schema design followed the plan: `JSONB` payload, unique `event_id` as the
  idempotency key, plus a separate `dlq_events` table.
- `EventRepository.insert_event` uses `ON CONFLICT DO NOTHING` and reports
  whether a row was actually inserted, which is what makes duplicate handling
  atomic rather than a read-then-write race.
- The Kafka consumer wrapper disables auto-commit. Offsets advance only after a
  message has been durably handled — committing on read would silently drop
  messages whenever the consumer died mid-processing.

**Gotcha:** `.env.example` had been scaffolded as an empty *directory*, not a
file, so writing it failed with `EISDIR`. Removed the directory first. Worth
checking the rest of a scaffold when one entry turns out to be wrong.

**Refactor:** `DATABASE_URL` was initially defined alongside the individual
`POSTGRES_*` settings, which meant two sources of truth that had to be edited in
lockstep. It is now derived from those fields, and only set explicitly to
override.

---

## Pass 2 — Application and consumer

The FastAPI app, then the standalone consumer service, then tests.

- `POST /events` does a best-effort duplicate lookup so an obvious repeat gets a
  fast `409`. This is explicitly *not* the correctness guarantee — two concurrent
  requests can both pass it. The unique constraint in the consumer is the real
  one, and the code says so, because a future reader could easily mistake the
  check for the actual protection.
- The consumer separates transient from permanent failure: a DB blip is retried
  with exponential backoff, while a malformed payload or bad `event_id` goes
  straight to the DLQ. Retrying a permanently invalid message just delays the
  inevitable and burns throughput.
- 14 tests, all mock-based, so the suite runs without Kafka or PostgreSQL.

**Gotcha:** the shell's active Python turned out to be an unrelated project's
virtualenv (`oracle_to_clickhouse_insertion`), which is where the first few
`pip install`s landed. Created a dedicated `.venv` for this project and re-ran
everything there. All project commands should use `.venv/bin/python3`.

**Follow-ups in the same pass:**
- The async engine was created at import and never disposed, leaking pooled
  connections on exit. `dispose_engine()` is now wired into both the FastAPI
  lifespan and the consumer's shutdown path.
- Added a ruff config. Two of its five findings were deliberately ignored rather
  than "fixed": `B008` flags FastAPI's `Depends()`-in-defaults, which is the
  intended idiom, and `UP017` wants `datetime.UTC`, which would drop Python 3.10
  support. Silencing a rule with a reason beats contorting correct code.
- The consumer had no signal handling, so `SIGTERM` killed it mid-message. It now
  races the message fetch against a shutdown event, so an idle consumer reacts
  immediately instead of blocking until the next message arrives, and any
  in-flight message still finishes and commits its offset.

---

## Pass 3 — Packaging and tooling

Dockerfiles, the compose stacks, and the load-testing tools.

- Both images install dependencies in a layer separate from the source copy, and
  run as a non-root user. The consumer uses exec-form `CMD` so it is PID 1 and
  actually receives `SIGTERM` — otherwise the graceful shutdown built in pass 2
  would never fire.
- Kafka advertises two listeners (`kafka:29092` internally, `localhost:9092` from
  the host) because one listener cannot serve both: the advertised address has to
  be resolvable by whichever client is connecting.
- Topic auto-creation is disabled and a `kafka-init` job creates the topics with
  3 partitions. Auto-created topics get 1 partition, which would quietly cap
  consumer parallelism at one — the kind of default that looks fine until you try
  to scale out.
- The Locust lookup task counts `404` as success on purpose. Ingestion is
  asynchronous, so a just-published event may not be in Postgres yet; treating
  that as an error would bury real failures under expected noise.

**Gotcha:** the `kafka-init` topic-creation command used a folded YAML block with
*indented* continuation lines. YAML only folds lines that share an indent, so the
newlines survived and the rendered command would have run
`kafka-topics --bootstrap-server kafka:29092 --create --if-not-exists` as one
complete command and `--topic events.raw ...` as another — failing at `up` time.
`docker compose config` reported the file as perfectly valid, because it *was*
valid YAML; the bug only showed up by rendering the config and reading the actual
command string back. Validation that only checks syntax will not catch this class
of error.

**Follow-ups in the same pass:**
- The Postgres settings were written out three times — once for Postgres to
  initialise the database, once each for the API and consumer to connect to it —
  with nothing keeping them in sync. They now come from shared YAML anchors. The
  rendered `docker compose config` was diffed before and after to confirm the
  refactor changed nothing: identical services, identical resolved environment.
- The API healthcheck existed only on the compose service, so the image reported
  no health at all under plain `docker run`. Moved into `Dockerfile.api`, which
  compose inherits, leaving one definition instead of two. Verified end to end
  against a real build: a container serving `/health` reports `healthy`, and the
  probe exits non-zero when nothing is listening.

---

## Known gaps

- **Load test results are not measured.** The Locust scenarios parse and are
  verified, but no run has been executed against a live stack, so the results
  table in the README is intentionally empty rather than populated with invented
  numbers.
- **No integration tests against real Kafka/Postgres.** The suite is entirely
  mock-based. `docker-compose.test.yml` exists to support real integration tests,
  but none are written yet.
- **No schema registry.** Discussed in the README — event contracts are enforced
  only at the API layer today.
