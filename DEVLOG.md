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
- Tests are all mock-based, so the suite runs without Kafka or PostgreSQL.
  (Started at 14 here; later passes took it to 23.)

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

## Pass 4 — Health semantics and coverage gaps

**Gotcha (real bug):** `/health` reported `"status": "degraded"` in its body but
still answered `200`. Nothing that consumes the endpoint reads the body — the
image's `HEALTHCHECK` runs `urlopen`, which only fails on a non-2xx, and load
balancers and orchestrator probes key off the status code too. So a container
with Kafka *and* Postgres down reported itself perfectly healthy to Docker. The
two halves (health endpoint, container healthcheck) were each reasonable on their
own and only became a bug where they met. It now returns `503` when degraded.

The endpoint had no tests at all, which is why the gap survived that long; it now
has three (healthy, fully degraded, Kafka-only-down). Confirmed they actually
catch the bug by reverting the fix and watching them fail with `200 != 503` — a
test that passes both before and after a fix proves nothing.

Also filled the other untested spot: `app/core/dlq.py` and
`app/core/idempotency.py` had no direct tests, only indirect exercise through the
consumer. Six tests now cover DLQ writes going to both Postgres and Kafka, the
DLQ message keeping the payload and reason intact, the generated-key fallback for
unparseable payloads (and that the fake key is not recorded as a real
`event_id`), the Postgres-before-Kafka ordering, and both branches of the
duplicate check. Verified with a deliberate mutation: deleting the Kafka publish
makes four of them fail.

Suite: 14 → 23 tests.

---

## Pass 5 — Integration tests, and the bug they found

Wrote real end-to-end tests against Kafka and Postgres from
`docker-compose.test.yml`, closing the "no integration tests" gap.

**Gotcha (real bug, and a serious one):** bringing the stack up for the first
time revealed that it could not start at all. The ZooKeeper healthcheck used the
`ruok` four-letter command, but ZooKeeper 3.5+ refuses those unless explicitly
whitelisted:

```
ruok is not executed because it is not in the whitelist.
```

So ZooKeeper never became healthy, and everything gated on
`zookeeper: condition: service_healthy` — Kafka, and in the dev stack the API and
consumer too — waited forever. The documented `docker-compose up -d` in Getting
Started simply hung. Both compose files were affected.

The first fix attempt was wrong in an instructive way: setting
`ZOOKEEPER_4LW_COMMANDS_WHITELIST` looks right and the variable *was* present in
the container, but it never reached `zookeeper.properties` — the Confluent
image's env-to-property translation drops it, apparently because the property
name starts with a digit. Checking the generated properties file rather than
trusting the env var showed this. Passing
`KAFKA_OPTS=-Dzookeeper.4lw.commands.whitelist=ruok` works, and the whole stack
then came up healthy.

Two things stand out. First, `docker compose config` validated these files
happily through both this bug and the earlier folded-YAML one — schema validation
says nothing about whether a stack actually runs. Second, this was only ever going
to be found by running it, which is exactly the category of defect an all-mock
suite cannot reach.

With the stack up, the tests confirmed the things mocks can only assume: the
JSONB payload round trip, producer and consumer genuinely agreeing on
serialization across a real broker, `events.raw` really having 3 partitions
(Decision 2), the DLQ writing to both destinations (Decision 6), and the
`ON CONFLICT DO NOTHING` idempotency guarantee hitting the real unique
constraint — returning False on the duplicate and leaving the original row
untouched, rather than raising. That is the system's central correctness claim
and it had only ever been asserted against a mock.

Suite: 34 → 40 tests (6 integration, skipped when the stack is absent).

---

## Pass 6 — Ruff's target version

Ruff was configured with `target-version = "py311"` while the project supported
3.10 and, since pass 4, actively tested it in CI. That is not a cosmetic
mismatch: `target-version` tells ruff which idioms are safe to *recommend*, so it
was suggesting `datetime.UTC` — 3.11-only — in code that has to run on 3.10. The
`UP017` ignore added back in pass 2 was suppressing the symptom rather than
correcting the cause.

The sharp edge is that those suggestions were marked auto-fixable. Anyone running
`ruff check --fix` would have had the linter rewrite working code into a form
that fails the 3.10 CI job, while reporting success.

Setting `target-version = "py310"` fixes it at the root, and makes the `UP017`
ignore dead config — confirmed by removing it and re-running: with py311 and no
ignore ruff reports 4 UP017 errors, with py310 and no ignore it reports none.
`ruff check --fix` is now verifiably a no-op. (The `B008` ignore stays: FastAPI's
`Depends()` in argument defaults is the intended idiom, unrelated to versions.)

---

## Known gaps

- **Load test results are not measured.** The Locust scenarios parse and are
  verified, but no run has been executed against a live stack, so the results
  table in the README is intentionally empty rather than populated with invented
  numbers.
- **Load test still not run against the full stack.** The infrastructure now
  demonstrably comes up, so this is finally unblocked — it just hasn't been done.
- **No schema registry.** Discussed in the README — event contracts are enforced
  only at the API layer today.
