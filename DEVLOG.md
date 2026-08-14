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

## Pass 7 — Config resolved at use, not at import

Several modules ran `get_settings()` at import and bound the result to a
module-level `_settings`. The database engine was the worst case: it was
constructed at import, so the connection URL was frozen by whatever environment
existed the moment anything first imported `app.db.connection`. Nothing could
redirect the application afterwards.

The symptom had already shown up without being named: the integration tests in
pass 5 had to hardcode their own connection constants for the test stack, because
pointing the application's own settings at it would have had no effect.

Now `get_engine()` builds on first use, `async_session_factory()` stays callable
exactly as before so no call site changed, and the Kafka producer and consumer
read settings when they connect rather than when they load. `dispose_engine()`
also clears the cached engine, so a later call rebuilds instead of returning an
engine that has already been disposed.

**Scoped out deliberately:** `app/kafka/topics.py` still resolves its topic names
at import. Making it lazy would be theatre — its five consumers all use
`from app.kafka.topics import EVENTS_RAW`, and that form snapshots the value at
the importing module's import time no matter what the defining module does. It
would only buy something if every call site switched to attribute access, and
topic names are deploy-time static anyway. Better an honest untouched module than
a lazy-looking one that isn't.

Verified against the real stack, not just mocks: all 6 integration tests still
pass after the change, which is the part that actually exercises the reworked
Kafka and database wiring.

Suite: 34 → 37 unit tests (the 3 new ones pin the lazy behaviour so it can't
regress quietly).

---

## Pass 8 — A database outage could kill the consumer for good

**Gotcha (real bug):** the consume loop had no guard around per-message work, and
nothing in `docker-compose.yml` set a restart policy. Those two facts combined
badly.

`process_message` handles bad messages and exhausted retries by routing them to
the DLQ — but `DLQHandler.send` writes to Postgres *first* (deliberately: a
durable record before the publish). So when the database is the thing that's
down, the DLQ fallback fails too. That exception unwound the `while` loop, hit
the `finally`, stopped the consumer, and the container exited. With no restart
policy it never came back. A transient outage therefore took the consumer down
permanently, and the only symptom was a bare traceback with no indication of
which message was in flight.

Confirmed rather than assumed, by driving `process_message` with a failing
repository *and* a failing DLQ handler and watching the `RuntimeError` escape.

The fix keeps the failure loud but survivable: catch it, log with
topic/partition/offset so the message is findable, deliberately do **not** commit
the offset, and re-raise. Not committing is the important half — committing would
skip the message permanently, turning an outage into silent data loss. Continuing
to the next message would be just as bad, since a later commit would strand this
one. Exiting is only sound because the container now restarts, so
`restart: unless-stopped` on the consumer is load-bearing rather than boilerplate
(the API got it too).

Four tests pin this, including the negative guarantee that a failure never
commits. Checked they have teeth by mutating the `raise` into a fall-through and
confirming two of them fail.

Suite: 37 → 41 unit tests.

---

## Pass 9 — The integration test stops reaching into a private attribute

The DLQ integration test used to build an `EventProducer` and then assign
`wrapper._producer = producer`, grafting on an already-started client from
another fixture. That was a workaround for the import-time settings binding: the
broker address was fixed before the test could influence it, so there was no
supported way to point a real producer at the test stack.

Since settings resolve at `start()` (pass 7), a fixture can now set
`KAFKA_BOOTSTRAP_SERVERS`, clear the `lru_cache`, and construct a genuine
`EventProducer` that connects to the test broker on its own. The private poke is
gone, and the test exercises the same object graph production uses instead of a
hand-assembled stand-in — which is the entire point of an integration test.

Worth noting as the concrete payoff of pass 7: that refactor was justified on
the grounds that import-time configuration made things untestable, and this is
the case that was actually paying the price. Verified against the real stack —
all 6 integration tests pass.

---

## Pass 10 — The load test finally runs

The results table had said "not measured" since the README was written. The
stack now demonstrably starts, so there was no excuse left.

Smoke-tested the whole path first, because publishing throughput figures for a
pipeline that isn't actually persisting would be worse than publishing nothing:
five events accepted, five rows in Postgres, `status: processed`.

Then 60-second Locust runs at 10/50/100/200 users. Zero failed requests at every
level. Throughput plateaus around 165 req/s — 100 to 200 users moved it under 2%
while average latency tripled, which is what saturation looks like.

Two things worth recording:

The 50-user result is anomalous: worse latency than the 100-user run at a third
of the throughput. Left it in the README as measured, flagged as anomalous,
with a plausible-but-unverified mechanism rather than a confident story. One run
per level on a machine also running the stack is not enough evidence to explain
it, and quietly dropping the row would have been the dishonest fix.

The `/metrics` counters added earlier turned out to be useful immediately: the
API reported 24,193 accepted against 24,192 rows in Postgres. The single-event
gap is exactly the deliberate duplicate from the smoke test — both copies
accepted, since the pre-publish check is best-effort, then collapsed by
`ON CONFLICT DO NOTHING`. Decision 4 verified under real load rather than by
argument. The consumer trailed by ~4,700 events at peak and drained in ~30s,
which is the asynchrony Decision 1 buys, not loss — worth confirming explicitly
before reporting, because the two look identical in a single snapshot.

---

## Pass 11 — Real migrations, and a type mismatch they exposed

The schema came from SQL files mounted into `/docker-entrypoint-initdb.d`, which
only runs against an empty data directory. That could create a schema but never
change one: any future column or index would have meant wiping the volume.
Replaced with Alembic, applied by a one-shot `migrate` service that api and
consumer gate on via `service_completed_successfully`.

**Gotcha found while writing the initial revision:** the models and the actual
database disagreed. `Mapped[datetime]` without an explicit type maps to
`TIMESTAMP WITHOUT TIME ZONE`, but the SQL had created the columns as
`TIMESTAMPTZ`. Invisible while the schema came from hand-written SQL — the ORM
simply read whatever was there — but the moment anything generated DDL *from*
the models (autogenerate, `create_all`), it would have produced timezone-naive
columns silently disagreeing with production. Timezone bugs found later are
miserable to diagnose. `error_reason` had drifted too: `VARCHAR` in the model,
`TEXT` in the database.

Fixed the models to state `DateTime(timezone=True)` and `Text` explicitly, so
model and schema now agree, and set `compare_type=True` in `env.py` so this
class of drift is reported by future autogenerate runs instead of ignored.

The old `.sql` files are deleted rather than kept "for reference" — two
descriptions of one schema drift apart, and this pass is a demonstration of
exactly that.

The test stack no longer seeds SQL either; the integration tests run
`alembic upgrade head` themselves, so they exercise the same migration path
production does rather than a parallel one that could rot.

Verified against a genuinely empty volume: `down -v`, then up, migrate exited 0
having applied `-> 0001`, and `psql \d` showed both tables matching the old SQL
exactly — `timestamp with time zone`, the unique constraint on `event_id`, and
all four indexes including the two `DESC` ones. End to end afterwards: health
200, 5 accepted, 5 persisted, `created_at` serialising with a `Z`. Re-running
the migrate service on the already-migrated database exited 0 without
re-applying anything and left the rows intact.

---

## Pass 12 — Restart policies, and a test method that lied

Only `api` and `consumer` had `restart: unless-stopped`. Postgres, Kafka,
ZooKeeper and Kafka UI had none, which interacts badly with an earlier
decision: the consumer deliberately *exits* when the database is unreachable,
on the reasoning that the container comes back. If Postgres itself never
returns, that turns a recoverable outage into a permanent crash loop against a
corpse. Added policies to the long-running services.

`kafka-init` and `migrate` deliberately keep none, and the compose file now says
why: they are one-shot jobs whose contract is to finish and exit 0.
`unless-stopped` would restart them forever and break the
`service_completed_successfully` gate that api and consumer wait on. The
inconsistency is correct, so it is documented to stop someone tidying it away.

**Gotcha — the first verification was wrong, not the policy.** `docker kill
eia-postgres` left the container `exited` with `restarts=0`, which looked like a
failed restart policy. Inspecting the container showed
`RestartPolicy: unless-stopped` was present and correct. The flaw was the test:
Docker treats `kill` and `stop` as operator intent and suppresses the restart
policy until the container is started manually again. A test that cannot
distinguish "policy missing" from "policy deliberately suppressed" is worse than
no test — it would have been easy to "fix" a policy that was never broken.

Simulating a genuine crash needed the process to die without Docker being asked:
killing the host PID wasn't possible (Docker Desktop runs the daemon in a VM, so
container PIDs aren't in this namespace), so `pg_ctl stop -m immediate` from
inside made the postmaster exit on its own. Docker restarted it — `restarts=1` —
health returned to 200, the three pre-crash rows survived, and four further
events ingested cleanly.

Two things confirmed in passing: during the outage `/health` reported
`"database": "unreachable"` with 503, which is the failure-mode granularity from
pass 8 working in a real outage rather than a mocked one; and api/consumer both
rode it out with `restarts=0`, so nothing crash-looped. The consumer's
exit-on-database-failure path was *not* exercised — no message was in flight
during the outage — so that remains verified only by unit test.

---

## Pass 13 — Correlation IDs

Nothing tied a log line to a request. When `POST /events` logged a publish
failure and returned 503, there was no way to connect it to the caller who saw
it — and under load the log is one undifferentiated stream. Metrics tell you how
many requests are failing; this lets you follow one.

Middleware reads `X-Request-ID` or generates one, stashes it in a
`contextvars.ContextVar`, and echoes it on the response. A `logging.Filter`
reads that contextvar at emit time, so call sites don't have to accept and
forward an ID they have no other use for.

Three details that took thought rather than typing:

*Client-supplied IDs are validated, not trusted.* Honouring them is the whole
point — it is what makes a trace span services — but it means attacker-supplied
text heading straight for the logs. A newline would let a caller forge log
entries; an unbounded value would bloat every record. Values outside
`[A-Za-z0-9._:-]{1,64}` are replaced with a generated ID rather than sanitised,
so nothing is echoed back that the caller neither sent nor would recognise.

*The filter is attached to handlers, not to a logger.* Filters on a logger do
not apply to records propagated up from child loggers, so library records would
have arrived without `request_id` and the format string would have raised on
them — turning a logging improvement into a crash.

*The contextvar is reset in a `finally`.* Without it the value survives the
request and mislabels whatever that worker task picks up next, which is worse
than no ID at all: a wrong trace is more misleading than a missing one.

The first version of the log test only asserted the negative — that records
outside a request lacked the ID — and never proved a record *inside* one carried
it, which is the actual claim. Rewritten to drive the health probe's failure
path (real code that logs mid-request) and assert the ID is present.
Mutation-checked both the sanitising and the filter.

Verified live as well as by unit test: no header → generated UUID echoed;
`X-Request-ID: my-trace-001` → echoed unchanged; an ID containing whitespace →
replaced with a UUID; and `docker logs eia-api` showed
`[trace-me-abc] Published event ...`, the caller's own ID on an application log
line.

Scope is the API only, stated in the README rather than implied away: the
consumer is a separate process, and carrying the ID across would mean putting it
in the Kafka message.

---

## Pass 14 — Real time: the stack feeds itself

Up to here the pipeline only moved data when someone pushed a button. An
`ingest` service now polls public APIs (USGS earthquakes, GitHub public events,
Open-Meteo weather, all unauthenticated) and posts what it finds to the API over
HTTP, so it goes through exactly the same validation and idempotency path a real
client would rather than shortcutting into Kafka.

The design decision that carries the most weight is the smallest: each event's
`event_id` is derived with `uuid5` from the upstream record's own identifier.
Feeds repeat unchanged records between polls, so re-polling produces genuine
duplicates from real data. Decision 4 stops being an argument supported by tests
and becomes something the system demonstrates every thirty seconds. It also
means duplicates are the healthy steady state here, which is why the ingest
metrics count them separately rather than as failures.

Also added: `GET /events/stream` (SSE) so the real-time behaviour is observable;
Prometheus counters for ingest on `:9200`, giving one endpoint per process;
per source backoff; and bounded concurrent publishing.

**Gotcha (routing):** `/events/stream` returned 422. `events.router` owns
`GET /events/{event_id}` and was registered first, so "stream" was parsed as an
event_id and rejected as an invalid UUID. Routes match in registration order.
Fixed by registering the literal path first, with a test that resolves the path
against the real router, since the endpoint never completes a response body and
a normal request test hangs.

**Gotcha (rate limiting):** `fetch_events` called `raise_for_status`, so every
upstream failure landed in one `except` branch with the response headers already
gone. GitHub refuses an over-quota unauthenticated caller with `403` and
`X-RateLimit-Remaining: 0`, not `429`, so both are now recognised before the
raise, and `Retry-After` in seconds is honoured over the local guess. A plain
`403` is deliberately not treated as rate limiting: being forbidden for other
reasons should not be quietly waited out. Only the integer seconds form of
`Retry-After` is parsed; an HTTP date falls through to exponential backoff,
which is a safe default rather than a wrong parse.

Two lessons about verification, both worth more than the code they produced.

`httpx` was only in `requirements-dev.txt`, while `Dockerfile.ingest` installs
from `requirements.txt`. The image built cleanly and would have crashed on
import the moment it ran. A successful build says nothing about a working
service, and running the container is what caught it.

The `return_exceptions=True` on the publish `gather` was tested by a case that
did not exercise it. `publish_one` catches `Exception` around the request
itself, so `gather` never sees one on that path, and the mutation check proved
it: deleting `return_exceptions` changed nothing and all tests still passed. The
test was passing for the wrong reason and would have let the safeguard be
removed silently. Rewritten to drive a failure from outside the inner handler,
after which the same mutation fails as it should. Without the mutation check
this would have been reported as verified when it was not.

---

## Known gaps

- **Load test numbers are single runs on a contended machine.** They are real
  and honestly reported, but Locust shares CPUs with the stack it measures and
  each level ran once. The anomalous 50-user result is called out in the README
  rather than smoothed away; explaining it properly needs repeated runs.
- **The duplicate-check `SELECT` still couples API latency to database load by
  default.** It is now opt-out (`ENABLE_DUPLICATE_PRECHECK`) rather than
  mandatory, so a throughput-sensitive deployment can remove the coupling — but
  the default is still on, so the out-of-the-box behaviour is unchanged and the
  anomaly seen in the load test would still reproduce. Measuring both modes
  under load would settle how much it actually costs; that hasn't been done.
- **No schema registry.** Discussed in the README, event contracts are enforced
  only at the API layer today.
- **The integration tests never exercise the ingest poller.**
  `docker-compose.test.yml` has no `ingest` service, so the poller is covered by
  unit tests and by manual runs against the full stack, but nothing automated
  checks it end to end the way the API and consumer are checked.
- **The SSE stream has no cap on concurrent clients.** Each connection polls
  Postgres once a second, so N open dashboards mean N pollers. Fine for one or
  two, not something to leave unbounded on a shared deployment.
- **Ingest publishes are bounded but not adaptive.** The concurrency limit is a
  fixed number rather than a response to observed API latency, so a slow API is
  answered with the same pressure as a fast one.
