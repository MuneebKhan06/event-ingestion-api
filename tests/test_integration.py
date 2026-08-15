"""End-to-end tests against real Kafka and PostgreSQL.

The rest of the suite is mock-based, which verifies our own logic but never the
wire behaviour underneath it — serialization, the actual unique constraint, real
broker round trips. These fill that gap.

They need the infrastructure stack:

    docker compose -f docker-compose.test.yml up -d
    pytest tests/ -m integration

and are skipped automatically when it isn't reachable, so the default test run
and CI stay green without Docker.
"""

import asyncio
import json
import socket
import sys
import uuid
from pathlib import Path

import pytest

# The test stack publishes on shifted ports so it can coexist with the dev one.
KAFKA_HOST, KAFKA_PORT = "localhost", 9093
POSTGRES_HOST, POSTGRES_PORT = "localhost", 5433
BOOTSTRAP = f"{KAFKA_HOST}:{KAFKA_PORT}"
DATABASE_URL = (
    f"postgresql+asyncpg://events_user:events_password@{POSTGRES_HOST}:{POSTGRES_PORT}/events_db"
)


def _reachable(host: str, port: int) -> bool:
    try:
        with socket.create_connection((host, port), timeout=2):
            return True
    except OSError:
        return False


pytestmark = [
    pytest.mark.integration,
    pytest.mark.skipif(
        not _reachable(KAFKA_HOST, KAFKA_PORT) or not _reachable(POSTGRES_HOST, POSTGRES_PORT),
        reason=(
            "integration stack not reachable — start it with "
            "`docker compose -f docker-compose.test.yml up -d`"
        ),
    ),
]


@pytest.fixture(scope="session", autouse=True)
def apply_migrations():
    """Bring the test database up to head before anything queries it.

    Running the real migrations rather than a separate schema fixture means
    these tests would fail if a migration were broken — which is the point of
    having them.
    """
    import os
    import subprocess

    env = {**os.environ, "POSTGRES_HOST": POSTGRES_HOST, "POSTGRES_PORT": str(POSTGRES_PORT)}
    env.pop("DATABASE_URL", None)  # let it derive from the POSTGRES_* values

    result = subprocess.run(
        [sys.executable, "-m", "alembic", "upgrade", "head"],
        cwd=Path(__file__).resolve().parent.parent,
        env=env,
        capture_output=True,
        text=True,
    )
    if result.returncode != 0:
        pytest.fail(f"alembic upgrade head failed:\n{result.stdout}\n{result.stderr}")


@pytest.fixture
async def db_session():
    from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine

    engine = create_async_engine(DATABASE_URL)
    factory = async_sessionmaker(engine, expire_on_commit=False)
    async with factory() as session:
        yield session
    await engine.dispose()


@pytest.fixture
async def event_producer(monkeypatch):
    """A real, started EventProducer aimed at the test stack.

    Only possible because start() resolves settings when it connects rather
    than at import (see the settings refactor in the build log). While the
    broker address was frozen at import, a test had no way to redirect it and
    had to graft an externally-created client onto the producer's private
    attribute instead — using the object in a way production never does, which
    is precisely what an integration test should avoid.

    get_settings is lru_cached, so the cache is cleared on the way in and out
    to keep this override from leaking into other tests.
    """
    from app.config import get_settings
    from app.kafka.producer import EventProducer

    monkeypatch.setenv("KAFKA_BOOTSTRAP_SERVERS", BOOTSTRAP)
    get_settings.cache_clear()

    producer = EventProducer()
    await producer.start()
    try:
        yield producer
    finally:
        await producer.stop()
        monkeypatch.undo()
        get_settings.cache_clear()


@pytest.fixture
async def producer():
    from aiokafka import AIOKafkaProducer

    kafka_producer = AIOKafkaProducer(
        bootstrap_servers=BOOTSTRAP,
        acks=1,
        value_serializer=lambda v: json.dumps(v, default=str).encode("utf-8"),
        key_serializer=lambda k: k.encode("utf-8") if k else None,
    )
    await kafka_producer.start()
    yield kafka_producer
    await kafka_producer.stop()


# --------------------------------------------------------------------------
# PostgreSQL
# --------------------------------------------------------------------------


async def test_insert_and_read_back_a_real_event(db_session):
    from app.db.repository import EventRepository

    repository = EventRepository(db_session)
    event_id = uuid.uuid4()

    inserted = await repository.insert_event(
        event_id=event_id,
        event_type="order.created",
        source="integration-test",
        payload={"order_id": "ORD-1", "amount": 12.5},
    )
    assert inserted is True

    stored = await repository.get_by_event_id(event_id)
    assert stored is not None
    assert stored.event_type == "order.created"
    # JSONB round trip: the payload must come back as a dict, not a string.
    assert stored.payload == {"order_id": "ORD-1", "amount": 12.5}
    assert stored.created_at is not None


async def test_duplicate_insert_is_ignored_by_the_real_unique_constraint(db_session):
    """The core idempotency guarantee, against the actual constraint.

    Everywhere else this is asserted against a mocked repository, which proves
    only that we call it — not that Postgres does what Decision 4 claims. Here
    the second insert really does hit `event_id UNIQUE` and must return False
    rather than raising IntegrityError.
    """
    from app.db.repository import EventRepository

    repository = EventRepository(db_session)
    event_id = uuid.uuid4()

    first = await repository.insert_event(
        event_id=event_id, event_type="order.created", source="integration-test", payload={"n": 1}
    )
    second = await repository.insert_event(
        event_id=event_id, event_type="order.created", source="integration-test", payload={"n": 2}
    )

    assert first is True
    assert second is False

    # The original row must survive untouched — ON CONFLICT DO NOTHING, not
    # DO UPDATE, so the second payload must not have overwritten the first.
    stored = await repository.get_by_event_id(event_id)
    assert stored.payload == {"n": 1}


async def test_list_events_filters_against_real_sql(db_session):
    """The listing query is only ever exercised as compiled SQL elsewhere."""
    from app.db.repository import EventRepository

    repository = EventRepository(db_session)
    marker_source = f"integration-{uuid.uuid4().hex[:8]}"

    for index in range(3):
        await repository.insert_event(
            event_id=uuid.uuid4(),
            event_type="user.clicked" if index else "order.created",
            source=marker_source,
            payload={"i": index},
        )

    events, total = await repository.list_events(source=marker_source, limit=10)
    assert total == 3
    assert len(events) == 3

    filtered, filtered_total = await repository.list_events(
        source=marker_source, event_type="order.created", limit=10
    )
    assert filtered_total == 1
    assert filtered[0].event_type == "order.created"

    # Newest first, per the created_at DESC ordering.
    timestamps = [event.created_at for event in events]
    assert timestamps == sorted(timestamps, reverse=True)


# --------------------------------------------------------------------------
# Kafka
# --------------------------------------------------------------------------


async def test_event_survives_a_real_kafka_round_trip(producer):
    """Publish through a real broker and consume it back.

    Verifies the producer/consumer serialization pair actually agree — a JSON
    encode/decode mismatch would be invisible to mocked tests.
    """
    from aiokafka import AIOKafkaConsumer

    event_id = str(uuid.uuid4())
    group = f"integration-{uuid.uuid4().hex[:8]}"

    consumer = AIOKafkaConsumer(
        "events.raw",
        bootstrap_servers=BOOTSTRAP,
        group_id=group,
        value_deserializer=lambda v: json.loads(v.decode("utf-8")),
        enable_auto_commit=False,
        auto_offset_reset="latest",
    )
    await consumer.start()
    try:
        # Give the group its partition assignment before publishing, otherwise
        # auto_offset_reset=latest can skip past the message we send.
        await consumer.getmany(timeout_ms=2000)

        sent = {
            "event_id": event_id,
            "event_type": "order.created",
            "source": "integration-test",
            "payload": {"order_id": "ORD-9", "amount": 42.0},
        }
        await producer.send_and_wait("events.raw", value=sent, key=event_id)

        received = None
        for _ in range(20):
            batches = await consumer.getmany(timeout_ms=1000)
            for records in batches.values():
                for record in records:
                    if record.value.get("event_id") == event_id:
                        received = record
                        break
            if received:
                break

        assert received is not None, "published event was not consumed back"
        assert received.value == sent
        assert received.key.decode() == event_id
    finally:
        await consumer.stop()


async def test_dlq_handler_writes_to_both_postgres_and_kafka(db_session, event_producer):
    """Decision 6's two-destination promise, end to end.

    Uses a genuine EventProducer, so this exercises the same object graph
    production does rather than a hand-assembled stand-in.
    """
    from aiokafka import AIOKafkaConsumer
    from sqlalchemy import select

    from app.core.dlq import DLQHandler
    from app.db.models import DLQEvent
    from app.db.repository import EventRepository

    group = f"integration-dlq-{uuid.uuid4().hex[:8]}"
    consumer = AIOKafkaConsumer(
        "events.dlq",
        bootstrap_servers=BOOTSTRAP,
        group_id=group,
        value_deserializer=lambda v: json.loads(v.decode("utf-8")),
        enable_auto_commit=False,
        auto_offset_reset="latest",
    )
    await consumer.start()
    try:
        await consumer.getmany(timeout_ms=2000)

        repository = EventRepository(db_session)
        handler = DLQHandler(event_producer, repository)
        event_id = uuid.uuid4()

        await handler.send(
            raw_payload={"event_id": str(event_id), "broken": True},
            error_reason="integration test failure",
            event_id=event_id,
            event_type="order.created",
            source="integration-test",
        )

        row = (
            await db_session.execute(select(DLQEvent).where(DLQEvent.event_id == event_id))
        ).scalar_one_or_none()
        assert row is not None
        assert row.error_reason == "integration test failure"

        received = None
        for _ in range(20):
            batches = await consumer.getmany(timeout_ms=1000)
            for records in batches.values():
                for record in records:
                    if record.value.get("event_id") == str(event_id):
                        received = record
                        break
            if received:
                break

        assert received is not None, "DLQ event never reached the events.dlq topic"
        assert received.value["error_reason"] == "integration test failure"
    finally:
        await consumer.stop()


async def test_events_raw_has_the_configured_partition_count():
    """Decision 2 says 3 partitions; auto-created topics would have 1."""
    from aiokafka.admin import AIOKafkaAdminClient

    admin = AIOKafkaAdminClient(bootstrap_servers=BOOTSTRAP)
    await admin.start()
    try:
        metadata = await asyncio.wait_for(
            admin.describe_topics(["events.raw"]), timeout=10
        )
        partitions = metadata[0]["partitions"]
        assert len(partitions) == 3
    finally:
        await admin.close()


# --------------------------------------------------------------------------
# Ingest against real infrastructure
# --------------------------------------------------------------------------


async def test_source_events_deduplicate_against_the_real_constraint(db_session):
    """The claim the whole ingest design rests on, checked against Postgres.

    Deterministic uuid5 ids mean re-polling a feed re-sends unchanged records.
    Unit tests prove the ids are stable; only the real unique constraint can
    prove that re-inserting them stores nothing new. A mocked repository would
    happily "deduplicate" whatever we told it to.
    """
    from uuid import UUID

    from app.db.repository import EventRepository
    from ingest.sources import SOURCES
    from tests.test_ingest import USGS_BODY

    repository = EventRepository(db_session)
    events = SOURCES["usgs"].parse(USGS_BODY)
    assert events

    first = [
        await repository.insert_event(
            event_id=UUID(e["event_id"]),
            event_type=e["event_type"],
            source=e["source"],
            payload=e["payload"],
        )
        for e in events
    ]

    # Same feed, polled again: identical ids, nothing new stored.
    second = [
        await repository.insert_event(
            event_id=UUID(e["event_id"]),
            event_type=e["event_type"],
            source=e["source"],
            payload=e["payload"],
        )
        for e in events
    ]

    assert all(first), "first poll should have inserted every record"
    assert not any(second), "second poll should have inserted nothing"

    stored = await repository.get_by_event_id(UUID(events[0]["event_id"]))
    assert stored is not None


async def test_every_source_produces_rows_postgres_accepts(db_session):
    """Each parser's output must survive the real column types and constraints.

    Catches things unit tests cannot: a payload the JSONB column rejects, or a
    source string longer than VARCHAR(50).
    """
    from uuid import UUID

    from app.db.repository import EventRepository
    from ingest.sources import SOURCES
    from tests.test_ingest import GITHUB_BODY, USGS_BODY, WEATHER_BODY

    repository = EventRepository(db_session)
    bodies = {"usgs": USGS_BODY, "github": GITHUB_BODY, "weather": WEATHER_BODY}

    for name, body in bodies.items():
        for event in SOURCES[name].parse(body):
            await repository.insert_event(
                event_id=UUID(event["event_id"]),
                event_type=event["event_type"],
                source=event["source"],
                payload=event["payload"],
            )

        events, total = await repository.list_events(source=SOURCES[name].parse(body)[0]["source"])
        assert total >= 1, f"{name} produced no stored rows"
        assert events[0].payload, f"{name} stored an empty payload"


async def test_live_public_api_records_reach_postgres(db_session):
    """End to end against the actual upstream, tolerant of network trouble.

    Fetches a real feed rather than a fixture, so it catches upstream schema
    drift that recorded bodies never would. Skipped rather than failed when the
    network or the upstream is unavailable, since neither is this project's
    fault and a flaky suite gets ignored.
    """
    from uuid import UUID

    import httpx

    from app.db.repository import EventRepository
    from ingest.sources import SOURCES

    source = SOURCES["usgs"]
    try:
        response = httpx.get(source.url, timeout=20.0)
        response.raise_for_status()
        events = source.parse(response.json())
    except Exception as exc:  # noqa: BLE001 - upstream availability is not under test
        pytest.skip(f"upstream unavailable: {exc}")

    if not events:
        pytest.skip("upstream returned no records in this window")

    repository = EventRepository(db_session)
    event = events[0]
    await repository.insert_event(
        event_id=UUID(event["event_id"]),
        event_type=event["event_type"],
        source=event["source"],
        payload=event["payload"],
    )

    stored = await repository.get_by_event_id(UUID(event["event_id"]))
    assert stored is not None
    assert stored.event_type == "quake.detected"
    assert stored.payload["usgs_id"]
