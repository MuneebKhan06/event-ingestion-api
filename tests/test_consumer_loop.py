"""The consume loop's offset-commit guarantees.

The important property is negative: when a message could not be durably handled,
its offset must NOT be committed. Committing on failure would strand the message
permanently, which is silent data loss rather than a visible outage.
"""

from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest

import consumer.main as consumer_main


class _FakeSessionContext:
    async def __aenter__(self):
        return None

    async def __aexit__(self, *_exc_info):
        return False


@pytest.fixture
def wiring(monkeypatch):
    """Drive run() with one message, then a shutdown signal."""
    consumer = AsyncMock()
    consumer.commit = AsyncMock()
    producer = AsyncMock()

    message = SimpleNamespace(topic="events.raw", partition=0, offset=42, value={})
    remaining = [message, None]  # None ends the loop

    async def fake_wait(_consumer, _shutdown):
        return remaining.pop(0)

    monkeypatch.setattr(consumer_main, "EventConsumer", lambda *a, **k: consumer)
    monkeypatch.setattr(consumer_main, "EventProducer", lambda: producer)
    monkeypatch.setattr(consumer_main, "async_session_factory", _FakeSessionContext)
    monkeypatch.setattr(consumer_main, "dispose_engine", AsyncMock())
    monkeypatch.setattr(consumer_main, "EventRepository", lambda _s: object())
    monkeypatch.setattr(consumer_main, "DLQHandler", lambda *_a: object())
    monkeypatch.setattr(consumer_main, "_wait_for_message_or_shutdown", fake_wait)

    return SimpleNamespace(consumer=consumer, producer=producer, message=message)


async def test_offset_is_committed_after_a_message_is_handled(wiring, monkeypatch):
    monkeypatch.setattr(consumer_main, "process_message", AsyncMock())

    await consumer_main.run()

    wiring.consumer.commit.assert_awaited_once()


async def test_offset_is_not_committed_when_handling_fails(wiring, monkeypatch):
    """A failure here means even the DLQ write failed — the DB is unreachable.

    Committing would skip the message for good; the loop must surface the
    failure instead so the process restarts and Kafka redelivers it.
    """
    monkeypatch.setattr(
        consumer_main, "process_message", AsyncMock(side_effect=RuntimeError("db down"))
    )

    with pytest.raises(RuntimeError, match="db down"):
        await consumer_main.run()

    wiring.consumer.commit.assert_not_awaited()


async def test_failure_is_logged_with_enough_context_to_find_the_message(
    wiring, monkeypatch, caplog
):
    monkeypatch.setattr(
        consumer_main, "process_message", AsyncMock(side_effect=RuntimeError("db down"))
    )

    with caplog.at_level("ERROR"), pytest.raises(RuntimeError):
        await consumer_main.run()

    logged = caplog.text
    assert "events.raw" in logged
    assert "42" in logged  # the offset — otherwise the operator can't locate it
    assert "offset not committed" in logged


async def test_consumer_still_runs_when_the_metrics_port_is_taken(wiring, monkeypatch):
    """Metrics are auxiliary — losing them must not stop the consumer.

    A bind failure taking down the service would trade availability for
    observability, which is the wrong way round.
    """
    monkeypatch.setattr(consumer_main, "process_message", AsyncMock())
    monkeypatch.setattr(
        consumer_main,
        "start_http_server",
        lambda _port: (_ for _ in ()).throw(OSError("Address already in use")),
    )

    await consumer_main.run()

    # The message was still handled and its offset committed.
    wiring.consumer.commit.assert_awaited_once()


async def test_shutdown_still_stops_cleanly(wiring, monkeypatch):
    monkeypatch.setattr(consumer_main, "process_message", AsyncMock())

    await consumer_main.run()

    # Teardown must run on the normal path too, not just on failure.
    wiring.consumer.stop.assert_awaited_once()
    wiring.producer.stop.assert_awaited_once()
