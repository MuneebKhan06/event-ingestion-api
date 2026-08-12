import sys
import uuid
from pathlib import Path
from unittest.mock import create_autospec

from app.db.repository import EventRepository

sys.path.insert(0, str(Path(__file__).resolve().parent.parent / "scripts"))

from replay_dlq import classify, parse_args, replay  # noqa: E402


def _valid_payload(event_type: str = "order.created") -> dict:
    return {
        "event_id": str(uuid.uuid4()),
        "event_type": event_type,
        "source": "order-service",
        "payload": {"order_id": "ORD-1"},
    }


# --------------------------------------------------------------------------
# classify — which stored payloads are worth republishing
# --------------------------------------------------------------------------


def test_classify_accepts_a_well_formed_payload():
    event, reason = classify(_valid_payload())

    assert reason is None
    assert event is not None
    assert event.event_type == "order.created"


def test_classify_rejects_payload_missing_fields():
    """These are why the event failed originally — replaying just re-fails."""
    event, reason = classify({"event_type": "order.created"})

    assert event is None
    assert reason is not None


def test_classify_rejects_bad_event_id():
    event, reason = classify(_valid_payload() | {"event_id": "not-a-uuid"})

    assert event is None
    assert "event_id" in reason


def test_classify_rejects_non_object_payload():
    event, reason = classify("just a string")

    assert event is None
    assert "not an object" in reason


# --------------------------------------------------------------------------
# replay — dry run must not publish
# --------------------------------------------------------------------------


class _FakeDLQEvent:
    def __init__(self, dlq_id: int, raw_payload):
        self.id = dlq_id
        self.raw_payload = raw_payload


async def test_dry_run_publishes_nothing(monkeypatch, capsys):
    """A replay mutates the system, so --dry-run has to be genuinely inert."""
    repository = create_autospec(EventRepository, instance=True)
    repository.list_dlq_events.return_value = (
        [_FakeDLQEvent(1, _valid_payload()), _FakeDLQEvent(2, {"event_type": "broken"})],
        2,
    )

    import replay_dlq

    monkeypatch.setattr(replay_dlq, "EventRepository", lambda _session: repository)
    monkeypatch.setattr(replay_dlq, "async_session_factory", _fake_session_factory)

    def _fail_if_constructed():
        raise AssertionError("dry run must not construct a Kafka producer")

    monkeypatch.setattr(replay_dlq, "EventProducer", _fail_if_constructed)

    exit_code = await replay(parse_args(["--dry-run"]))

    assert exit_code == 0
    output = capsys.readouterr().out
    assert "Would replay 1 of 2" in output
    assert "nothing published" in output


async def test_replay_publishes_only_valid_events(monkeypatch, capsys):
    repository = create_autospec(EventRepository, instance=True)
    good = _valid_payload()
    repository.list_dlq_events.return_value = (
        [_FakeDLQEvent(1, good), _FakeDLQEvent(2, {"event_type": "broken"})],
        2,
    )

    import replay_dlq

    producer = create_autospec(replay_dlq.EventProducer, instance=True)
    monkeypatch.setattr(replay_dlq, "EventRepository", lambda _session: repository)
    monkeypatch.setattr(replay_dlq, "async_session_factory", _fake_session_factory)
    monkeypatch.setattr(replay_dlq, "EventProducer", lambda: producer)

    exit_code = await replay(parse_args([]))

    assert exit_code == 0
    producer.send_event.assert_awaited_once()
    topic, key, message = producer.send_event.call_args.args
    assert topic == "events.raw"
    assert str(key) == good["event_id"]
    assert message["event_type"] == "order.created"
    # The producer must be shut down even on the happy path.
    producer.stop.assert_awaited_once()

    output = capsys.readouterr().out
    assert "Skipping 1 unreplayable" in output


class _FakeSessionContext:
    async def __aenter__(self):
        return None

    async def __aexit__(self, *_exc_info):
        return False


def _fake_session_factory():
    return _FakeSessionContext()
