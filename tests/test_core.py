from unittest.mock import Mock, create_autospec, patch
from uuid import UUID, uuid4

import pytest

from app.core.dlq import DLQHandler
from app.core.idempotency import DuplicateEventError, ensure_not_duplicate
from app.db.repository import EventRepository
from app.kafka.producer import EventProducer
from app.kafka.topics import EVENTS_DLQ


@pytest.fixture
def producer():
    return create_autospec(EventProducer, instance=True)


@pytest.fixture
def repository():
    return create_autospec(EventRepository, instance=True)


# --------------------------------------------------------------------------
# DLQHandler
# --------------------------------------------------------------------------


async def test_dlq_send_writes_to_both_postgres_and_kafka(producer, repository):
    """Decision 6: a failed event is recorded in both places.

    Postgres gives durable inspection, the Kafka topic gives replay and
    alerting — dropping either one loses half of that.
    """
    handler = DLQHandler(producer, repository)
    event_id = uuid4()

    await handler.send(
        raw_payload={"broken": True},
        error_reason="something failed",
        event_id=event_id,
        event_type="order.created",
        source="order-service",
    )

    repository.insert_dlq_event.assert_awaited_once_with(
        raw_payload={"broken": True},
        error_reason="something failed",
        event_id=event_id,
        event_type="order.created",
        source="order-service",
    )
    producer.send_event.assert_awaited_once()


async def test_dlq_send_publishes_to_the_dlq_topic_with_full_context(producer, repository):
    handler = DLQHandler(producer, repository)
    event_id = uuid4()

    await handler.send(
        raw_payload={"order_id": "ORD-1"},
        error_reason="DB insert failed after retries",
        event_id=event_id,
        event_type="order.created",
        source="order-service",
    )

    topic, key, message = producer.send_event.call_args.args
    assert topic == EVENTS_DLQ
    assert key == event_id
    # The original payload and the reason both have to survive, otherwise the
    # DLQ can't be used to diagnose or replay the failure.
    assert message == {
        "event_id": str(event_id),
        "event_type": "order.created",
        "source": "order-service",
        "error_reason": "DB insert failed after retries",
        "raw_payload": {"order_id": "ORD-1"},
    }


async def test_dlq_send_falls_back_to_generated_key_when_event_id_unknown(
    producer, repository
):
    """An unparseable payload has no event_id, but Kafka still needs a key."""
    handler = DLQHandler(producer, repository)
    generated = UUID("11111111-2222-3333-4444-555555555555")

    with patch("app.core.dlq.uuid4", return_value=generated):
        await handler.send(raw_payload={"garbage": "?"}, error_reason="Invalid event_id")

    _topic, key, message = producer.send_event.call_args.args
    assert key == generated
    # The generated key is a routing detail only — it must not be recorded as
    # if it were the real event_id.
    assert message["event_id"] is None


async def test_dlq_send_records_durably_before_publishing(producer, repository):
    """Postgres write comes first, so a Kafka failure still leaves a record."""
    manager = Mock()
    manager.attach_mock(repository.insert_dlq_event, "insert_dlq_event")
    manager.attach_mock(producer.send_event, "send_event")

    handler = DLQHandler(producer, repository)
    await handler.send(raw_payload={}, error_reason="boom")

    called = [name for name, _args, _kwargs in manager.mock_calls]
    assert called == ["insert_dlq_event", "send_event"]


# --------------------------------------------------------------------------
# ensure_not_duplicate
# --------------------------------------------------------------------------


async def test_ensure_not_duplicate_passes_when_event_is_new(repository):
    repository.get_by_event_id.return_value = None

    await ensure_not_duplicate(repository, uuid4())  # must not raise


async def test_ensure_not_duplicate_raises_when_event_exists(repository):
    event_id = uuid4()
    repository.get_by_event_id.return_value = object()

    with pytest.raises(DuplicateEventError) as exc_info:
        await ensure_not_duplicate(repository, event_id)

    # The API turns this into a 409, so the id has to be carried through.
    assert exc_info.value.event_id == event_id
    assert str(event_id) in str(exc_info.value)
