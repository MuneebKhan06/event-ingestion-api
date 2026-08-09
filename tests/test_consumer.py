from types import SimpleNamespace
from unittest.mock import create_autospec
from uuid import uuid4

from app.core.dlq import DLQHandler
from app.db.repository import EventRepository
from consumer.processor import process_message


def _make_message(value: dict) -> SimpleNamespace:
    return SimpleNamespace(value=value)


async def test_valid_message_inserts_event():
    repository = create_autospec(EventRepository, instance=True)
    repository.insert_event.return_value = True
    dlq_handler = create_autospec(DLQHandler, instance=True)

    message = _make_message(
        {
            "event_id": str(uuid4()),
            "event_type": "order.created",
            "source": "order-service",
            "payload": {"a": 1},
        }
    )

    await process_message(message, repository, dlq_handler)

    repository.insert_event.assert_awaited_once()
    dlq_handler.send.assert_not_awaited()


async def test_missing_fields_routed_to_dlq():
    repository = create_autospec(EventRepository, instance=True)
    dlq_handler = create_autospec(DLQHandler, instance=True)

    message = _make_message({"event_type": "order.created", "source": "order-service"})

    await process_message(message, repository, dlq_handler)

    repository.insert_event.assert_not_awaited()
    dlq_handler.send.assert_awaited_once()
    assert "Missing required field" in dlq_handler.send.call_args.kwargs["error_reason"]


async def test_invalid_event_id_routed_to_dlq():
    repository = create_autospec(EventRepository, instance=True)
    dlq_handler = create_autospec(DLQHandler, instance=True)

    message = _make_message(
        {
            "event_id": "not-a-uuid",
            "event_type": "order.created",
            "source": "order-service",
            "payload": {},
        }
    )

    await process_message(message, repository, dlq_handler)

    repository.insert_event.assert_not_awaited()
    dlq_handler.send.assert_awaited_once()
    assert "Invalid event_id" in dlq_handler.send.call_args.kwargs["error_reason"]


async def test_retry_exhaustion_routed_to_dlq(monkeypatch):
    from app.config import get_settings

    settings = get_settings()
    monkeypatch.setattr(settings, "consumer_max_retries", 0)
    monkeypatch.setattr(settings, "consumer_retry_backoff_base_seconds", 0.0)

    repository = create_autospec(EventRepository, instance=True)
    repository.insert_event.side_effect = RuntimeError("db down")
    dlq_handler = create_autospec(DLQHandler, instance=True)

    message = _make_message(
        {
            "event_id": str(uuid4()),
            "event_type": "order.created",
            "source": "order-service",
            "payload": {},
        }
    )

    await process_message(message, repository, dlq_handler)

    dlq_handler.send.assert_awaited_once()
    assert "DB insert failed after retries" in dlq_handler.send.call_args.kwargs["error_reason"]
