from types import SimpleNamespace
from unittest.mock import create_autospec
from uuid import uuid4

from app.core.dlq import DLQHandler
from app.db.repository import EventRepository
from consumer.metrics import (
    DLQ_REASON_INVALID_EVENT_ID,
    DLQ_REASON_MISSING_FIELDS,
    DLQ_REASON_RETRY_EXHAUSTED,
    dlq_writes,
    events_duplicates_skipped,
    events_persisted,
)
from consumer.processor import process_message


def _message(**overrides) -> SimpleNamespace:
    value = {
        "event_id": str(uuid4()),
        "event_type": "order.created",
        "source": "order-service",
        "payload": {"a": 1},
    }
    value.update(overrides)
    return SimpleNamespace(value=value)


def _dlq_count(reason: str) -> float:
    return dlq_writes.labels(reason=reason)._value.get()


async def test_persisted_counter_increments_on_insert():
    repository = create_autospec(EventRepository, instance=True)
    repository.insert_event.return_value = True
    before = events_persisted._value.get()

    await process_message(_message(), repository, create_autospec(DLQHandler, instance=True))

    assert events_persisted._value.get() == before + 1


async def test_duplicate_skip_is_counted_separately_from_a_persist():
    """A redelivery is expected under at-least-once, not a failure.

    Counting it as persisted would overstate throughput; counting it as an
    error would make normal operation look broken.
    """
    repository = create_autospec(EventRepository, instance=True)
    repository.insert_event.return_value = False
    before_dupes = events_duplicates_skipped._value.get()
    before_persisted = events_persisted._value.get()

    await process_message(_message(), repository, create_autospec(DLQHandler, instance=True))

    assert events_duplicates_skipped._value.get() == before_dupes + 1
    assert events_persisted._value.get() == before_persisted


async def test_dlq_writes_are_counted_by_reason():
    repository = create_autospec(EventRepository, instance=True)
    dlq_handler = create_autospec(DLQHandler, instance=True)
    before = _dlq_count(DLQ_REASON_MISSING_FIELDS)

    await process_message(_message(source=None), repository, dlq_handler)

    assert _dlq_count(DLQ_REASON_MISSING_FIELDS) == before + 1


async def test_invalid_event_id_uses_its_own_reason():
    repository = create_autospec(EventRepository, instance=True)
    dlq_handler = create_autospec(DLQHandler, instance=True)
    before_invalid = _dlq_count(DLQ_REASON_INVALID_EVENT_ID)
    before_missing = _dlq_count(DLQ_REASON_MISSING_FIELDS)

    await process_message(_message(event_id="not-a-uuid"), repository, dlq_handler)

    assert _dlq_count(DLQ_REASON_INVALID_EVENT_ID) == before_invalid + 1
    # Reasons must not bleed into each other, or the breakdown is useless.
    assert _dlq_count(DLQ_REASON_MISSING_FIELDS) == before_missing


async def test_retry_exhaustion_uses_its_own_reason(monkeypatch):
    from app.config import get_settings

    settings = get_settings()
    monkeypatch.setattr(settings, "consumer_max_retries", 0)
    monkeypatch.setattr(settings, "consumer_retry_backoff_base_seconds", 0.0)

    repository = create_autospec(EventRepository, instance=True)
    repository.insert_event.side_effect = RuntimeError("db down")
    before = _dlq_count(DLQ_REASON_RETRY_EXHAUSTED)

    await process_message(_message(), repository, create_autospec(DLQHandler, instance=True))

    assert _dlq_count(DLQ_REASON_RETRY_EXHAUSTED) == before + 1


def test_every_dlq_reason_exists_before_any_failure_occurs():
    """Series must pre-exist, or rate()/alerts referencing them break.

    A label that only materialises on first failure is absent from the
    exposition until the incident starts — exactly when the query matters.
    """
    exposed = {
        sample.labels["reason"]
        for metric in dlq_writes.collect()
        for sample in metric.samples
        if "reason" in sample.labels
    }
    assert {
        DLQ_REASON_MISSING_FIELDS,
        DLQ_REASON_INVALID_EVENT_ID,
        DLQ_REASON_RETRY_EXHAUSTED,
    } <= exposed
