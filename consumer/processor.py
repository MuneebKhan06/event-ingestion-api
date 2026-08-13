import logging
from typing import Any
from uuid import UUID

from aiokafka.structs import ConsumerRecord

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
from consumer.retry import RetryExhaustedError, with_retry

logger = logging.getLogger(__name__)


async def process_message(
    message: ConsumerRecord,
    repository: EventRepository,
    dlq_handler: DLQHandler,
) -> None:
    raw_value: dict[str, Any] = message.value

    event_id_raw = raw_value.get("event_id")
    event_type = raw_value.get("event_type")
    source = raw_value.get("source")
    payload = raw_value.get("payload")

    if not event_id_raw or not event_type or not source or payload is None:
        await dlq_handler.send(
            raw_payload=raw_value,
            error_reason="Missing required field(s) in Kafka message",
            event_type=event_type,
            source=source,
        )
        dlq_writes.labels(reason=DLQ_REASON_MISSING_FIELDS).inc()
        return

    try:
        event_id = UUID(event_id_raw)
    except (ValueError, AttributeError, TypeError):
        await dlq_handler.send(
            raw_payload=raw_value,
            error_reason=f"Invalid event_id: {event_id_raw!r}",
            event_type=event_type,
            source=source,
        )
        dlq_writes.labels(reason=DLQ_REASON_INVALID_EVENT_ID).inc()
        return

    async def _insert() -> bool:
        return await repository.insert_event(
            event_id=event_id, event_type=event_type, source=source, payload=payload
        )

    try:
        inserted = await with_retry(_insert)
    except RetryExhaustedError as exc:
        await dlq_handler.send(
            raw_payload=raw_value,
            error_reason=f"DB insert failed after retries: {exc.last_error}",
            event_id=event_id,
            event_type=event_type,
            source=source,
        )
        dlq_writes.labels(reason=DLQ_REASON_RETRY_EXHAUSTED).inc()
        return

    if inserted:
        events_persisted.inc()
        logger.info("Processed event %s", event_id)
    else:
        # Not an error: at-least-once delivery means redeliveries are expected,
        # and this is the unique constraint doing its job. Counted separately
        # so a rising duplicate rate is visible without looking like failure.
        events_duplicates_skipped.inc()
        logger.info("Event %s already processed (duplicate), skipping", event_id)
