import logging
from typing import Any
from uuid import UUID, uuid4

from app.db.repository import EventRepository
from app.kafka.producer import EventProducer
from app.kafka.topics import EVENTS_DLQ

logger = logging.getLogger(__name__)

# Width of dlq_events.event_type and dlq_events.source (VARCHAR(50)).
_COLUMN_WIDTH = 50


def _fit_column(value: str | None) -> str | None:
    """Trim a value to fit its DLQ column.

    The DLQ has to accept whatever failed, and "a field too long for its
    column" is itself one way to fail. Recording such an event with the value
    as is would be refused for the same reason the event was, leaving the
    failure with nowhere to go. These columns only index the row for filtering;
    raw_payload keeps the original untouched, so nothing is lost by trimming.
    """
    if value is None or len(value) <= _COLUMN_WIDTH:
        return value
    return value[:_COLUMN_WIDTH]


class DLQHandler:
    """Records a failed event both for durable inspection (Postgres) and for
    replay/alerting (Kafka DLQ topic), per Decision 6 in the design doc.
    """

    def __init__(self, producer: EventProducer, repository: EventRepository):
        self._producer = producer
        self._repository = repository

    async def send(
        self,
        raw_payload: dict[str, Any],
        error_reason: str,
        event_id: UUID | None = None,
        event_type: str | None = None,
        source: str | None = None,
    ) -> None:
        await self._repository.insert_dlq_event(
            raw_payload=raw_payload,
            error_reason=error_reason,
            event_id=event_id,
            event_type=_fit_column(event_type),
            source=_fit_column(source),
        )
        dlq_message = {
            "event_id": str(event_id) if event_id else None,
            "event_type": event_type,
            "source": source,
            "error_reason": error_reason,
            "raw_payload": raw_payload,
        }
        # Falls back to a random key when the original event_id is unknown
        # (e.g. an unparseable payload), just to satisfy the producer's key type.
        await self._producer.send_event(EVENTS_DLQ, event_id or uuid4(), dlq_message)
        logger.warning("Event routed to DLQ (event_id=%s): %s", event_id, error_reason)
