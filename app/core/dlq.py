import logging
from typing import Any
from uuid import UUID, uuid4

from app.db.repository import EventRepository
from app.kafka.producer import EventProducer
from app.kafka.topics import EVENTS_DLQ

logger = logging.getLogger(__name__)


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
            event_type=event_type,
            source=source,
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
