from uuid import UUID

from app.db.repository import EventRepository


class DuplicateEventError(Exception):
    def __init__(self, event_id: UUID):
        self.event_id = event_id
        super().__init__(f"Event {event_id} already exists")


async def ensure_not_duplicate(repository: EventRepository, event_id: UUID) -> None:
    """Best-effort pre-publish check so the API can return 409 quickly.

    This is not the source of truth for idempotency — that's the unique
    constraint on events.event_id, enforced atomically by the consumer via
    ON CONFLICT DO NOTHING. Two concurrent requests for the same event_id can
    both pass this check and both get published to Kafka; the consumer then
    silently dedupes at insert time. This check just avoids the round trip
    through Kafka for the common case of an obvious duplicate.
    """
    existing = await repository.get_by_event_id(event_id)
    if existing is not None:
        raise DuplicateEventError(event_id)
