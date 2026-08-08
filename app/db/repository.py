import logging
from uuid import UUID

from sqlalchemy import select
from sqlalchemy.dialects.postgresql import insert as pg_insert
from sqlalchemy.ext.asyncio import AsyncSession

from app.db.models import DLQEvent, Event

logger = logging.getLogger(__name__)


class EventRepository:
    def __init__(self, session: AsyncSession):
        self._session = session

    async def insert_event(
        self,
        event_id: UUID,
        event_type: str,
        source: str,
        payload: dict,
    ) -> bool:
        """Insert an event. Returns False without raising if event_id already exists."""
        stmt = (
            pg_insert(Event)
            .values(event_id=event_id, event_type=event_type, source=source, payload=payload)
            .on_conflict_do_nothing(index_elements=["event_id"])
            .returning(Event.id)
        )
        result = await self._session.execute(stmt)
        await self._session.commit()
        inserted = result.first() is not None
        if inserted:
            logger.info("Inserted event %s (type=%s, source=%s)", event_id, event_type, source)
        else:
            logger.info("Skipped duplicate event %s (already exists)", event_id)
        return inserted

    async def get_by_event_id(self, event_id: UUID) -> Event | None:
        stmt = select(Event).where(Event.event_id == event_id)
        result = await self._session.execute(stmt)
        return result.scalar_one_or_none()

    async def insert_dlq_event(
        self,
        raw_payload: dict,
        error_reason: str,
        event_id: UUID | None = None,
        event_type: str | None = None,
        source: str | None = None,
    ) -> None:
        dlq_event = DLQEvent(
            event_id=event_id,
            event_type=event_type,
            raw_payload=raw_payload,
            source=source,
            error_reason=error_reason,
        )
        self._session.add(dlq_event)
        await self._session.commit()
        logger.info(
            "Routed event %s to DLQ (type=%s, source=%s, reason=%s)",
            event_id,
            event_type,
            source,
            error_reason,
        )
