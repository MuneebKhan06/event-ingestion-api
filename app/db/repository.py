import logging
from datetime import datetime
from uuid import UUID

from sqlalchemy import func, select
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

    async def list_events(
        self,
        event_type: str | None = None,
        source: str | None = None,
        since: datetime | None = None,
        until: datetime | None = None,
        limit: int = 50,
        offset: int = 0,
    ) -> tuple[list[Event], int]:
        """Return a page of events plus the total matching the same filters.

        The filter columns (event_type, source, created_at) are exactly the ones
        indexed in migration 001 — these are the access patterns that schema was
        designed for. Ordering is created_at DESC to match the
        idx_events_created_at index rather than forcing a sort.
        """
        filters = []
        if event_type is not None:
            filters.append(Event.event_type == event_type)
        if source is not None:
            filters.append(Event.source == source)
        if since is not None:
            filters.append(Event.created_at >= since)
        if until is not None:
            filters.append(Event.created_at <= until)

        page_stmt = (
            select(Event)
            .where(*filters)
            .order_by(Event.created_at.desc())
            .limit(limit)
            .offset(offset)
        )
        page_result = await self._session.execute(page_stmt)
        events = list(page_result.scalars().all())

        # Counted separately so the caller can page without losing sight of how
        # much there is; the filters have to match the page query exactly.
        count_stmt = select(func.count()).select_from(Event).where(*filters)
        count_result = await self._session.execute(count_stmt)
        total = count_result.scalar_one()

        return events, total

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

    async def list_events_after(self, last_id: int, limit: int = 100) -> list[Event]:
        """Events with an id above the cursor, oldest first.

        Paged by id rather than created_at because ids are strictly increasing
        and unique, so a cursor can never skip or repeat a row. Two events
        sharing a timestamp would make a created_at cursor ambiguous.
        """
        stmt = (
            select(Event)
            .where(Event.id > last_id)
            .order_by(Event.id.asc())
            .limit(limit)
        )
        result = await self._session.execute(stmt)
        return list(result.scalars().all())

    async def latest_event_id(self) -> int:
        """Highest id currently stored, or 0 when the table is empty.

        Lets a new stream start from "now" instead of replaying history.
        """
        result = await self._session.execute(select(func.max(Event.id)))
        return result.scalar_one() or 0

    async def list_dlq_events(
        self,
        event_type: str | None = None,
        source: str | None = None,
        limit: int = 100,
        offset: int = 0,
        newest_first: bool = False,
    ) -> tuple[list[DLQEvent], int]:
        """Return a page of failed events plus the total matching the filters.

        Ordering is a genuine split by use case, so it's explicit rather than
        assumed: replay wants oldest-first, to reprocess in roughly the order
        things originally failed; someone inspecting a live incident wants
        newest-first, to see what just broke. The latter is what
        idx_dlq_events_failed_at (DESC) is built for.
        """
        filters = []
        if event_type is not None:
            filters.append(DLQEvent.event_type == event_type)
        if source is not None:
            filters.append(DLQEvent.source == source)

        ordering = DLQEvent.failed_at.desc() if newest_first else DLQEvent.failed_at.asc()
        page_stmt = (
            select(DLQEvent).where(*filters).order_by(ordering).limit(limit).offset(offset)
        )
        page_result = await self._session.execute(page_stmt)
        dlq_events = list(page_result.scalars().all())

        count_stmt = select(func.count()).select_from(DLQEvent).where(*filters)
        count_result = await self._session.execute(count_stmt)

        return dlq_events, count_result.scalar_one()
