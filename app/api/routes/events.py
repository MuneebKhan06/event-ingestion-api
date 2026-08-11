import logging
from datetime import datetime
from uuid import UUID

from fastapi import APIRouter, Depends, HTTPException, Query, status
from sqlalchemy.ext.asyncio import AsyncSession

from app.core.idempotency import DuplicateEventError, ensure_not_duplicate
from app.db.connection import get_db_session
from app.db.repository import EventRepository
from app.kafka.producer import producer as event_producer
from app.kafka.topics import EVENTS_RAW
from app.schemas.events import (
    EventAccepted,
    EventCreate,
    EventListResponse,
    EventResponse,
)

logger = logging.getLogger(__name__)
router = APIRouter()


@router.post("/events", response_model=EventAccepted, status_code=status.HTTP_202_ACCEPTED)
async def create_event(
    event: EventCreate, session: AsyncSession = Depends(get_db_session)
) -> EventAccepted:
    repository = EventRepository(session)

    try:
        await ensure_not_duplicate(repository, event.event_id)
    except DuplicateEventError as exc:
        raise HTTPException(status_code=status.HTTP_409_CONFLICT, detail=str(exc)) from exc

    await event_producer.send_event(
        EVENTS_RAW,
        event.event_id,
        {
            "event_id": str(event.event_id),
            "event_type": event.event_type,
            "source": event.source,
            "payload": event.payload,
        },
    )
    logger.info("Published event %s to %s", event.event_id, EVENTS_RAW)
    return EventAccepted(event_id=event.event_id)


@router.get("/events", response_model=EventListResponse)
async def list_events(
    event_type: str | None = Query(None, description="Exact match, e.g. 'order.created'"),
    source: str | None = Query(None, description="Exact match, e.g. 'order-service'"),
    since: datetime | None = Query(None, description="Only events created at or after this"),
    until: datetime | None = Query(None, description="Only events created at or before this"),
    limit: int = Query(50, ge=1, le=200, description="Page size"),
    offset: int = Query(0, ge=0, description="Rows to skip"),
    session: AsyncSession = Depends(get_db_session),
) -> EventListResponse:
    """List stored events, newest first.

    The limit is capped rather than unbounded so a single request can't ask the
    database for the entire table.
    """
    repository = EventRepository(session)
    events, total = await repository.list_events(
        event_type=event_type,
        source=source,
        since=since,
        until=until,
        limit=limit,
        offset=offset,
    )
    return EventListResponse(
        total=total,
        limit=limit,
        offset=offset,
        events=[EventResponse.model_validate(event) for event in events],
    )


@router.get("/events/{event_id}", response_model=EventResponse)
async def get_event(
    event_id: UUID, session: AsyncSession = Depends(get_db_session)
) -> EventResponse:
    repository = EventRepository(session)
    db_event = await repository.get_by_event_id(event_id)
    if db_event is None:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Event not found")
    return EventResponse.model_validate(db_event)
