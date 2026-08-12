import logging

from fastapi import APIRouter, Depends, Query
from sqlalchemy.ext.asyncio import AsyncSession

from app.db.connection import get_db_session
from app.db.repository import EventRepository
from app.schemas.events import DLQEventResponse, DLQListResponse

logger = logging.getLogger(__name__)
router = APIRouter()


@router.get("/dlq", response_model=DLQListResponse)
async def list_dlq_events(
    event_type: str | None = Query(None, description="Exact match, e.g. 'order.created'"),
    source: str | None = Query(None, description="Exact match, e.g. 'order-service'"),
    limit: int = Query(50, ge=1, le=200, description="Page size"),
    offset: int = Query(0, ge=0, description="Rows to skip"),
    session: AsyncSession = Depends(get_db_session),
) -> DLQListResponse:
    """List failed events, newest first.

    Decision 6 keeps failures for inspection as well as replay, but until now
    inspection meant querying Postgres by hand. Newest-first because the
    question being asked is almost always "what is failing right now" — and
    `total` is what an alert on DLQ growth would watch.
    """
    repository = EventRepository(session)
    dlq_events, total = await repository.list_dlq_events(
        event_type=event_type,
        source=source,
        limit=limit,
        offset=offset,
        newest_first=True,
    )
    return DLQListResponse(
        total=total,
        limit=limit,
        offset=offset,
        events=[DLQEventResponse.model_validate(event) for event in dlq_events],
    )
