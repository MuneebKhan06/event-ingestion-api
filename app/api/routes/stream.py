"""Server-Sent Events feed of newly stored events.

Deliberately does NOT use Depends(get_db_session). A dependency-scoped session
lives as long as the request, and these requests are open-ended, so every
connected client would hold a pooled connection idle between polls. A handful
of dashboards left open would exhaust the pool and starve actual traffic. Each
poll therefore opens a short-lived session and returns it immediately.
"""

import asyncio
import json
import logging
import random
from collections.abc import AsyncIterator

from fastapi import APIRouter, HTTPException, Query, status
from fastapi.responses import StreamingResponse

from app.config import get_settings
from app.db.connection import async_session_factory
from app.db.repository import EventRepository
from app.schemas.events import EventResponse

logger = logging.getLogger(__name__)
router = APIRouter()

POLL_SECONDS = 1.0

# Clients that all sleep exactly POLL_SECONDS settle into the same rhythm, so
# their queries arrive together in bursts with idle gaps between. Spreading the
# interval slightly lets connections drift apart and keeps the load even. Small
# effect at twenty clients, but it costs nothing and the alternative gets worse
# with every client added.
JITTER_RANGE = (0.85, 1.15)

# Live count of open streams. Each one polls the database every POLL_SECONDS,
# so connections are a database load multiplier, not just sockets: twenty idle
# dashboards mean twenty queries a second against Postgres forever. Refusing
# the twenty first is better than degrading ingestion for everyone.
_open_streams = 0
# Caps how much one connection can pull per poll, so a client attaching to a
# large backlog drains it steadily instead of in a single huge read.
MAX_BATCH = 100


def _next_delay() -> float:
    """POLL_SECONDS with jitter, so concurrent streams do not synchronise."""
    return POLL_SECONDS * random.uniform(*JITTER_RANGE)


async def _event_frames(after: int | None) -> AsyncIterator[str]:
    global _open_streams

    _open_streams += 1
    try:
        async for frame in _poll_loop(after):
            yield frame
    finally:
        # In a finally so a disconnect, an error and a normal close all release
        # the slot. Leaking one on any path would eventually close the endpoint
        # to everybody with no way back short of a restart.
        _open_streams -= 1


async def _poll_loop(after: int | None) -> AsyncIterator[str]:
    async with async_session_factory() as session:
        cursor = after if after is not None else await EventRepository(session).latest_event_id()

    try:
        while True:
            async with async_session_factory() as session:
                events = await EventRepository(session).list_events_after(cursor, MAX_BATCH)

            if events:
                for event in events:
                    body = EventResponse.model_validate(event).model_dump(mode="json")
                    yield f"data: {json.dumps(body)}\n\n"
                cursor = events[-1].id
            else:
                # An SSE comment. Keeps proxies and load balancers from closing
                # a connection that is simply idle rather than broken.
                yield ": keepalive\n\n"

            await asyncio.sleep(_next_delay())
    except asyncio.CancelledError:
        # Normal: the client navigated away or closed the tab. Nothing failed,
        # so this must not surface as an error.
        logger.debug("SSE client disconnected")
        raise


@router.get("/events/stream")
async def stream_events(
    after: int | None = Query(
        None,
        ge=0,
        description="Resume after this event id. Omit to start from the newest.",
    ),
) -> StreamingResponse:
    """Stream events as they are persisted.

    Reads what the consumer has already written rather than tapping Kafka, so
    what a client sees here is exactly what is durably stored.
    """
    # Checked before the response starts, so a refused client gets a normal
    # 503 it can act on rather than an event-stream that opens and dies.
    limit = get_settings().stream_max_clients
    if _open_streams >= limit:
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail=f"Too many open event streams (limit {limit}); retry shortly.",
        )

    return StreamingResponse(
        _event_frames(after),
        media_type="text/event-stream",
        headers={
            "Cache-Control": "no-cache",
            # nginx buffers proxied responses by default, which would hold
            # frames back until the buffer filled and defeat the point.
            "X-Accel-Buffering": "no",
        },
    )
