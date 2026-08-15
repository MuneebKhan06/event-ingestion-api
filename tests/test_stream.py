import asyncio
import json
import uuid
from datetime import datetime, timezone
from unittest.mock import create_autospec

import pytest
from fastapi import HTTPException

import app.api.routes.stream as stream_module
from app.db.models import Event
from app.db.repository import EventRepository


def _event(event_id: int) -> Event:
    return Event(
        id=event_id,
        event_id=uuid.uuid4(),
        event_type="quake.detected",
        source="usgs",
        payload={"magnitude": 4.2},
        status="processed",
        created_at=datetime.now(timezone.utc),
        processed_at=None,
    )


class _FakeSessionContext:
    async def __aenter__(self):
        return None

    async def __aexit__(self, *_exc_info):
        return False


@pytest.fixture
def repository(monkeypatch):
    repo = create_autospec(EventRepository, instance=True)
    monkeypatch.setattr(stream_module, "async_session_factory", _FakeSessionContext)
    monkeypatch.setattr(stream_module, "EventRepository", lambda _session: repo)
    monkeypatch.setattr(stream_module, "POLL_SECONDS", 0.01)
    return repo


async def _take(generator, count: int) -> list[str]:
    """Pull a fixed number of frames, then close the generator."""
    frames = []
    try:
        async for frame in generator:
            frames.append(frame)
            if len(frames) >= count:
                break
    finally:
        await generator.aclose()
    return frames


async def test_streams_events_newer_than_the_cursor(repository):
    repository.list_events_after.return_value = [_event(11), _event(12)]

    frames = await _take(stream_module._event_frames(after=10), 2)

    assert all(f.startswith("data: ") for f in frames)
    payload = json.loads(frames[0].removeprefix("data: ").strip())
    assert payload["event_type"] == "quake.detected"
    assert repository.list_events_after.call_args.args[0] == 10


async def test_cursor_advances_so_events_are_not_resent(repository):
    """Without this the same rows would stream forever."""
    repository.list_events_after.side_effect = [[_event(11), _event(12)], [_event(13)]]

    await _take(stream_module._event_frames(after=10), 3)

    # Second poll must start from the last id yielded, not the original cursor.
    assert repository.list_events_after.call_args.args[0] == 12


async def test_emits_a_heartbeat_when_nothing_is_new(repository):
    """Idle connections still need traffic or proxies close them."""
    repository.list_events_after.return_value = []

    frames = await _take(stream_module._event_frames(after=0), 2)

    assert frames == [": keepalive\n\n", ": keepalive\n\n"]


async def test_starts_from_the_newest_event_when_no_cursor_given(repository):
    repository.latest_event_id.return_value = 99
    repository.list_events_after.return_value = []

    await _take(stream_module._event_frames(after=None), 1)

    repository.latest_event_id.assert_awaited_once()
    assert repository.list_events_after.call_args.args[0] == 99


async def test_explicit_cursor_skips_the_latest_id_lookup(repository):
    repository.list_events_after.return_value = []

    await _take(stream_module._event_frames(after=5), 1)

    repository.latest_event_id.assert_not_awaited()


async def test_client_disconnect_does_not_raise(repository):
    """Closing a tab is normal, not a failure."""
    repository.list_events_after.return_value = []
    generator = stream_module._event_frames(after=0)

    await generator.__anext__()
    await generator.aclose()  # raises CancelledError inside the generator


def test_route_is_not_shadowed_by_the_event_id_route():
    """/events/stream must not be captured by /events/{event_id}.

    Route order is load bearing: registered the other way round, "stream" is
    parsed as an event_id and the request fails 422 as an invalid UUID. Checked
    by resolving the path against the real router rather than by issuing a
    request, because the endpoint never completes a response body.
    """
    from starlette.routing import Match

    import app.main as main_module

    scope = {"type": "http", "method": "GET", "path": "/events/stream"}
    matched = [
        route
        for route in main_module.app.routes
        if route.matches(scope)[0] is Match.FULL
    ]

    assert matched, "no route matched /events/stream"
    assert matched[0].endpoint is stream_module.stream_events


async def test_response_carries_streaming_headers():
    """Missing these lets proxies buffer or cache the stream into uselessness."""
    response = await stream_module.stream_events(after=0)

    assert response.media_type == "text/event-stream"
    assert response.headers["cache-control"] == "no-cache"
    assert response.headers["x-accel-buffering"] == "no"


async def test_batch_size_is_bounded(repository):
    """One client must not be able to pull the whole table in a single read."""
    repository.list_events_after.return_value = []

    await _take(stream_module._event_frames(after=0), 1)

    assert repository.list_events_after.call_args.args[1] == stream_module.MAX_BATCH
    assert stream_module.MAX_BATCH <= 1000


def test_poll_interval_is_bounded():
    assert 0 < stream_module.POLL_SECONDS <= 5
    assert asyncio  # imported for the generator's sleep


# --------------------------------------------------------------------------
# Concurrent client cap
# --------------------------------------------------------------------------


@pytest.fixture(autouse=True)
def _reset_open_streams():
    """Counter is module state, so leaking it between tests would cascade."""
    stream_module._open_streams = 0
    yield
    stream_module._open_streams = 0


async def test_refuses_new_streams_past_the_limit(monkeypatch):
    """Each stream is a database poller, so the cap protects Postgres."""
    from app.config import get_settings

    monkeypatch.setattr(get_settings(), "stream_max_clients", 2)
    stream_module._open_streams = 2

    with pytest.raises(HTTPException) as exc_info:
        await stream_module.stream_events(after=0)

    assert exc_info.value.status_code == 503
    assert "Too many open event streams" in exc_info.value.detail


async def test_allows_a_stream_while_below_the_limit(monkeypatch):
    from app.config import get_settings

    monkeypatch.setattr(get_settings(), "stream_max_clients", 2)
    stream_module._open_streams = 1

    response = await stream_module.stream_events(after=0)

    assert response.media_type == "text/event-stream"


async def test_open_streams_is_counted_and_released(repository):
    repository.list_events_after.return_value = []
    assert stream_module._open_streams == 0

    generator = stream_module._event_frames(after=0)
    await generator.__anext__()
    assert stream_module._open_streams == 1

    await generator.aclose()
    assert stream_module._open_streams == 0


async def test_slot_is_released_even_when_the_stream_errors(repository):
    """A leaked slot would slowly close the endpoint with no way back."""
    repository.list_events_after.side_effect = RuntimeError("database gone")

    generator = stream_module._event_frames(after=0)
    with pytest.raises(RuntimeError):
        await generator.__anext__()

    assert stream_module._open_streams == 0
