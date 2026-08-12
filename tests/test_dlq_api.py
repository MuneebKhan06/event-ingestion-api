import uuid
from datetime import datetime, timezone
from unittest.mock import AsyncMock, create_autospec

import pytest
from fastapi.testclient import TestClient

import app.api.routes.dlq as dlq_module
import app.main as main_module
from app.db.connection import get_db_session
from app.db.models import DLQEvent
from app.db.repository import EventRepository
from app.kafka.producer import producer as kafka_producer


@pytest.fixture
def client(monkeypatch):
    monkeypatch.setattr(kafka_producer, "start", AsyncMock())
    monkeypatch.setattr(kafka_producer, "stop", AsyncMock())

    mock_repository = create_autospec(EventRepository, instance=True)
    monkeypatch.setattr(dlq_module, "EventRepository", lambda _session: mock_repository)

    async def _fake_session():
        yield None

    main_module.app.dependency_overrides[get_db_session] = _fake_session
    with TestClient(main_module.app) as test_client:
        test_client.mock_repository = mock_repository
        yield test_client
    main_module.app.dependency_overrides.clear()


def _fake_dlq_event(reason: str = "DB insert failed after retries") -> DLQEvent:
    return DLQEvent(
        id=1,
        event_id=uuid.uuid4(),
        event_type="order.created",
        source="order-service",
        raw_payload={"order_id": "ORD-1"},
        error_reason=reason,
        failed_at=datetime.now(timezone.utc),
    )


def test_lists_failed_events_with_total(client):
    client.mock_repository.list_dlq_events.return_value = ([_fake_dlq_event()], 12)

    response = client.get("/dlq")

    assert response.status_code == 200
    body = response.json()
    assert body["total"] == 12
    assert len(body["events"]) == 1
    # The reason is the whole point of inspecting the DLQ.
    assert body["events"][0]["error_reason"] == "DB insert failed after retries"
    assert body["events"][0]["raw_payload"] == {"order_id": "ORD-1"}


def test_requests_newest_first(client):
    """Inspecting a DLQ is normally asking what is failing *now*."""
    client.mock_repository.list_dlq_events.return_value = ([], 0)

    client.get("/dlq")

    assert client.mock_repository.list_dlq_events.call_args.kwargs["newest_first"] is True


def test_passes_filters_and_paging_through(client):
    client.mock_repository.list_dlq_events.return_value = ([], 0)

    response = client.get("/dlq", params={"event_type": "user.clicked", "source": "web",
                                          "limit": 5, "offset": 20})

    assert response.status_code == 200
    kwargs = client.mock_repository.list_dlq_events.call_args.kwargs
    assert kwargs["event_type"] == "user.clicked"
    assert kwargs["source"] == "web"
    assert kwargs["limit"] == 5
    assert kwargs["offset"] == 20


def test_rejects_limit_above_cap(client):
    response = client.get("/dlq?limit=5000")

    assert response.status_code == 422
    client.mock_repository.list_dlq_events.assert_not_awaited()


def test_handles_rows_with_no_event_id(client):
    """An unparseable payload reaches the DLQ with null event_id/type/source."""
    orphan = DLQEvent(
        id=2,
        event_id=None,
        event_type=None,
        source=None,
        raw_payload={"garbage": "?"},
        error_reason="Invalid event_id: 'nope'",
        failed_at=datetime.now(timezone.utc),
    )
    client.mock_repository.list_dlq_events.return_value = ([orphan], 1)

    response = client.get("/dlq")

    assert response.status_code == 200
    entry = response.json()["events"][0]
    assert entry["event_id"] is None
    assert entry["event_type"] is None
