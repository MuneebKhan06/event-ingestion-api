import uuid
from datetime import datetime, timezone
from unittest.mock import AsyncMock, create_autospec

import pytest
from fastapi.testclient import TestClient

import app.api.routes.events as events_module
import app.main as main_module
from app.db.connection import get_db_session
from app.db.models import Event
from app.db.repository import EventRepository
from app.kafka.producer import producer as kafka_producer


@pytest.fixture
def client(monkeypatch):
    monkeypatch.setattr(kafka_producer, "start", AsyncMock())
    monkeypatch.setattr(kafka_producer, "stop", AsyncMock())
    monkeypatch.setattr(kafka_producer, "send_event", AsyncMock())

    mock_repository = create_autospec(EventRepository, instance=True)
    monkeypatch.setattr(events_module, "EventRepository", lambda session: mock_repository)

    async def _fake_session():
        yield None

    main_module.app.dependency_overrides[get_db_session] = _fake_session

    with TestClient(main_module.app) as test_client:
        test_client.mock_repository = mock_repository
        yield test_client

    main_module.app.dependency_overrides.clear()


def _payload(event_id: str | None = None) -> dict:
    return {
        "event_id": event_id or str(uuid.uuid4()),
        "event_type": "order.created",
        "source": "order-service",
        "payload": {"order_id": "ORD-001", "amount": 99.99},
    }


def test_create_event_happy_path(client):
    client.mock_repository.get_by_event_id.return_value = None

    response = client.post("/events", json=_payload())

    assert response.status_code == 202
    assert response.json()["status"] == "accepted"


def test_create_event_duplicate_returns_409(client):
    client.mock_repository.get_by_event_id.return_value = object()

    response = client.post("/events", json=_payload())

    assert response.status_code == 409


def test_create_event_invalid_payload_returns_422(client):
    response = client.post(
        "/events",
        json={"event_id": "not-a-uuid", "event_type": "bad", "source": "x", "payload": {}},
    )

    assert response.status_code == 422


def test_get_event_found(client):
    event_id = uuid.uuid4()
    fake_event = Event(
        id=1,
        event_id=event_id,
        event_type="order.created",
        source="order-service",
        payload={"order_id": "ORD-001"},
        status="processed",
        created_at=datetime.now(timezone.utc),
        processed_at=None,
    )
    client.mock_repository.get_by_event_id.return_value = fake_event

    response = client.get(f"/events/{event_id}")

    assert response.status_code == 200
    assert response.json()["event_id"] == str(event_id)


def test_get_event_not_found(client):
    client.mock_repository.get_by_event_id.return_value = None

    response = client.get(f"/events/{uuid.uuid4()}")

    assert response.status_code == 404
