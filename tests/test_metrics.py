import uuid
from unittest.mock import AsyncMock, create_autospec

import pytest
from fastapi.testclient import TestClient

import app.api.routes.events as events_module
import app.main as main_module
from app.core.metrics import events_accepted, events_duplicates
from app.db.connection import get_db_session
from app.db.repository import EventRepository
from app.kafka.producer import producer as kafka_producer


@pytest.fixture
def client(monkeypatch):
    monkeypatch.setattr(kafka_producer, "start", AsyncMock())
    monkeypatch.setattr(kafka_producer, "stop", AsyncMock())
    monkeypatch.setattr(kafka_producer, "send_event", AsyncMock())

    mock_repository = create_autospec(EventRepository, instance=True)
    monkeypatch.setattr(events_module, "EventRepository", lambda _session: mock_repository)

    async def _fake_session():
        yield None

    main_module.app.dependency_overrides[get_db_session] = _fake_session
    with TestClient(main_module.app) as test_client:
        test_client.mock_repository = mock_repository
        yield test_client
    main_module.app.dependency_overrides.clear()


def _counter_value(counter) -> float:
    return counter._value.get()


def _payload() -> dict:
    return {
        "event_id": str(uuid.uuid4()),
        "event_type": "order.created",
        "source": "order-service",
        "payload": {"order_id": "ORD-1"},
    }


def test_metrics_endpoint_serves_prometheus_format(client):
    response = client.get("/metrics")

    assert response.status_code == 200
    assert response.headers["content-type"].startswith("text/plain")
    body = response.text
    # Exposition format: HELP/TYPE lines precede each metric.
    assert "# HELP events_accepted_total" in body
    assert "# TYPE events_accepted_total counter" in body
    assert "events_duplicates_total" in body


def test_accepted_counter_increments_on_202(client):
    client.mock_repository.get_by_event_id.return_value = None
    before = _counter_value(events_accepted)

    assert client.post("/events", json=_payload()).status_code == 202

    assert _counter_value(events_accepted) == before + 1


def test_duplicate_counter_increments_on_409(client):
    client.mock_repository.get_by_event_id.return_value = object()
    before_dupes = _counter_value(events_duplicates)
    before_accepted = _counter_value(events_accepted)

    assert client.post("/events", json=_payload()).status_code == 409

    assert _counter_value(events_duplicates) == before_dupes + 1
    # A rejected event must not also count as accepted.
    assert _counter_value(events_accepted) == before_accepted


def test_validation_failure_counts_as_neither(client):
    """A 422 never reached the publish path, so it is not an accepted event."""
    before_accepted = _counter_value(events_accepted)
    before_dupes = _counter_value(events_duplicates)

    response = client.post("/events", json={"event_id": "not-a-uuid"})

    assert response.status_code == 422
    assert _counter_value(events_accepted) == before_accepted
    assert _counter_value(events_duplicates) == before_dupes


def test_counters_carry_no_client_supplied_labels():
    """Labelling on event_type/source would let a caller explode cardinality.

    Prometheus creates a time series per label combination, and event_type is
    an arbitrary client-supplied string, so this stays deliberately unlabelled.
    """
    assert events_accepted._labelnames == ()
    assert events_duplicates._labelnames == ()
