import uuid

import pytest
from pydantic import ValidationError

from app.schemas.events import MAX_PAYLOAD_BYTES, EventCreate


def test_valid_event_create():
    event = EventCreate(
        event_id=uuid.uuid4(),
        event_type="order.created",
        source="order-service",
        payload={"order_id": "ORD-001", "amount": 99.99},
    )
    assert event.event_type == "order.created"


def test_event_type_without_dot_rejected():
    with pytest.raises(ValidationError, match="format"):
        EventCreate(
            event_id=uuid.uuid4(),
            event_type="ordercreated",
            source="order-service",
            payload={},
        )


def test_oversized_payload_rejected():
    with pytest.raises(ValidationError, match="exceeding"):
        EventCreate(
            event_id=uuid.uuid4(),
            event_type="order.created",
            source="order-service",
            payload={"blob": "x" * (MAX_PAYLOAD_BYTES + 1000)},
        )
