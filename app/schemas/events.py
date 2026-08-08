import json
from datetime import datetime
from typing import Any
from uuid import UUID

from pydantic import BaseModel, Field, field_validator

# Generous enough for real event payloads while blocking accidental/abusive
# oversized bodies (e.g. a client mistakenly embedding a file) from reaching Kafka.
MAX_PAYLOAD_BYTES = 32_768


class EventCreate(BaseModel):
    event_id: UUID
    event_type: str = Field(min_length=1, max_length=50)
    source: str = Field(min_length=1, max_length=50)
    payload: dict[str, Any]

    model_config = {
        "json_schema_extra": {
            "examples": [
                {
                    "event_id": "550e8400-e29b-41d4-a716-446655440000",
                    "event_type": "order.created",
                    "source": "order-service",
                    "payload": {
                        "order_id": "ORD-001",
                        "user_id": "USR-123",
                        "amount": 99.99,
                        "currency": "USD",
                    },
                }
            ]
        }
    }

    @field_validator("event_type")
    @classmethod
    def event_type_must_be_dotted(cls, value: str) -> str:
        if "." not in value:
            raise ValueError("event_type must be in '<domain>.<action>' format, e.g. 'order.created'")
        return value

    @field_validator("payload")
    @classmethod
    def payload_must_not_be_oversized(cls, value: dict[str, Any]) -> dict[str, Any]:
        size = len(json.dumps(value))
        if size > MAX_PAYLOAD_BYTES:
            raise ValueError(f"payload is {size} bytes, exceeding the {MAX_PAYLOAD_BYTES} byte limit")
        return value


class EventAccepted(BaseModel):
    event_id: UUID
    status: str = "accepted"

    model_config = {
        "json_schema_extra": {
            "examples": [
                {"event_id": "550e8400-e29b-41d4-a716-446655440000", "status": "accepted"}
            ]
        }
    }


class EventResponse(BaseModel):
    id: int
    event_id: UUID
    event_type: str
    source: str
    payload: dict[str, Any]
    status: str
    created_at: datetime
    processed_at: datetime | None = None

    model_config = {"from_attributes": True}
