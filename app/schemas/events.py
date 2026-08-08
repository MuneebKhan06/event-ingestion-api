from datetime import datetime
from typing import Any
from uuid import UUID

from pydantic import BaseModel, Field, field_validator


class EventCreate(BaseModel):
    event_id: UUID
    event_type: str = Field(min_length=1, max_length=50)
    source: str = Field(min_length=1, max_length=50)
    payload: dict[str, Any]

    @field_validator("event_type")
    @classmethod
    def event_type_must_be_dotted(cls, value: str) -> str:
        if "." not in value:
            raise ValueError("event_type must be in '<domain>.<action>' format, e.g. 'order.created'")
        return value


class EventAccepted(BaseModel):
    event_id: UUID
    status: str = "accepted"


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
