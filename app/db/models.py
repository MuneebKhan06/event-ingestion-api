import uuid
from datetime import datetime

from sqlalchemy import BigInteger, DateTime, String, Text, func
from sqlalchemy.dialects.postgresql import JSONB, UUID
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column


class Base(DeclarativeBase):
    pass


# Timestamps are spelled out as timezone-aware rather than left to inference.
# A bare `Mapped[datetime]` maps to TIMESTAMP WITHOUT TIME ZONE, which is not
# what the schema actually has — the tables were created with TIMESTAMPTZ. The
# mismatch was invisible while the schema came from hand-written SQL, but any
# tool generating DDL from these models (Alembic autogenerate, create_all)
# would have produced naive columns that silently disagree with production.
_TIMESTAMPTZ = DateTime(timezone=True)


class Event(Base):
    __tablename__ = "events"

    id: Mapped[int] = mapped_column(BigInteger, primary_key=True, autoincrement=True)
    event_id: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True), nullable=False, unique=True)
    event_type: Mapped[str] = mapped_column(String(50), nullable=False)
    payload: Mapped[dict] = mapped_column(JSONB, nullable=False)
    source: Mapped[str] = mapped_column(String(50), nullable=False)
    status: Mapped[str] = mapped_column(String(20), nullable=False, default="processed")
    created_at: Mapped[datetime] = mapped_column(
        _TIMESTAMPTZ, server_default=func.now(), nullable=False
    )
    processed_at: Mapped[datetime | None] = mapped_column(_TIMESTAMPTZ, nullable=True)


class DLQEvent(Base):
    __tablename__ = "dlq_events"

    id: Mapped[int] = mapped_column(BigInteger, primary_key=True, autoincrement=True)
    event_id: Mapped[uuid.UUID | None] = mapped_column(UUID(as_uuid=True), nullable=True)
    event_type: Mapped[str | None] = mapped_column(String(50), nullable=True)
    raw_payload: Mapped[dict] = mapped_column(JSONB, nullable=False)
    source: Mapped[str | None] = mapped_column(String(50), nullable=True)
    error_reason: Mapped[str] = mapped_column(Text, nullable=False)
    failed_at: Mapped[datetime] = mapped_column(
        _TIMESTAMPTZ, server_default=func.now(), nullable=False
    )
