"""Initial schema: events and dlq_events

Reproduces exactly what migrations/001_*.sql and 002_*.sql used to create on
first container init, so an existing database migrated by those scripts is
indistinguishable from one built by this revision.

Revision ID: 0001
Revises:
Create Date: 2026-08-11
"""

from collections.abc import Sequence

import sqlalchemy as sa
from sqlalchemy.dialects import postgresql

from alembic import op

revision: str = "0001"
down_revision: str | None = None
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    op.create_table(
        "events",
        sa.Column("id", sa.BigInteger(), autoincrement=True, nullable=False),
        # The idempotency key. UNIQUE here is what actually makes redelivery
        # safe — everything else about duplicate handling is an optimisation.
        sa.Column("event_id", postgresql.UUID(as_uuid=True), nullable=False),
        sa.Column("event_type", sa.String(length=50), nullable=False),
        sa.Column("payload", postgresql.JSONB(astext_type=sa.Text()), nullable=False),
        sa.Column("source", sa.String(length=50), nullable=False),
        sa.Column(
            "status", sa.String(length=20), server_default="processed", nullable=False
        ),
        sa.Column(
            "created_at",
            sa.DateTime(timezone=True),
            server_default=sa.text("NOW()"),
            nullable=False,
        ),
        sa.Column("processed_at", sa.DateTime(timezone=True), nullable=True),
        sa.PrimaryKeyConstraint("id"),
        sa.UniqueConstraint("event_id"),
    )
    # These three cover the documented access patterns: filter by type, filter
    # by source, and recent-events-first time ranges.
    op.create_index("idx_events_event_type", "events", ["event_type"])
    op.create_index("idx_events_source", "events", ["source"])
    op.create_index(
        "idx_events_created_at", "events", [sa.text("created_at DESC")]
    )

    op.create_table(
        "dlq_events",
        sa.Column("id", sa.BigInteger(), autoincrement=True, nullable=False),
        # Nullable: a payload too malformed to parse has no usable event_id,
        # type or source, and losing the row entirely would defeat the DLQ.
        sa.Column("event_id", postgresql.UUID(as_uuid=True), nullable=True),
        sa.Column("event_type", sa.String(length=50), nullable=True),
        sa.Column("raw_payload", postgresql.JSONB(astext_type=sa.Text()), nullable=False),
        sa.Column("source", sa.String(length=50), nullable=True),
        sa.Column("error_reason", sa.Text(), nullable=False),
        sa.Column(
            "failed_at",
            sa.DateTime(timezone=True),
            server_default=sa.text("NOW()"),
            nullable=False,
        ),
        sa.PrimaryKeyConstraint("id"),
    )
    op.create_index(
        "idx_dlq_events_failed_at", "dlq_events", [sa.text("failed_at DESC")]
    )


def downgrade() -> None:
    op.drop_index("idx_dlq_events_failed_at", table_name="dlq_events")
    op.drop_table("dlq_events")
    op.drop_index("idx_events_created_at", table_name="events")
    op.drop_index("idx_events_source", table_name="events")
    op.drop_index("idx_events_event_type", table_name="events")
    op.drop_table("events")
