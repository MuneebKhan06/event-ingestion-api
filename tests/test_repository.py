"""Session hygiene in EventRepository's write paths.

A failed statement aborts the Postgres transaction, and every later command on
that session is refused until a rollback. The consumer retries on the same
session and then writes the DLQ row on it too, so a write that fails without
rolling back turns one rejected row into a consumer that crash loops on the
same message forever. These pin the rollback down.
"""

from unittest.mock import AsyncMock, MagicMock
from uuid import uuid4

import pytest

from app.db.repository import EventRepository


class Rejected(Exception):
    """Stands in for whatever the driver raises, e.g. a value too long."""


@pytest.fixture
def session():
    session = MagicMock()
    session.execute = AsyncMock()
    session.commit = AsyncMock()
    session.rollback = AsyncMock()
    return session


async def test_failed_insert_rolls_back_and_reraises(session):
    session.execute.side_effect = Rejected("value too long for type character varying(50)")

    with pytest.raises(Rejected):
        await EventRepository(session).insert_event(
            event_id=uuid4(), event_type="a.b", source="test", payload={}
        )

    # Re-raised, not swallowed: the consumer's retry and DLQ routing decide
    # what happens next, and they need to see the real error to do it.
    session.rollback.assert_awaited_once()
    session.commit.assert_not_awaited()


async def test_failed_commit_on_insert_also_rolls_back(session):
    session.commit.side_effect = Rejected("commit failed")

    with pytest.raises(Rejected):
        await EventRepository(session).insert_event(
            event_id=uuid4(), event_type="a.b", source="test", payload={}
        )

    session.rollback.assert_awaited_once()


async def test_failed_dlq_write_rolls_back_and_reraises(session):
    session.commit.side_effect = Rejected("dlq insert failed")

    with pytest.raises(Rejected):
        await EventRepository(session).insert_dlq_event(raw_payload={}, error_reason="x")

    session.rollback.assert_awaited_once()


async def test_successful_insert_does_not_roll_back(session):
    session.execute.return_value.first.return_value = (1,)

    inserted = await EventRepository(session).insert_event(
        event_id=uuid4(), event_type="a.b", source="test", payload={}
    )

    assert inserted is True
    session.rollback.assert_not_awaited()
