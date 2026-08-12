#!/usr/bin/env python3
"""Replay failed events from the DLQ back onto the ingestion topic.

Decision 6 keeps the full original payload in `dlq_events` precisely so that a
batch of failures can be reprocessed once the underlying bug is fixed. This is
that step.

Events are re-published to `events.raw`, so the normal consumer picks them up
and the usual idempotency rules apply — an event that did eventually land in
Postgres will be deduplicated on insert rather than duplicated.

Examples:

    # see what would be replayed, without publishing anything
    python scripts/replay_dlq.py --dry-run

    # replay the first 50 failures for one event type
    python scripts/replay_dlq.py --event-type order.created --limit 50
"""

import argparse
import asyncio
import sys
from pathlib import Path

# Running this file directly puts scripts/ on sys.path rather than the repo
# root, so `app` would not be importable without adding it here.
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from pydantic import ValidationError  # noqa: E402

from app.db.connection import async_session_factory, dispose_engine  # noqa: E402
from app.db.repository import EventRepository  # noqa: E402
from app.kafka.producer import EventProducer  # noqa: E402
from app.kafka.topics import EVENTS_RAW  # noqa: E402
from app.schemas.events import EventCreate  # noqa: E402


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Replay failed events from the DLQ back onto events.raw.",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )
    parser.add_argument("--limit", type=int, default=100, help="maximum events to replay")
    parser.add_argument("--event-type", help="only replay this event type")
    parser.add_argument("--source", help="only replay events from this source")
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="list what would be replayed without publishing anything",
    )
    return parser.parse_args(argv)


def classify(raw_payload: object) -> tuple[EventCreate | None, str | None]:
    """Decide whether a stored payload is worth republishing.

    A payload that fails validation is why it landed in the DLQ in the first
    place — republishing it would fail again and land straight back. Those are
    reported instead, so a bad batch is visible rather than silently looping.
    """
    if not isinstance(raw_payload, dict):
        return None, f"raw_payload is {type(raw_payload).__name__}, not an object"
    try:
        return EventCreate(**raw_payload), None
    except ValidationError as exc:
        first = exc.errors()[0]
        location = ".".join(str(part) for part in first["loc"]) or "payload"
        return None, f"{location}: {first['msg']}"
    except TypeError as exc:
        return None, str(exc)


async def replay(args: argparse.Namespace) -> int:
    async with async_session_factory() as session:
        repository = EventRepository(session)
        # Oldest-first (the default) so events are reprocessed in roughly the
        # order they originally failed.
        dlq_events, _total = await repository.list_dlq_events(
            event_type=args.event_type, source=args.source, limit=args.limit
        )

    if not dlq_events:
        print("No DLQ events matched.")
        return 0

    replayable: list[tuple[int, EventCreate]] = []
    skipped: list[tuple[int, str]] = []
    for dlq_event in dlq_events:
        event, reason = classify(dlq_event.raw_payload)
        if event is None:
            skipped.append((dlq_event.id, reason or "unknown"))
        else:
            replayable.append((dlq_event.id, event))

    verb = "Would replay" if args.dry_run else "Replaying"
    print(f"{verb} {len(replayable)} of {len(dlq_events)} DLQ event(s):")
    for dlq_id, event in replayable:
        print(f"  dlq#{dlq_id:<6} {event.event_type:<16} {event.event_id}")

    if skipped:
        print(f"\nSkipping {len(skipped)} unreplayable event(s):")
        for dlq_id, reason in skipped:
            print(f"  dlq#{dlq_id:<6} {reason}")

    if args.dry_run:
        print("\nDry run — nothing published.")
        return 0

    if not replayable:
        return 1

    producer = EventProducer()
    await producer.start()
    try:
        for _dlq_id, event in replayable:
            await producer.send_event(
                EVENTS_RAW,
                event.event_id,
                {
                    "event_id": str(event.event_id),
                    "event_type": event.event_type,
                    "source": event.source,
                    "payload": event.payload,
                },
            )
    finally:
        await producer.stop()

    print(f"\nRepublished {len(replayable)} event(s) to {EVENTS_RAW}.")
    return 0


async def main(argv: list[str] | None = None) -> int:
    args = parse_args(argv)
    try:
        return await replay(args)
    finally:
        await dispose_engine()


if __name__ == "__main__":
    sys.exit(asyncio.run(main()))
