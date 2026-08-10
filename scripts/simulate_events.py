#!/usr/bin/env python3
"""Post synthetic events to the ingestion API for manual smoke-testing.

Examples:

    # send 10 assorted events
    python scripts/simulate_events.py --count 10

    # send 5 of one type
    python scripts/simulate_events.py --count 5 --event-type order.created

    # send the same event_id twice to watch the idempotency path return 409
    python scripts/simulate_events.py --duplicate
"""

import argparse
import random
import sys
import uuid

import httpx

EVENT_TYPES = ["order.created", "user.clicked", "cart.updated"]


def build_payload(event_type: str) -> dict:
    if event_type == "order.created":
        return {
            "order_id": f"ORD-{random.randint(1000, 999999)}",
            "user_id": f"USR-{random.randint(1, 50000)}",
            "amount": round(random.uniform(5.0, 2500.0), 2),
            "currency": random.choice(["USD", "EUR", "GBP", "PKR"]),
        }
    if event_type == "user.clicked":
        return {
            "user_id": f"USR-{random.randint(1, 50000)}",
            "page": random.choice(["/", "/search", "/product", "/cart"]),
            "session_id": str(uuid.uuid4()),
        }
    return {
        "user_id": f"USR-{random.randint(1, 50000)}",
        "sku": f"SKU-{random.randint(100, 9999)}",
        "quantity": random.randint(1, 5),
    }


def build_event(event_type: str, event_id: str | None = None) -> dict:
    return {
        "event_id": event_id or str(uuid.uuid4()),
        "event_type": event_type,
        "source": "simulate-events-script",
        "payload": build_payload(event_type),
    }


def post_event(client: httpx.Client, event: dict) -> int:
    response = client.post("/events", json=event, timeout=10.0)
    label = f"{event['event_type']:<14} {event['event_id']}"
    print(f"  {response.status_code}  {label}")
    return response.status_code


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Post synthetic events to the ingestion API.",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )
    parser.add_argument("--host", default="http://localhost:8000", help="API base URL")
    parser.add_argument("--count", type=int, default=5, help="number of events to send")
    parser.add_argument(
        "--event-type",
        choices=EVENT_TYPES,
        help="send only this event type (default: pick at random per event)",
    )
    parser.add_argument(
        "--duplicate",
        action="store_true",
        help="send one event twice to exercise the duplicate/409 path",
    )
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    args = parse_args(argv)

    with httpx.Client(base_url=args.host) as client:
        if args.duplicate:
            event = build_event(args.event_type or EVENT_TYPES[0])
            print(f"Sending event {event['event_id']} twice:")
            post_event(client, event)
            second_status = post_event(client, event)
            # The duplicate is only rejected once the consumer has persisted
            # the first copy — until then the pre-publish check finds nothing.
            if second_status == 409:
                print("\nDuplicate correctly rejected with 409.")
            else:
                print(
                    f"\nSecond send returned {second_status}, not 409 — the consumer "
                    "may not have persisted the first copy yet. Retry in a moment."
                )
            return 0

        print(f"Sending {args.count} event(s) to {args.host}:")
        accepted = 0
        for _ in range(args.count):
            event_type = args.event_type or random.choice(EVENT_TYPES)
            if post_event(client, build_event(event_type)) == 202:
                accepted += 1

        print(f"\n{accepted}/{args.count} accepted.")
        return 0 if accepted == args.count else 1


if __name__ == "__main__":
    try:
        sys.exit(main())
    except httpx.ConnectError:
        print(
            "Could not reach the API. Is the stack running? (docker-compose up -d)",
            file=sys.stderr,
        )
        sys.exit(2)
