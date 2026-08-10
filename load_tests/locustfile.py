"""Locust load test for the ingestion API.

Run against a live stack (docker-compose up -d) with:

    locust -f load_tests/locustfile.py --host=http://localhost:8000

then open http://localhost:8089 and drive it at 10 / 50 / 100 / 200 users to
fill in the load-test table in the README.
"""

import random
import uuid

from locust import HttpUser, between, task

CURRENCIES = ["USD", "EUR", "GBP", "PKR"]


def _order_created_payload() -> dict:
    return {
        "order_id": f"ORD-{random.randint(1000, 999999)}",
        "user_id": f"USR-{random.randint(1, 50000)}",
        "amount": round(random.uniform(5.0, 2500.0), 2),
        "currency": random.choice(CURRENCIES),
        "items": random.randint(1, 8),
    }


def _user_clicked_payload() -> dict:
    return {
        "user_id": f"USR-{random.randint(1, 50000)}",
        "page": random.choice(["/", "/search", "/product", "/cart", "/checkout"]),
        "session_id": str(uuid.uuid4()),
    }


def _cart_updated_payload() -> dict:
    return {
        "user_id": f"USR-{random.randint(1, 50000)}",
        "sku": f"SKU-{random.randint(100, 9999)}",
        "quantity": random.randint(1, 5),
        "action": random.choice(["add", "remove", "update"]),
    }


EVENT_BUILDERS = {
    "order.created": _order_created_payload,
    "user.clicked": _user_clicked_payload,
    "cart.updated": _cart_updated_payload,
}


class EventIngestionUser(HttpUser):
    wait_time = between(0.1, 0.5)

    def on_start(self) -> None:
        # event_ids this user has successfully published, so the lookup task
        # queries IDs that actually exist rather than random misses.
        self.published_ids: list[str] = []

    def _publish(self, event_type: str) -> None:
        event_id = str(uuid.uuid4())
        response = self.client.post(
            "/events",
            json={
                "event_id": event_id,
                "event_type": event_type,
                "source": "load-test",
                "payload": EVENT_BUILDERS[event_type](),
            },
            name=f"POST /events [{event_type}]",
        )
        if response.status_code == 202:
            self.published_ids.append(event_id)
            # Unbounded growth would leak memory over a long run; the tail is
            # the interesting part anyway (most recently published).
            if len(self.published_ids) > 100:
                del self.published_ids[:50]

    @task(10)
    def publish_order_created(self) -> None:
        self._publish("order.created")

    @task(6)
    def publish_user_clicked(self) -> None:
        self._publish("user.clicked")

    @task(4)
    def publish_cart_updated(self) -> None:
        self._publish("cart.updated")

    @task(1)
    def lookup_event(self) -> None:
        """Read back a previously published event.

        A 404 here is an expected outcome, not a failure: ingestion is
        asynchronous, so an event published moments ago may not have been
        consumed into Postgres yet. Counting those as errors would drown the
        failure rate in noise and hide real problems.
        """
        if not self.published_ids:
            return

        event_id = random.choice(self.published_ids)
        with self.client.get(
            f"/events/{event_id}", name="GET /events/{id}", catch_response=True
        ) as response:
            if response.status_code in (200, 404):
                response.success()
            else:
                response.failure(f"Unexpected status {response.status_code}")
