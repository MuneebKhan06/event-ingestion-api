"""Polls public APIs and feeds the ingestion API with real traffic.

A third process alongside the API and consumer. It deliberately talks HTTP to
the API rather than publishing to Kafka directly, so the events it produces go
through exactly the same validation and idempotency path a real client's would.
"""

import asyncio
import logging
import signal
import sys
import time
from pathlib import Path

import httpx
from prometheus_client import start_http_server

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from app.config import get_settings  # noqa: E402
from ingest.backoff import BackoffRegistry  # noqa: E402
from ingest.metrics import backoff_skips, record  # noqa: E402
from ingest.sources import Source, resolve  # noqa: E402

logger = logging.getLogger("ingest")


def rate_limit_delay(response: httpx.Response) -> float | None:
    """Seconds to wait if this response is a rate limit, else None.

    Recognises 429, and 403 with an exhausted X-RateLimit-Remaining, which is
    how GitHub refuses an over-quota unauthenticated caller rather than 429.

    Retry-After may also be an HTTP date. Only the integer seconds form is
    handled; a date falls through to the normal exponential backoff, which is
    a safe default rather than a wrong parse.
    """
    limited = response.status_code == 429 or (
        response.status_code == 403 and response.headers.get("X-RateLimit-Remaining") == "0"
    )
    if not limited:
        return None

    retry_after = response.headers.get("Retry-After", "")
    if retry_after.isdigit():
        return float(retry_after)

    reset = response.headers.get("X-RateLimit-Reset", "")
    if reset.isdigit():
        # Absolute epoch seconds; convert to a delay, floored at zero.
        return max(0.0, float(reset) - time.time())

    return 0.0


class RateLimited(Exception):
    def __init__(self, delay: float | None):
        self.delay = delay
        super().__init__(f"rate limited, retry after {delay}s")


async def fetch_events(client: httpx.AsyncClient, source: Source) -> list[dict]:
    response = await client.get(source.url, timeout=20.0)

    delay = rate_limit_delay(response)
    if delay is not None:
        raise RateLimited(delay)

    response.raise_for_status()
    return source.parse(response.json())


async def publish(client: httpx.AsyncClient, api_url: str, event: dict) -> int:
    response = await client.post(f"{api_url}/events", json=event, timeout=20.0)
    return response.status_code


async def poll_once(client: httpx.AsyncClient, source: Source, api_url: str) -> dict[str, int]:
    """Fetch one source and forward its events. Never raises.

    An upstream API being slow, rate limited or briefly broken is expected
    rather than exceptional, so it is logged and counted; letting it escape
    would take down ingestion for every other source too.
    """
    tally = {"fetched": 0, "accepted": 0, "duplicate": 0, "rejected": 0, "failed": 0}
    try:
        events = await fetch_events(client, source)
    except RateLimited as exc:
        logger.warning("Rate limited by %s", source.name)
        tally["failed"] += 1
        tally["retry_after"] = exc.delay
        record(source.name, tally)
        return tally
    except Exception as exc:
        logger.warning("Fetch failed for %s: %s", source.name, exc)
        tally["failed"] += 1
        record(source.name, tally)
        return tally

    tally["fetched"] = len(events)

    # Events are independent and Kafka partitions by event_id, so the order
    # they are published in carries no meaning. Sending them one at a time was
    # simply serialising a round trip per record: a 30 record GitHub poll cost
    # 30 sequential requests. Concurrency is bounded so a large feed cannot
    # open an unlimited number of connections against the API at once.
    semaphore = asyncio.Semaphore(get_settings().ingest_publish_concurrency)

    async def publish_one(event: dict) -> str:
        """Return this event's outcome rather than mutating shared state.

        Folding returned outcomes afterwards keeps the tally exact without a
        lock, and makes the counting obvious to read.
        """
        async with semaphore:
            try:
                status = await publish(client, api_url, event)
            except Exception as exc:
                logger.warning("Publish failed for %s: %s", source.name, exc)
                return "failed"

        if status == 202:
            return "accepted"
        if status == 409:
            # Expected: the feed repeats records between polls.
            return "duplicate"
        logger.warning("Unexpected %s for %s event", status, source.name)
        return "rejected"

    # return_exceptions so one unexpected error cannot cancel the rest of the
    # batch and silently drop events that would otherwise have been published.
    outcomes = await asyncio.gather(
        *(publish_one(event) for event in events), return_exceptions=True
    )
    for outcome in outcomes:
        if isinstance(outcome, BaseException):
            logger.warning("Publish task failed for %s: %s", source.name, outcome)
            tally["failed"] += 1
        else:
            tally[outcome] += 1

    record(source.name, tally)
    logger.info(
        "%s: fetched=%d accepted=%d duplicate=%d rejected=%d failed=%d",
        source.name,
        tally["fetched"],
        tally["accepted"],
        tally["duplicate"],
        tally["rejected"],
        tally["failed"],
    )
    return tally


async def run() -> None:
    settings = get_settings()
    logging.basicConfig(level=settings.log_level, format="%(levelname)s:%(name)s:%(message)s")

    sources = resolve(settings.ingest_sources)
    api_url = settings.ingest_api_url.rstrip("/")
    interval = settings.ingest_interval_seconds

    shutdown = asyncio.Event()
    loop = asyncio.get_running_loop()
    for sig in (signal.SIGTERM, signal.SIGINT):
        loop.add_signal_handler(sig, shutdown.set)

    # Bind failure is logged and swallowed: losing metrics should cost
    # observability, not the ingestion this process exists to do.
    try:
        start_http_server(settings.ingest_metrics_port)
        logger.info("Ingest metrics on :%d/metrics", settings.ingest_metrics_port)
    except OSError:
        logger.exception(
            "Could not bind metrics port %d; continuing without metrics",
            settings.ingest_metrics_port,
        )

    logger.info(
        "Ingest started (sources=%s, target=%s, interval=%ss)",
        ",".join(s.name for s in sources),
        api_url,
        interval,
    )

    backoff = BackoffRegistry()

    async with httpx.AsyncClient() as client:
        while not shutdown.is_set():
            for source in sources:
                if shutdown.is_set():
                    break

                if backoff.should_skip(source.name):
                    # Counted, not just skipped silently, so a source sitting
                    # in backoff cannot be mistaken for a healthy quiet one.
                    backoff_skips.labels(source=source.name).inc()
                    logger.info("Skipping %s while backing off", source.name)
                    continue

                tally = await poll_once(client, source, api_url)
                if tally["failed"]:
                    backoff.record_failure(
                        source.name,
                        reason="fetch failed",
                        retry_after=tally.get("retry_after"),
                    )
                else:
                    backoff.record_success(source.name)

            # Waiting on the shutdown event rather than sleeping means SIGTERM
            # is honoured immediately instead of after the full interval.
            try:
                await asyncio.wait_for(shutdown.wait(), timeout=interval)
            except asyncio.TimeoutError:
                pass

    logger.info("Ingest stopped")


if __name__ == "__main__":
    asyncio.run(run())
