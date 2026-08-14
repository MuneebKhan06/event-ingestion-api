"""Polls public APIs and feeds the ingestion API with real traffic.

A third process alongside the API and consumer. It deliberately talks HTTP to
the API rather than publishing to Kafka directly, so the events it produces go
through exactly the same validation and idempotency path a real client's would.
"""

import asyncio
import logging
import signal
import sys
from pathlib import Path

import httpx
from prometheus_client import start_http_server

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from app.config import get_settings  # noqa: E402
from ingest.metrics import record  # noqa: E402
from ingest.sources import Source, resolve  # noqa: E402

logger = logging.getLogger("ingest")


async def fetch_events(client: httpx.AsyncClient, source: Source) -> list[dict]:
    response = await client.get(source.url, timeout=20.0)
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
    except Exception as exc:
        logger.warning("Fetch failed for %s: %s", source.name, exc)
        tally["failed"] += 1
        record(source.name, tally)
        return tally

    tally["fetched"] = len(events)
    for event in events:
        try:
            status = await publish(client, api_url, event)
        except Exception as exc:
            logger.warning("Publish failed for %s: %s", source.name, exc)
            tally["failed"] += 1
            continue

        if status == 202:
            tally["accepted"] += 1
        elif status == 409:
            # Expected: the feed repeats records between polls.
            tally["duplicate"] += 1
        else:
            tally["rejected"] += 1
            logger.warning("Unexpected %s for %s event", status, source.name)

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

    async with httpx.AsyncClient() as client:
        while not shutdown.is_set():
            for source in sources:
                if shutdown.is_set():
                    break
                await poll_once(client, source, api_url)

            # Waiting on the shutdown event rather than sleeping means SIGTERM
            # is honoured immediately instead of after the full interval.
            try:
                await asyncio.wait_for(shutdown.wait(), timeout=interval)
            except asyncio.TimeoutError:
                pass

    logger.info("Ingest stopped")


if __name__ == "__main__":
    asyncio.run(run())
