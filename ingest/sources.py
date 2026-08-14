"""Public API sources for real traffic.

Every source derives its `event_id` deterministically from the upstream
record's own identifier via uuid5. That is the important design choice here:
polling the same feed repeatedly re-sends records that have not changed, so
duplicates arise naturally from real data rather than being simulated. The
unique constraint on `event_id` is what keeps the table correct, which makes
this a live exercise of Decision 4 instead of a demonstration of it.

All three APIs are public and unauthenticated.
"""

import uuid
from collections.abc import Callable
from dataclasses import dataclass
from typing import Any

# Stable namespace so an id generated today matches one generated next week.
_NAMESPACE = uuid.uuid5(uuid.NAMESPACE_URL, "event-ingestion-api/sources")


def stable_event_id(source: str, native_id: str) -> str:
    return str(uuid.uuid5(_NAMESPACE, f"{source}:{native_id}"))


def _parse_usgs(body: dict[str, Any]) -> list[dict[str, Any]]:
    events = []
    for feature in body.get("features", []):
        properties = feature.get("properties") or {}
        native_id = feature.get("id")
        if not native_id:
            continue
        events.append(
            {
                "event_id": stable_event_id("usgs", native_id),
                "event_type": "quake.detected",
                "source": "usgs",
                "payload": {
                    "usgs_id": native_id,
                    "place": properties.get("place"),
                    "magnitude": properties.get("mag"),
                    "time": properties.get("time"),
                    "coordinates": (feature.get("geometry") or {}).get("coordinates"),
                },
            }
        )
    return events


def _parse_github(body: list[dict[str, Any]]) -> list[dict[str, Any]]:
    events = []
    for item in body:
        native_id = item.get("id")
        if not native_id:
            continue
        # GitHub types are "PushEvent", "WatchEvent"; the API requires a dotted
        # "<domain>.<action>", so they are normalised rather than passed through.
        raw_type = item.get("type") or "Unknown"
        action = raw_type.removesuffix("Event").lower() or "unknown"
        events.append(
            {
                "event_id": stable_event_id("github", str(native_id)),
                "event_type": f"github.{action}",
                "source": "github",
                "payload": {
                    "github_id": native_id,
                    "repo": (item.get("repo") or {}).get("name"),
                    "actor": (item.get("actor") or {}).get("login"),
                    "created_at": item.get("created_at"),
                },
            }
        )
    return events


def _parse_open_meteo(body: dict[str, Any]) -> list[dict[str, Any]]:
    current = body.get("current") or {}
    timestamp = current.get("time")
    if not timestamp:
        return []
    return [
        {
            "event_id": stable_event_id("open-meteo", str(timestamp)),
            "event_type": "weather.sampled",
            "source": "open-meteo",
            "payload": {
                "observed_at": timestamp,
                "temperature_c": current.get("temperature_2m"),
                "latitude": body.get("latitude"),
                "longitude": body.get("longitude"),
            },
        }
    ]


@dataclass(frozen=True)
class Source:
    name: str
    url: str
    parse: Callable[[Any], list[dict[str, Any]]]


SOURCES: dict[str, Source] = {
    # Updates continuously and repeats unchanged records between polls, which
    # is exactly the duplicate behaviour worth exercising.
    "usgs": Source(
        name="usgs",
        url="https://earthquake.usgs.gov/earthquakes/feed/v1.0/summary/all_hour.geojson",
        parse=_parse_usgs,
    ),
    # Unauthenticated GitHub allows roughly 60 requests/hour, so keep the poll
    # interval well above one request per minute if this source is enabled.
    "github": Source(
        name="github",
        url="https://api.github.com/events",
        parse=_parse_github,
    ),
    "weather": Source(
        name="weather",
        url=(
            "https://api.open-meteo.com/v1/forecast"
            "?latitude=33.6844&longitude=73.0479&current=temperature_2m"
        ),
        parse=_parse_open_meteo,
    ),
}


def resolve(names: str) -> list[Source]:
    """Turn a comma separated list into Sources, rejecting unknown names."""
    resolved = []
    for raw in names.split(","):
        name = raw.strip()
        if not name:
            continue
        if name not in SOURCES:
            raise ValueError(f"Unknown source {name!r}; known: {', '.join(sorted(SOURCES))}")
        resolved.append(SOURCES[name])
    return resolved
