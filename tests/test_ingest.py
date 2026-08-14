import asyncio

import pytest

from app.schemas.events import EventCreate
from ingest.main import poll_once
from ingest.sources import SOURCES, resolve, stable_event_id

USGS_BODY = {
    "features": [
        {
            "id": "us7000abcd",
            "properties": {"place": "10km N of Somewhere", "mag": 4.2, "time": 1760000000000},
            "geometry": {"coordinates": [12.3, 45.6, 10.0]},
        },
        {"id": None, "properties": {}},  # skipped: no upstream identifier
    ]
}

GITHUB_BODY = [
    {
        "id": "999",
        "type": "PushEvent",
        "repo": {"name": "octocat/hello"},
        "actor": {"login": "octocat"},
        "created_at": "2026-08-14T00:00:00Z",
    }
]

WEATHER_BODY = {
    "latitude": 33.68,
    "longitude": 73.04,
    "current": {"time": "2026-08-14T09:00", "temperature_2m": 31.4},
}


def test_ids_are_stable_across_polls():
    """The same upstream record must always map to the same event_id.

    This is what makes re-polling produce genuine duplicates instead of
    endlessly inserting new rows for unchanged data.
    """
    assert stable_event_id("usgs", "us7000abcd") == stable_event_id("usgs", "us7000abcd")


def test_ids_differ_across_sources():
    """Two feeds could reuse the same native id without meaning the same thing."""
    assert stable_event_id("usgs", "1") != stable_event_id("github", "1")


@pytest.mark.parametrize(
    ("source_name", "body"),
    [("usgs", USGS_BODY), ("github", GITHUB_BODY), ("weather", WEATHER_BODY)],
)
def test_parsed_events_pass_the_apis_own_validation(source_name, body):
    """Every parser must emit events the API would actually accept.

    Catches drift such as GitHub's "PushEvent" not being a dotted event_type,
    which the API rejects with 422.
    """
    events = SOURCES[source_name].parse(body)
    assert events
    for event in events:
        EventCreate(**event)


def test_records_without_an_upstream_id_are_skipped():
    events = SOURCES["usgs"].parse(USGS_BODY)
    assert len(events) == 1  # the id-less feature is dropped


def test_github_types_are_normalised_to_dotted_form():
    assert SOURCES["github"].parse(GITHUB_BODY)[0]["event_type"] == "github.push"


def test_resolve_rejects_unknown_sources():
    with pytest.raises(ValueError, match="Unknown source"):
        resolve("usgs,nope")


def test_resolve_ignores_blank_entries():
    assert [s.name for s in resolve("usgs, ,weather")] == ["usgs", "weather"]


class _FakeResponse:
    def __init__(self, status_code=200, json_body=None):
        self.status_code = status_code
        self._json = json_body

    def json(self):
        return self._json

    def raise_for_status(self):
        if self.status_code >= 400:
            raise RuntimeError(f"HTTP {self.status_code}")


class _FakeClient:
    def __init__(self, get_response=None, post_statuses=None, get_error=None):
        self._get_response = get_response
        self._get_error = get_error
        self._post_statuses = list(post_statuses or [])
        self.posts = 0

    async def get(self, _url, **_kwargs):
        if self._get_error:
            raise self._get_error
        return self._get_response

    async def post(self, _url, **_kwargs):
        self.posts += 1
        status = self._post_statuses.pop(0) if self._post_statuses else 202
        return _FakeResponse(status_code=status)


async def test_duplicates_are_counted_not_treated_as_errors():
    """Re-sending unchanged records is the expected steady state."""
    client = _FakeClient(get_response=_FakeResponse(json_body=USGS_BODY), post_statuses=[409])

    tally = await poll_once(client, SOURCES["usgs"], "http://api")

    assert tally == {"fetched": 1, "accepted": 0, "duplicate": 1, "rejected": 0, "failed": 0}


async def test_an_upstream_outage_does_not_stop_the_poller():
    """One failing feed must not take ingestion down for the others."""
    client = _FakeClient(get_error=RuntimeError("upstream down"))

    tally = await poll_once(client, SOURCES["usgs"], "http://api")

    assert tally["failed"] == 1
    assert client.posts == 0


async def test_unexpected_status_is_recorded_separately_from_success():
    client = _FakeClient(get_response=_FakeResponse(json_body=USGS_BODY), post_statuses=[422])

    tally = await poll_once(client, SOURCES["usgs"], "http://api")

    assert tally["rejected"] == 1
    assert tally["accepted"] == 0


# --------------------------------------------------------------------------
# Metrics
# --------------------------------------------------------------------------


def _counter(metric, source: str) -> float:
    return metric.labels(source=source)._value.get()


async def test_accepted_and_fetched_counters_move_together():
    from ingest import metrics

    client = _FakeClient(get_response=_FakeResponse(json_body=USGS_BODY), post_statuses=[202])
    before_fetched = _counter(metrics.events_fetched, "usgs")
    before_accepted = _counter(metrics.events_accepted, "usgs")

    await poll_once(client, SOURCES["usgs"], "http://api")

    assert _counter(metrics.events_fetched, "usgs") == before_fetched + 1
    assert _counter(metrics.events_accepted, "usgs") == before_accepted + 1


async def test_duplicates_are_counted_apart_from_failures():
    """Duplicates are the steady state here, so they must not read as errors."""
    from ingest import metrics

    client = _FakeClient(get_response=_FakeResponse(json_body=USGS_BODY), post_statuses=[409])
    before_dupe = _counter(metrics.events_duplicate, "usgs")
    before_failed = _counter(metrics.failures, "usgs")

    await poll_once(client, SOURCES["usgs"], "http://api")

    assert _counter(metrics.events_duplicate, "usgs") == before_dupe + 1
    assert _counter(metrics.failures, "usgs") == before_failed


async def test_fetch_failure_is_counted_even_though_it_returns_early():
    """The early return path must not skip recording."""
    from ingest import metrics

    client = _FakeClient(get_error=RuntimeError("upstream down"))
    before = _counter(metrics.failures, "weather")

    await poll_once(client, SOURCES["weather"], "http://api")

    assert _counter(metrics.failures, "weather") == before + 1


async def test_counters_are_scoped_per_source():
    from ingest import metrics

    client = _FakeClient(get_response=_FakeResponse(json_body=USGS_BODY), post_statuses=[202])
    before_other = _counter(metrics.events_accepted, "github")

    await poll_once(client, SOURCES["usgs"], "http://api")

    assert _counter(metrics.events_accepted, "github") == before_other


def test_every_source_series_exists_before_any_poll():
    """Series that appear only on first use break alerts that reference them."""
    from ingest import metrics

    exposed = {
        sample.labels["source"]
        for metric in metrics.failures.collect()
        for sample in metric.samples
        if "source" in sample.labels
    }
    assert set(SOURCES) <= exposed


# --------------------------------------------------------------------------
# Bounded concurrent publishing
# --------------------------------------------------------------------------


class _ConcurrencyTrackingClient:
    """Records how many publishes overlap, to prove the bound is real."""

    def __init__(self, event_count: int, statuses=None, fail_at=None):
        self._body = {
            "features": [
                {
                    "id": f"id-{i}",
                    "properties": {"place": "x", "mag": 1.0, "time": 1},
                    "geometry": {"coordinates": [0, 0, 0]},
                }
                for i in range(event_count)
            ]
        }
        self._statuses = list(statuses or [])
        self._fail_at = fail_at
        self.in_flight = 0
        self.max_in_flight = 0
        self.posts = 0

    async def get(self, _url, **_kwargs):
        return _FakeResponse(json_body=self._body)

    async def post(self, _url, **_kwargs):
        self.in_flight += 1
        self.max_in_flight = max(self.max_in_flight, self.in_flight)
        try:
            # Yield control so overlapping requests actually interleave.
            await asyncio.sleep(0)
            index = self.posts
            self.posts += 1
            if self._fail_at is not None and index == self._fail_at:
                raise RuntimeError("publish blew up")
            status = self._statuses[index] if index < len(self._statuses) else 202
            return _FakeResponse(status_code=status)
        finally:
            self.in_flight -= 1


async def test_every_event_is_still_counted_under_concurrency():
    client = _ConcurrencyTrackingClient(event_count=25)

    tally = await poll_once(client, SOURCES["usgs"], "http://api")

    assert tally["fetched"] == 25
    assert tally["accepted"] == 25
    counted = tally["accepted"] + tally["duplicate"] + tally["rejected"] + tally["failed"]
    assert counted == tally["fetched"]


async def test_concurrency_is_bounded():
    """Unbounded gather would open one connection per record."""
    from app.config import get_settings

    limit = get_settings().ingest_publish_concurrency
    client = _ConcurrencyTrackingClient(event_count=40)

    await poll_once(client, SOURCES["usgs"], "http://api")

    assert client.max_in_flight <= limit
    assert client.posts == 40


async def test_one_failing_publish_does_not_lose_the_rest():
    """Without return_exceptions a single raise cancels the whole batch."""
    client = _ConcurrencyTrackingClient(event_count=10, fail_at=3)

    tally = await poll_once(client, SOURCES["usgs"], "http://api")

    assert tally["failed"] == 1
    assert tally["accepted"] == 9
    assert client.posts == 10  # every event was still attempted


async def test_mixed_outcomes_are_tallied_correctly():
    client = _ConcurrencyTrackingClient(
        event_count=6, statuses=[202, 409, 409, 202, 422, 202]
    )

    tally = await poll_once(client, SOURCES["usgs"], "http://api")

    assert tally["accepted"] == 3
    assert tally["duplicate"] == 2
    assert tally["rejected"] == 1


async def test_an_error_outside_the_inner_handler_does_not_abort_the_batch(monkeypatch):
    """gather must collect exceptions rather than propagate the first one.

    publish_one already catches Exception around the request itself, so a
    failing request alone never reaches gather. This drives a failure from
    outside that handler (semaphore acquisition) to prove return_exceptions is
    doing real work and poll_once still returns a complete tally.
    """

    class _ExplodingSemaphore:
        def __init__(self, _limit):
            pass

        async def __aenter__(self):
            raise RuntimeError("semaphore unavailable")

        async def __aexit__(self, *_exc_info):
            return False

    monkeypatch.setattr(asyncio, "Semaphore", _ExplodingSemaphore)
    client = _ConcurrencyTrackingClient(event_count=5)

    tally = await poll_once(client, SOURCES["usgs"], "http://api")

    # Every event accounted for as failed, and no exception escaped.
    assert tally["fetched"] == 5
    assert tally["failed"] == 5
