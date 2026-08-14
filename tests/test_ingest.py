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
