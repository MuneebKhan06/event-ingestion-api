"""Prometheus counters for the ingest process.

Its own module, like consumer/metrics.py, so this process does not advertise
counters it never increments. A zero that means "not applicable here" is
misleading in a way that no metric at all is not.

Labelled by source, which is safe because source names come from the fixed
SOURCES dict in this repository rather than from anything upstream sends. The
same reasoning is why the API's counters are unlabelled: event_type there is
client supplied and would let a caller create unbounded series.

Duplicates get their own counter rather than being folded into failures. For
this poller they are the normal steady state, since feeds repeat unchanged
records between polls, so counting them as errors would make healthy operation
look broken and hide the rate that actually matters.
"""

from prometheus_client import Counter

from ingest.sources import SOURCES

events_fetched = Counter(
    "ingest_events_fetched_total",
    "Records parsed out of an upstream response.",
    ["source"],
)

events_accepted = Counter(
    "ingest_events_accepted_total",
    "Events the API accepted (HTTP 202).",
    ["source"],
)

events_duplicate = Counter(
    "ingest_events_duplicate_total",
    "Events the API rejected as already seen (HTTP 409).",
    ["source"],
)

events_rejected = Counter(
    "ingest_events_rejected_total",
    "Events the API refused for any other reason.",
    ["source"],
)

failures = Counter(
    "ingest_failures_total",
    "Upstream fetches or publishes that raised.",
    ["source"],
)

_ALL = (events_fetched, events_accepted, events_duplicate, events_rejected, failures)

# Every series exists at zero from startup. A counter that springs into being on
# first failure breaks the rate() and alert expressions that reference it,
# precisely when they are needed.
for _counter in _ALL:
    for _source in SOURCES:
        _counter.labels(source=_source)


def record(source: str, tally: dict[str, int]) -> None:
    """Apply one poll's tally. Takes the mapping poll_once already builds."""
    events_fetched.labels(source=source).inc(tally.get("fetched", 0))
    events_accepted.labels(source=source).inc(tally.get("accepted", 0))
    events_duplicate.labels(source=source).inc(tally.get("duplicate", 0))
    events_rejected.labels(source=source).inc(tally.get("rejected", 0))
    failures.labels(source=source).inc(tally.get("failed", 0))
