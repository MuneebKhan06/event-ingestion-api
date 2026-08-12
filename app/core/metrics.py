"""Prometheus counters for the API process.

Deliberately unlabelled. The obvious labels here would be `event_type` and
`source`, but both come straight from the request body — `event_type` is any
dotted string up to 50 characters. Prometheus creates a separate time series
per label combination, so labelling on client-supplied values lets any caller,
buggy or hostile, mint unbounded series and take down the monitoring system.
Breaking these down by type belongs in a query over the events table, not in a
metric.

Scope: these count what *this* process does. The API and the consumer run
separately, so events persisted, duplicates skipped at insert, and DLQ writes
all happen in the consumer and are not visible here. Exposing those would mean
giving the consumer its own exposition endpoint.
"""

from prometheus_client import Counter

events_accepted = Counter(
    "events_accepted_total",
    "Events validated and published to Kafka (HTTP 202).",
)

events_duplicates = Counter(
    "events_duplicates_total",
    "Events rejected by the pre-publish duplicate check (HTTP 409).",
)
