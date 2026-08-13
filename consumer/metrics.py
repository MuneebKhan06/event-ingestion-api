"""Prometheus counters for the consumer process.

Kept separate from app/core/metrics.py rather than sharing one module. Both
processes would otherwise define every counter, and the consumer's endpoint
would advertise `events_accepted_total 0` — which reads as "no events were
accepted" when it actually means "this process doesn't do that". A zero that
means "not applicable" is worse than no metric at all.

Note `dlq_writes_total` *is* labelled, unlike the API's counters. The
distinction is who controls the values: `reason` comes from a fixed set defined
here, whereas labelling on `event_type` would let a caller invent unbounded
label values. Labels are fine when the value space is ours.
"""

from prometheus_client import Counter

events_persisted = Counter(
    "events_persisted_total",
    "Events inserted into PostgreSQL by the consumer.",
)

events_duplicates_skipped = Counter(
    "events_duplicates_skipped_total",
    "Redeliveries dropped by the unique constraint (ON CONFLICT DO NOTHING).",
)

dlq_writes = Counter(
    "dlq_writes_total",
    "Events routed to the dead letter queue.",
    ["reason"],
)

# The full set of reasons, declared up front so each series exists at zero
# rather than appearing only on first failure — a counter that springs into
# being mid-incident breaks rate() and alert expressions that reference it.
DLQ_REASON_MISSING_FIELDS = "missing_fields"
DLQ_REASON_INVALID_EVENT_ID = "invalid_event_id"
DLQ_REASON_RETRY_EXHAUSTED = "retry_exhausted"

for _reason in (
    DLQ_REASON_MISSING_FIELDS,
    DLQ_REASON_INVALID_EVENT_ID,
    DLQ_REASON_RETRY_EXHAUSTED,
):
    dlq_writes.labels(reason=_reason)
