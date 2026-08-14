"""Per source backoff, so a failing upstream is left alone for a while.

Without this the poller retries every interval regardless. An unauthenticated
GitHub quota is roughly 60 requests an hour, so a 30 second interval would keep
hitting a source that has already refused, about 120 times an hour, achieving
nothing and being a poor client.

State is deliberately per source: one upstream having a bad day must not pause
the others.
"""

import logging
import time
from dataclasses import dataclass, field

logger = logging.getLogger("ingest")

# Skip 2 polls, then 4, 8, 16, up to the cap. Doubling gets out of an
# upstream's way quickly; the cap stops a long outage turning into an
# effectively permanent stop once it recovers.
FIRST_SKIP_POLLS = 2
MAX_SKIP_POLLS = 32


@dataclass
class SourceState:
    consecutive_failures: int = 0
    # Polls still to skip before trying again.
    skips_remaining: int = 0
    # Wall clock deadline from a Retry-After header, if the upstream gave one.
    retry_not_before: float = 0.0
    last_reason: str = ""


@dataclass
class BackoffRegistry:
    """Tracks each source independently."""

    states: dict[str, SourceState] = field(default_factory=dict)
    # Injectable so tests do not have to sleep in real time.
    now: callable = time.monotonic

    def _state(self, source: str) -> SourceState:
        return self.states.setdefault(source, SourceState())

    def should_skip(self, source: str) -> bool:
        state = self._state(source)

        if state.retry_not_before and self.now() < state.retry_not_before:
            return True

        if state.skips_remaining > 0:
            state.skips_remaining -= 1
            return True

        return False

    def record_success(self, source: str) -> None:
        """Any success clears the penalty entirely.

        Halving or decaying instead would keep punishing a source that has
        demonstrably recovered.
        """
        state = self._state(source)
        if state.consecutive_failures:
            logger.info("%s recovered after %d failures", source, state.consecutive_failures)
        state.consecutive_failures = 0
        state.skips_remaining = 0
        state.retry_not_before = 0.0
        state.last_reason = ""

    def record_failure(self, source: str, reason: str, retry_after: float | None = None) -> None:
        state = self._state(source)
        state.consecutive_failures += 1
        state.last_reason = reason

        if retry_after is not None:
            # An explicit instruction from the upstream beats our guess.
            state.retry_not_before = self.now() + retry_after
            state.skips_remaining = 0
            logger.warning(
                "%s asked to wait %.0fs (%s); honouring Retry-After",
                source,
                retry_after,
                reason,
            )
            return

        skips = min(FIRST_SKIP_POLLS * (2 ** (state.consecutive_failures - 1)), MAX_SKIP_POLLS)
        state.skips_remaining = skips
        state.retry_not_before = 0.0
        logger.warning(
            "%s failed (%s), skipping the next %d poll(s)", source, reason, skips
        )
