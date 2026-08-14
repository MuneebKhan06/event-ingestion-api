import httpx
import pytest

from ingest.backoff import FIRST_SKIP_POLLS, MAX_SKIP_POLLS, BackoffRegistry
from ingest.main import RateLimited, fetch_events, rate_limit_delay
from ingest.sources import SOURCES


class _Clock:
    """Controllable time, so tests never sleep for real."""

    def __init__(self):
        self.value = 1000.0

    def __call__(self) -> float:
        return self.value

    def advance(self, seconds: float) -> None:
        self.value += seconds


def _registry() -> tuple[BackoffRegistry, _Clock]:
    clock = _Clock()
    return BackoffRegistry(now=clock), clock


def test_a_healthy_source_is_never_skipped():
    registry, _ = _registry()

    assert registry.should_skip("usgs") is False
    assert registry.should_skip("usgs") is False


def test_failure_skips_the_next_polls():
    registry, _ = _registry()
    registry.record_failure("usgs", "boom")

    skipped = [registry.should_skip("usgs") for _ in range(FIRST_SKIP_POLLS + 1)]

    assert skipped[:FIRST_SKIP_POLLS] == [True] * FIRST_SKIP_POLLS
    assert skipped[-1] is False  # penalty served, try again


def test_backoff_grows_with_consecutive_failures():
    registry, _ = _registry()

    registry.record_failure("usgs", "boom")
    first = registry.states["usgs"].skips_remaining
    registry.record_failure("usgs", "boom")
    second = registry.states["usgs"].skips_remaining

    assert second > first


def test_backoff_is_capped():
    """An outage must not turn into an effectively permanent stop."""
    registry, _ = _registry()

    for _ in range(20):
        registry.record_failure("usgs", "boom")

    assert registry.states["usgs"].skips_remaining == MAX_SKIP_POLLS


def test_success_clears_the_penalty_entirely():
    registry, _ = _registry()
    registry.record_failure("usgs", "boom")
    registry.record_failure("usgs", "boom")

    registry.record_success("usgs")

    assert registry.should_skip("usgs") is False
    assert registry.states["usgs"].consecutive_failures == 0


def test_retry_after_is_honoured_over_our_own_guess():
    registry, clock = _registry()
    registry.record_failure("usgs", "rate limited", retry_after=120.0)

    assert registry.should_skip("usgs") is True
    clock.advance(119)
    assert registry.should_skip("usgs") is True
    clock.advance(2)
    assert registry.should_skip("usgs") is False


def test_one_failing_source_does_not_pause_the_others():
    registry, _ = _registry()
    registry.record_failure("github", "rate limited")

    assert registry.should_skip("github") is True
    assert registry.should_skip("usgs") is False


# --------------------------------------------------------------------------
# Rate limit classification
# --------------------------------------------------------------------------


def _response(status_code: int, headers: dict[str, str] | None = None) -> httpx.Response:
    return httpx.Response(status_code, headers=headers or {}, request=httpx.Request("GET", "http://x"))


def test_429_is_rate_limiting():
    assert rate_limit_delay(_response(429, {"Retry-After": "45"})) == 45.0


def test_github_style_403_with_exhausted_quota_is_rate_limiting():
    """GitHub refuses an over-quota caller with 403, not 429."""
    delay = rate_limit_delay(_response(403, {"X-RateLimit-Remaining": "0"}))

    assert delay is not None


def test_a_plain_403_is_not_treated_as_rate_limiting():
    """Forbidden for other reasons should not be waited out silently."""
    assert rate_limit_delay(_response(403)) is None


def test_ordinary_success_is_not_rate_limiting():
    assert rate_limit_delay(_response(200)) is None


def test_http_date_retry_after_falls_back_to_normal_backoff():
    """Only the integer seconds form is parsed; a date must not be misread."""
    delay = rate_limit_delay(
        _response(429, {"Retry-After": "Wed, 21 Oct 2026 07:28:00 GMT"})
    )

    assert delay == 0.0  # no explicit delay, caller applies its own backoff


class _FakeClient:
    def __init__(self, response):
        self._response = response

    async def get(self, _url, **_kwargs):
        return self._response


async def test_fetch_raises_rate_limited_so_the_caller_can_wait():
    client = _FakeClient(_response(429, {"Retry-After": "30"}))

    with pytest.raises(RateLimited) as exc_info:
        await fetch_events(client, SOURCES["github"])

    assert exc_info.value.delay == 30.0
