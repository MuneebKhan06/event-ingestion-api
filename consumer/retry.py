import asyncio
import logging
from collections.abc import Callable, Coroutine
from typing import TypeVar

from app.config import get_settings

logger = logging.getLogger(__name__)

T = TypeVar("T")


class RetryExhaustedError(Exception):
    def __init__(self, attempts: int, last_error: Exception):
        self.attempts = attempts
        self.last_error = last_error
        super().__init__(f"Exhausted {attempts} attempts, last error: {last_error}")


async def with_retry(
    func: Callable[[], Coroutine[None, None, T]],
    *,
    max_retries: int | None = None,
    backoff_base_seconds: float | None = None,
) -> T:
    """Retry an async operation with exponential backoff (base * 2**attempt).

    For transient failures (e.g. a DB connection blip) only — permanent
    failures like a malformed payload should go straight to the DLQ instead
    of being retried, which is why this lives independent of DLQ routing.
    """
    settings = get_settings()
    retries = max_retries if max_retries is not None else settings.consumer_max_retries
    base = (
        backoff_base_seconds
        if backoff_base_seconds is not None
        else settings.consumer_retry_backoff_base_seconds
    )

    last_error: Exception | None = None
    for attempt in range(retries + 1):
        try:
            return await func()
        except Exception as exc:  # noqa: BLE001 - broad on purpose, caller decides what's retryable
            last_error = exc
            if attempt == retries:
                break
            delay = base * (2**attempt)
            logger.warning(
                "Attempt %d/%d failed (%s), retrying in %.1fs",
                attempt + 1,
                retries + 1,
                exc,
                delay,
            )
            await asyncio.sleep(delay)

    assert last_error is not None
    raise RetryExhaustedError(retries + 1, last_error)
