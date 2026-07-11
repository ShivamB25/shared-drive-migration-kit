"""Retry policy primitives without a provider-specific error taxonomy."""

from __future__ import annotations

import time
from collections.abc import Callable
from typing import TypeVar


T = TypeVar("T")


def retry_with_exponential_backoff(
    operation: Callable[[], T],
    *,
    should_retry: Callable[[Exception], bool],
    retries: int,
    base_delay_seconds: float,
    max_delay_seconds: float = 300,
    sleep: Callable[[float], None] = time.sleep,
) -> T:
    """Retry a classified transient failure with a bounded exponential delay.

    ``retries`` is the number of attempts after the initial attempt. The final
    original exception is always preserved for the caller and status log.
    """
    if retries < 0:
        raise ValueError("retries must be >= 0")
    if base_delay_seconds <= 0 or max_delay_seconds <= 0:
        raise ValueError("retry delays must be positive")

    for attempt in range(retries + 1):
        try:
            return operation()
        except Exception as exc:
            if not should_retry(exc) or attempt >= retries:
                raise
            sleep(min(max_delay_seconds, base_delay_seconds * (2**attempt)))

    raise RuntimeError("unreachable retry state")
