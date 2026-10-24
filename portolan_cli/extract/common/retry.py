"""Retry logic with exponential backoff for extraction operations.

This module provides retry utilities for handling transient failures
during layer extraction. The retry strategy follows the design:
- Default: 3 attempts per layer
- Backoff: Exponential (1s, 2s, 4s by default)
- On persistent failure: Returns error for caller to handle

Typical usage:
    from portolan_cli.extract.common.retry import RetryConfig, retry_with_backoff

    config = RetryConfig(max_attempts=3)
    result = retry_with_backoff(
        lambda: extract_layer(url, output_path),
        config,
        on_retry=lambda attempt, err: print(f"Retry {attempt}: {err}"),
    )

    if result.success:
        print(f"Extracted in {result.attempts} attempts")
    else:
        print(f"Failed after {result.attempts} attempts: {result.error}")

Only a transient failure gets a retry: a transport error, a timeout, HTTP
408, 429, or 5xx, or an ArcGIS JSON body with one of those codes.
``permanent_status`` finds a failure that a retry cannot fix: any other HTTP
status, or an ArcGIS invalid-token error (codes 498 and 499). The layer fetch
of every source and the ArcGIS discovery client use it, so the rule has one
home. Each of those failures ends at once with ``PERMANENT_NOTE``.
"""

from __future__ import annotations

import time
from dataclasses import dataclass
from typing import TYPE_CHECKING, Any, Generic, TypeVar

if TYPE_CHECKING:
    from collections.abc import Callable

T = TypeVar("T")

#: The sentence that ends the message of a failure that a retry cannot fix.
PERMANENT_NOTE = "A retry cannot fix this failure."

# geoparquet-io raises this text, with no status, for the ArcGIS codes 498
# and 499 in a JSON body (core/arcgis.py ``_handle_arcgis_response``).
_GPIO_EXPIRED_AUTH = "Invalid or expired token"


class PermanentError(Exception):
    """A failure that a retry cannot fix. The message names the status."""


def is_transient_status(code: object) -> bool:
    """Return True for an HTTP status or ArcGIS JSON code that a retry can change."""
    try:
        status = int(str(code))
    except ValueError:
        return False
    return status in (408, 429) or 500 <= status < 600


def permanent_status(error: BaseException) -> str | None:
    """Return the status of a failure that a retry cannot fix, or None.

    The search follows the exception chain, because geoparquet-io wraps the
    HTTP error. A transport error or a timeout carries no status, so it
    gets the retries.

    Args:
        error: The failure.

    Returns:
        The status, such as ``HTTP 404``, or None for a transient failure.
    """
    seen: set[int] = set()
    current: BaseException | None = error
    while current is not None and id(current) not in seen:
        seen.add(id(current))
        status = getattr(current, "status_code", None)
        response = getattr(current, "response", None)
        status = status if isinstance(status, int) else getattr(response, "status_code", None)
        if isinstance(status, int):
            return None if is_transient_status(status) else f"HTTP {status}"
        if _GPIO_EXPIRED_AUTH in str(current):
            return "ArcGIS error 498 or 499"
        current = current.__cause__ or current.__context__
    return None


class RetryError(Exception):
    """Raised when retry_with_backoff exhausts all attempts."""

    def __init__(self, message: str, attempts: int) -> None:
        super().__init__(message)
        self.attempts = attempts


@dataclass
class RetryConfig:
    """Configuration for retry behavior.

    Attributes:
        max_attempts: Maximum number of attempts before giving up
        initial_delay: Delay in seconds before first retry
        backoff_factor: Multiplier for each subsequent delay
        max_delay: Maximum delay between retries (caps exponential growth)
    """

    max_attempts: int = 3
    initial_delay: float = 1.0
    backoff_factor: float = 2.0
    max_delay: float = 60.0

    def get_delay(self, attempt: int) -> float:
        """Calculate delay for the given attempt number."""
        delay = self.initial_delay * (self.backoff_factor ** (attempt - 1))
        return min(delay, self.max_delay)


@dataclass
class RetryResult(Generic[T]):
    """Result of a retry operation.

    Attributes:
        success: Whether the operation succeeded
        value: The return value (if successful)
        attempts: Number of attempts made
        error: The last error (if failed)
    """

    success: bool
    value: T | None
    attempts: int
    error: Exception | None

    def unwrap(self) -> T:
        """Return the value or raise RetryError if failed."""
        if self.success:
            return self.value  # type: ignore[return-value]

        msg = f"Operation failed after {self.attempts} attempts"
        error = RetryError(msg, self.attempts)
        raise error from self.error


def retry_with_backoff(
    operation: Callable[..., T],
    config: RetryConfig,
    *args: Any,
    retry_on: tuple[type[Exception], ...] | None = None,
    give_up_on: tuple[type[Exception], ...] = (),
    on_retry: Callable[[int, Exception], None] | None = None,
    **kwargs: Any,
) -> RetryResult[T]:
    """Execute an operation with retry and exponential backoff.

    Args:
        operation: Callable to execute
        config: Retry configuration
        *args: Positional arguments to pass to operation
        retry_on: Exception types to retry on (default: all exceptions)
        give_up_on: Exception types that end the retries at once. The
            result then fails with that error, as a retry cannot change it.
        on_retry: Callback called before each retry with (attempt, error)
        **kwargs: Keyword arguments to pass to operation

    Returns:
        RetryResult containing success status, value, attempts, and error
    """
    if retry_on is None:
        retry_on = (Exception,)

    last_error: Exception | None = None

    for attempt in range(1, config.max_attempts + 1):
        try:
            value = operation(*args, **kwargs)
            return RetryResult(
                success=True,
                value=value,
                attempts=attempt,
                error=None,
            )
        except give_up_on as e:
            return RetryResult(success=False, value=None, attempts=attempt, error=e)
        except retry_on as e:
            last_error = e

            if attempt < config.max_attempts:
                delay = config.get_delay(attempt)
                if on_retry:
                    on_retry(attempt, e)
                time.sleep(delay)
        except Exception:
            raise

    return RetryResult(
        success=False,
        value=None,
        attempts=config.max_attempts,
        error=last_error,
    )
