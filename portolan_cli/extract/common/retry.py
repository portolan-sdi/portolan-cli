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
"""

from __future__ import annotations

import time
from dataclasses import dataclass
from typing import TYPE_CHECKING, Any, Generic, TypeVar

if TYPE_CHECKING:
    from collections.abc import Callable

T = TypeVar("T")


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
    on_retry: Callable[[int, Exception], None] | None = None,
    **kwargs: Any,
) -> RetryResult[T]:
    """Execute an operation with retry and exponential backoff.

    Args:
        operation: Callable to execute
        config: Retry configuration
        *args: Positional arguments to pass to operation
        retry_on: Exception types to retry on (default: all exceptions)
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
        except retry_on as e:
            last_error = e

            if attempt < config.max_attempts:
                delay = config.get_delay(attempt)
                if on_retry:
                    on_retry(attempt, e)
                time.sleep(delay)
        except Exception as e:
            return RetryResult(
                success=False,
                value=None,
                attempts=attempt,
                error=e,
            )

    return RetryResult(
        success=False,
        value=None,
        attempts=config.max_attempts,
        error=last_error,
    )
