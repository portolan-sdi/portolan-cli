"""Tests for common retry logic."""

from __future__ import annotations

import pytest

from portolan_cli.extract.common.retry import (
    RetryConfig,
    RetryError,
    RetryResult,
    retry_with_backoff,
)

pytestmark = pytest.mark.unit


class TestRetryConfig:
    """Tests for RetryConfig."""

    def test_default_values(self) -> None:
        """Default config has sensible defaults."""
        config = RetryConfig()
        assert config.max_attempts == 3
        assert config.initial_delay == 1.0
        assert config.backoff_factor == 2.0
        assert config.max_delay == 60.0

    def test_get_delay_exponential(self) -> None:
        """Delay grows exponentially."""
        config = RetryConfig(initial_delay=1.0, backoff_factor=2.0)
        assert config.get_delay(1) == 1.0
        assert config.get_delay(2) == 2.0
        assert config.get_delay(3) == 4.0

    def test_get_delay_capped(self) -> None:
        """Delay is capped at max_delay."""
        config = RetryConfig(initial_delay=10.0, backoff_factor=2.0, max_delay=15.0)
        assert config.get_delay(1) == 10.0
        assert config.get_delay(2) == 15.0  # Capped at max_delay
        assert config.get_delay(3) == 15.0


class TestRetryResult:
    """Tests for RetryResult."""

    def test_successful_result(self) -> None:
        """Successful result has correct attributes."""
        result: RetryResult[int] = RetryResult(success=True, value=42, attempts=1, error=None)
        assert result.success
        assert result.value == 42
        assert result.attempts == 1
        assert result.error is None

    def test_unwrap_success(self) -> None:
        """Unwrap returns value on success."""
        result: RetryResult[str] = RetryResult(success=True, value="hello", attempts=1, error=None)
        assert result.unwrap() == "hello"

    def test_unwrap_failure(self) -> None:
        """Unwrap raises RetryError on failure."""
        result: RetryResult[str] = RetryResult(
            success=False, value=None, attempts=3, error=ValueError("test")
        )
        with pytest.raises(RetryError) as exc_info:
            result.unwrap()
        assert exc_info.value.attempts == 3


class TestRetryWithBackoff:
    """Tests for retry_with_backoff function."""

    def test_success_first_try(self) -> None:
        """Successful operation returns immediately."""

        def succeed() -> str:
            return "success"

        result = retry_with_backoff(succeed, RetryConfig(max_attempts=3))
        assert result.success
        assert result.value == "success"
        assert result.attempts == 1

    def test_success_after_retry(self) -> None:
        """Operation succeeds after initial failures."""
        call_count = 0

        def fail_then_succeed() -> str:
            nonlocal call_count
            call_count += 1
            if call_count < 2:
                raise ValueError("temporary failure")
            return "success"

        config = RetryConfig(max_attempts=3, initial_delay=0.01)
        result = retry_with_backoff(fail_then_succeed, config)

        assert result.success
        assert result.value == "success"
        assert result.attempts == 2

    def test_failure_exhausted(self) -> None:
        """Operation fails after exhausting all attempts."""

        def always_fail() -> str:
            raise ValueError("always fails")

        config = RetryConfig(max_attempts=2, initial_delay=0.01)
        result = retry_with_backoff(always_fail, config)

        assert not result.success
        assert result.value is None
        assert result.attempts == 2
        assert isinstance(result.error, ValueError)

    def test_with_args_and_kwargs(self) -> None:
        """Arguments are passed to operation."""

        def add(a: int, b: int) -> int:
            return a + b

        result = retry_with_backoff(add, RetryConfig(), 1, b=2)
        assert result.success
        assert result.value == 3

    def test_on_retry_callback(self) -> None:
        """on_retry callback is called on each retry."""
        retries: list[tuple[int, Exception]] = []
        call_count = 0

        def fail_twice() -> str:
            nonlocal call_count
            call_count += 1
            if call_count < 3:
                raise ValueError(f"fail {call_count}")
            return "success"

        def on_retry(attempt: int, error: Exception) -> None:
            retries.append((attempt, error))

        config = RetryConfig(max_attempts=3, initial_delay=0.01)
        retry_with_backoff(fail_twice, config, on_retry=on_retry)

        assert len(retries) == 2
        assert retries[0][0] == 1
        assert retries[1][0] == 2
