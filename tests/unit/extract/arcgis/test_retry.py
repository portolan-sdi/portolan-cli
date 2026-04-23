"""Tests for retry logic with exponential backoff.

The retry module provides utilities for retrying failed operations
during ArcGIS extraction, with configurable attempts and backoff.
"""

from __future__ import annotations

from unittest.mock import MagicMock, patch

import pytest

from portolan_cli.extract.arcgis.retry import (
    RetryConfig,
    RetryError,
    RetryResult,
    retry_with_backoff,
)

pytestmark = pytest.mark.unit


# =============================================================================
# RetryConfig tests
# =============================================================================


class TestRetryConfig:
    """Tests for RetryConfig dataclass."""

    def test_default_values(self) -> None:
        """Should have sensible defaults."""
        config = RetryConfig()

        assert config.max_attempts == 3
        assert config.initial_delay == 1.0
        assert config.backoff_factor == 2.0
        assert config.max_delay == 60.0

    def test_custom_values(self) -> None:
        """Should accept custom values."""
        config = RetryConfig(
            max_attempts=5,
            initial_delay=0.5,
            backoff_factor=3.0,
            max_delay=120.0,
        )

        assert config.max_attempts == 5
        assert config.initial_delay == 0.5
        assert config.backoff_factor == 3.0
        assert config.max_delay == 120.0

    def test_get_delay_exponential(self) -> None:
        """Should calculate exponential delays."""
        config = RetryConfig(initial_delay=1.0, backoff_factor=2.0)

        assert config.get_delay(1) == 1.0  # 1 * 2^0
        assert config.get_delay(2) == 2.0  # 1 * 2^1
        assert config.get_delay(3) == 4.0  # 1 * 2^2
        assert config.get_delay(4) == 8.0  # 1 * 2^3

    def test_get_delay_respects_max(self) -> None:
        """Should cap delay at max_delay."""
        config = RetryConfig(initial_delay=10.0, backoff_factor=2.0, max_delay=30.0)

        assert config.get_delay(1) == 10.0
        assert config.get_delay(2) == 20.0
        assert config.get_delay(3) == 30.0  # Capped at max
        assert config.get_delay(4) == 30.0  # Still capped


# =============================================================================
# retry_with_backoff tests
# =============================================================================


class TestRetryWithBackoff:
    """Tests for retry_with_backoff function."""

    def test_succeeds_on_first_attempt(self) -> None:
        """Should return result if operation succeeds immediately."""
        operation = MagicMock(return_value="success")

        result = retry_with_backoff(operation, RetryConfig())

        assert result.success is True
        assert result.value == "success"
        assert result.attempts == 1
        assert result.error is None
        operation.assert_called_once()

    def test_retries_on_failure(self) -> None:
        """Should retry if operation fails."""
        operation = MagicMock(side_effect=[ValueError("fail"), "success"])

        with patch("time.sleep"):  # Don't actually sleep in tests
            result = retry_with_backoff(operation, RetryConfig(max_attempts=3))

        assert result.success is True
        assert result.value == "success"
        assert result.attempts == 2

    def test_exhausts_retries(self) -> None:
        """Should give up after max_attempts."""
        error = ValueError("persistent failure")
        operation = MagicMock(side_effect=error)

        with patch("time.sleep"):
            result = retry_with_backoff(operation, RetryConfig(max_attempts=3))

        assert result.success is False
        assert result.value is None
        assert result.attempts == 3
        assert result.error is error

    def test_applies_exponential_backoff(self) -> None:
        """Should sleep with exponential delays between attempts."""
        operation = MagicMock(side_effect=[ValueError(), ValueError(), "success"])
        config = RetryConfig(initial_delay=1.0, backoff_factor=2.0)

        with patch("time.sleep") as mock_sleep:
            retry_with_backoff(operation, config)

        # Should have slept twice (after attempt 1 and 2)
        assert mock_sleep.call_count == 2
        mock_sleep.assert_any_call(1.0)  # First delay
        mock_sleep.assert_any_call(2.0)  # Second delay (1 * 2^1)

    def test_passes_args_to_operation(self) -> None:
        """Should pass args and kwargs to operation."""
        operation = MagicMock(return_value="ok")

        retry_with_backoff(
            operation,
            RetryConfig(),
            "arg1",
            "arg2",
            kwarg1="value1",
        )

        operation.assert_called_with("arg1", "arg2", kwarg1="value1")

    def test_only_retries_specified_exceptions(self) -> None:
        """Should only retry on specified exception types, re-raise others."""
        operation = MagicMock(side_effect=TypeError("wrong type"))

        with patch("time.sleep"), pytest.raises(TypeError, match="wrong type"):
            retry_with_backoff(
                operation,
                RetryConfig(max_attempts=3),
                retry_on=(ValueError,),  # Only retry ValueError
            )

        # Should fail immediately on first attempt, re-raising the exception
        operation.assert_called_once()

    def test_retries_all_exceptions_by_default(self) -> None:
        """Should retry all exceptions by default."""
        operation = MagicMock(side_effect=[TypeError(), RuntimeError(), "success"])

        with patch("time.sleep"):
            result = retry_with_backoff(operation, RetryConfig(max_attempts=3))

        assert result.success is True
        assert result.attempts == 3

    def test_calls_on_retry_callback(self) -> None:
        """Should call on_retry callback between attempts."""
        operation = MagicMock(side_effect=[ValueError("first"), "success"])
        on_retry = MagicMock()

        with patch("time.sleep"):
            retry_with_backoff(operation, RetryConfig(), on_retry=on_retry)

        # Should have called on_retry once (after first failure)
        on_retry.assert_called_once()
        call_args = on_retry.call_args
        assert call_args[0][0] == 1  # attempt number
        assert isinstance(call_args[0][1], ValueError)  # the error


# =============================================================================
# RetryResult tests
# =============================================================================


class TestRetryResult:
    """Tests for RetryResult dataclass."""

    def test_success_result(self) -> None:
        """Should create success result."""
        result = RetryResult(success=True, value="data", attempts=1, error=None)

        assert result.success is True
        assert result.value == "data"
        assert result.attempts == 1
        assert result.error is None

    def test_failure_result(self) -> None:
        """Should create failure result."""
        error = ValueError("failed")
        result = RetryResult(success=False, value=None, attempts=3, error=error)

        assert result.success is False
        assert result.value is None
        assert result.attempts == 3
        assert result.error is error

    def test_unwrap_success(self) -> None:
        """unwrap() should return value on success."""
        result = RetryResult(success=True, value="data", attempts=1, error=None)

        assert result.unwrap() == "data"

    def test_unwrap_failure_raises(self) -> None:
        """unwrap() should raise RetryError on failure."""
        error = ValueError("original error")
        result = RetryResult(success=False, value=None, attempts=3, error=error)

        with pytest.raises(RetryError) as exc_info:
            result.unwrap()

        assert "after 3 attempts" in str(exc_info.value)
        assert exc_info.value.__cause__ is error
