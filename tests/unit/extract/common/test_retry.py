"""Tests for common retry logic."""

from __future__ import annotations

import httpx
import pytest

from portolan_cli.extract.common.retry import (
    RetryConfig,
    RetryError,
    RetryResult,
    is_transient_status,
    permanent_status,
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

    def test_only_retries_specified_exceptions(self) -> None:
        """An error outside retry_on propagates on the first attempt."""
        calls: list[int] = []

        def fail() -> str:
            calls.append(1)
            raise KeyError("permanent")

        with pytest.raises(KeyError):
            retry_with_backoff(fail, RetryConfig(initial_delay=0.0), retry_on=(ValueError,))
        assert len(calls) == 1

    def test_give_up_on_ends_the_retries_with_a_failed_result(self) -> None:
        """An empty layer is not retried: the next answer is empty too (#1034)."""
        calls: list[int] = []
        error = LookupError("no features")

        def empty() -> str:
            calls.append(1)
            raise error

        result = retry_with_backoff(
            empty, RetryConfig(max_attempts=3, initial_delay=0.0), give_up_on=(LookupError,)
        )

        assert (result.success, result.attempts, result.error) == (False, 1, error)
        assert len(calls) == 1


def _http_error(status: int) -> httpx.HTTPStatusError:
    request = httpx.Request("GET", "https://h.example/x")
    response = httpx.Response(status, request=request)
    return httpx.HTTPStatusError(f"HTTP {status}", request=request, response=response)


class TestRetryClassification:
    """The maintainer rule: retry a transient failure only (#1035)."""

    @pytest.mark.parametrize(
        ("code", "transient"),
        [(408, True), (429, True), (500, True), (503, True), (599, True), (400, False)]
        + [(401, False), (403, False), (404, False), (498, False), (499, False), ("x", False)],
    )
    def test_transient_status(self, code: object, transient: bool) -> None:
        assert is_transient_status(code) is transient

    @pytest.mark.parametrize(
        ("status", "label"), [(401, "HTTP 401"), (404, "HTTP 404"), (503, None)]
    )
    def test_status_in_the_chain(self, status: int, label: str | None) -> None:
        """geoparquet-io raises its own error ``from None``. The context keeps the status."""
        try:
            try:
                raise _http_error(status)
            except httpx.HTTPStatusError:
                raise RuntimeError("Remote access error") from None
        except RuntimeError as wrapped:
            assert permanent_status(wrapped) == label

    def test_requests_error_with_a_response(self) -> None:
        import requests  # type: ignore[import-untyped]

        response = requests.Response()
        response.status_code = 403

        assert permanent_status(requests.HTTPError("403", response=response)) == "HTTP 403"

    def test_geoparquet_io_token_error(self) -> None:
        error = RuntimeError("Feature count: Invalid or expired token. Please re-authenticate.")

        assert permanent_status(error) == "ArcGIS error 498 or 499"

    @pytest.mark.parametrize(
        "error",
        [httpx.ConnectError("refused"), httpx.ReadTimeout("slow"), RuntimeError("Error 500")],
    )
    def test_failure_without_a_permanent_status_is_retried(self, error: Exception) -> None:
        assert permanent_status(error) is None

    @pytest.mark.parametrize(("code", "label"), [(499, "ArcGIS error 499"), (500, None)])
    def test_arcgis_code_of_a_200_body(self, code: int, label: str | None) -> None:
        """An ArcGIS JSON error that came with HTTP 200 carries its code as ``arcgis_code``."""

        class _BodyError(Exception):
            arcgis_code = code

        assert permanent_status(_BodyError("ArcGIS error")) == label
