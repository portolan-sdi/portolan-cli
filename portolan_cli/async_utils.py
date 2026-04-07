"""Async utilities for concurrent I/O operations.

This module provides async primitives for high-throughput parallel operations:
- AsyncIOExecutor: Semaphore-bounded concurrent task execution
- AsyncProgressReporter: Thread-safe progress tracking for async operations
- Circuit breaker pattern for resilience against cascading failures

Used by push_async() and pull_async() for efficient cloud storage operations.

Design Decisions:
- Uses asyncio for non-blocking I/O (vs ThreadPoolExecutor for CPU-bound)
- Semaphore limits concurrency to prevent overwhelming cloud APIs
- Circuit breaker trips after consecutive failures to fail fast
- Progress reporter is thread-safe for mixed sync/async usage
"""

from __future__ import annotations

import asyncio
import sys
import threading
import time
from collections.abc import Awaitable, Callable, Coroutine
from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Generic, TypeVar

if TYPE_CHECKING:
    from types import TracebackType

    from rich.progress import Progress, TaskID

# Generic type for operation results
T = TypeVar("T")


# =============================================================================
# Default Configuration
# =============================================================================


def get_default_concurrency() -> int:
    """Get default concurrency limit for async operations.

    For I/O-bound cloud operations, we use higher concurrency than CPU count.
    Cloud APIs typically handle 50-100 concurrent requests well.

    Returns:
        Default concurrency limit (50).
    """
    return 50


# =============================================================================
# Circuit Breaker
# =============================================================================


class CircuitBreakerError(Exception):
    """Raised when circuit breaker trips due to consecutive failures."""

    pass


@dataclass
class CircuitBreaker:
    """Circuit breaker for failing fast on repeated errors.

    When consecutive failures exceed the threshold, the circuit "opens"
    and raises CircuitBreakerError on subsequent calls, preventing
    cascading failures and wasted retries.

    Attributes:
        failure_threshold: Number of consecutive failures to trip.
        consecutive_failures: Current count of consecutive failures.
        is_open: True if circuit is tripped (failing fast).
    """

    failure_threshold: int = 5
    consecutive_failures: int = 0
    is_open: bool = False
    _lock: threading.Lock = field(default_factory=threading.Lock, repr=False)

    def record_success(self) -> None:
        """Record a successful operation, resetting failure count."""
        with self._lock:
            self.consecutive_failures = 0
            self.is_open = False

    def record_failure(self) -> None:
        """Record a failed operation, potentially tripping the circuit."""
        with self._lock:
            self.consecutive_failures += 1
            if self.consecutive_failures >= self.failure_threshold:
                self.is_open = True

    def check(self) -> None:
        """Check if circuit is open, raising if so.

        Raises:
            CircuitBreakerError: If circuit is open.
        """
        with self._lock:
            if self.is_open:
                raise CircuitBreakerError(
                    f"Circuit breaker open after {self.failure_threshold} consecutive failures"
                )


# =============================================================================
# Execution Result
# =============================================================================


@dataclass
class AsyncExecutionResult(Generic[T]):
    """Result of executing an async operation on a single item.

    Attributes:
        item: The item that was processed (e.g., file path).
        result: The operation result, or None if an exception occurred.
        error: Error message if an exception occurred, or None on success.
        duration_seconds: Time taken for this operation.
    """

    item: str
    result: T | None
    error: str | None
    duration_seconds: float = 0.0


# =============================================================================
# Async IO Executor
# =============================================================================


class AsyncIOExecutor(Generic[T]):
    """Semaphore-bounded async executor for concurrent I/O operations.

    Provides controlled concurrency with:
    - Configurable concurrency limit via semaphore
    - Circuit breaker for cascading failure protection
    - Progress callback for UI updates
    - Graceful error handling without stopping batch

    Example:
        >>> async def upload(path: str) -> int:
        ...     return await upload_file(path)
        >>> executor = AsyncIOExecutor(concurrency=50)
        >>> results = await executor.execute(
        ...     items=paths,
        ...     operation=upload,
        ...     on_complete=progress_callback,
        ... )
    """

    def __init__(
        self,
        concurrency: int = 50,
        *,
        circuit_breaker_threshold: int = 5,
    ) -> None:
        """Initialize the executor.

        Args:
            concurrency: Maximum concurrent operations.
            circuit_breaker_threshold: Failures before circuit trips.
        """
        self.concurrency = concurrency
        self._semaphore: asyncio.Semaphore | None = None
        self._circuit_breaker = CircuitBreaker(failure_threshold=circuit_breaker_threshold)

    async def execute(
        self,
        items: list[str],
        operation: Callable[[str], Awaitable[T]],
        *,
        on_complete: Callable[[str, T | None, str | None, int, int], None] | None = None,
    ) -> list[AsyncExecutionResult[T]]:
        """Execute operation on all items with bounded concurrency.

        Args:
            items: List of item identifiers to process.
            operation: Async callable that takes an item and returns a result.
            on_complete: Optional callback called after each item completes.
                Signature: (item, result, error, completed_count, total)

        Returns:
            List of AsyncExecutionResult objects, one per item.

        Raises:
            CircuitBreakerError: If too many consecutive failures occur.
        """
        if not items:
            return []

        # Create semaphore for this execution batch
        self._semaphore = asyncio.Semaphore(self.concurrency)

        total = len(items)
        completed = 0
        completed_lock = asyncio.Lock()
        results: list[AsyncExecutionResult[T]] = []

        async def execute_one(item: str) -> AsyncExecutionResult[T]:
            """Execute operation on a single item with semaphore."""
            nonlocal completed

            # Check circuit breaker before attempting
            self._circuit_breaker.check()

            async with self._semaphore:  # type: ignore[union-attr]
                start = time.perf_counter()
                try:
                    result = await operation(item)
                    duration = time.perf_counter() - start
                    self._circuit_breaker.record_success()
                    return AsyncExecutionResult(
                        item=item,
                        result=result,
                        error=None,
                        duration_seconds=duration,
                    )
                except CircuitBreakerError:
                    # Re-raise circuit breaker errors
                    raise
                except Exception as e:
                    duration = time.perf_counter() - start
                    self._circuit_breaker.record_failure()
                    return AsyncExecutionResult(
                        item=item,
                        result=None,
                        error=f"{type(e).__name__}: {e}",
                        duration_seconds=duration,
                    )

        async def execute_with_callback(item: str) -> AsyncExecutionResult[T]:
            """Execute and call completion callback."""
            nonlocal completed

            exec_result = await execute_one(item)

            async with completed_lock:
                completed += 1
                current_completed = completed

            if on_complete:
                on_complete(
                    item,
                    exec_result.result,
                    exec_result.error,
                    current_completed,
                    total,
                )

            return exec_result

        # Execute all items concurrently with bounded concurrency
        tasks = [execute_with_callback(item) for item in items]
        results = await asyncio.gather(*tasks, return_exceptions=False)

        return results


# =============================================================================
# Async Progress Reporter
# =============================================================================


class AsyncProgressReporter:
    """Thread-safe progress reporter for async operations.

    Compatible with asyncio event loops while maintaining thread-safety
    for callbacks from mixed sync/async code.

    Displays:
    - File progress: N/M files
    - Data progress: transferred / total (e.g., "54.2 MB / 1.2 GB")
    - Upload speed: current speed (e.g., "10.5 MiB/s")
    - Time elapsed and ETA

    Example:
        >>> async with AsyncProgressReporter(total_files=100, total_bytes=1_000_000) as reporter:
        ...     async for file in files:
        ...         await upload(file)
        ...         reporter.advance(bytes_uploaded=file.size)
    """

    def __init__(
        self,
        total_files: int,
        total_bytes: int = 0,
        *,
        json_mode: bool = False,
        description: str = "Uploading",
    ) -> None:
        """Initialize the progress reporter.

        Args:
            total_files: Total number of files to process.
            total_bytes: Total bytes to process (for accurate progress).
            json_mode: If True, suppress progress output.
            description: Description shown in progress bar.
        """
        self.total_files = total_files
        self.total_bytes = total_bytes
        self.json_mode = json_mode
        self.description = description
        self.files_completed = 0
        self.bytes_completed = 0
        self.elapsed_seconds: float = 0.0

        self._start_time: float | None = None
        self._progress: Progress | None = None
        self._task_id: TaskID | None = None
        self._lock = threading.Lock()

    async def __aenter__(self) -> AsyncProgressReporter:
        """Enter the async context and start progress display."""
        self._start_time = time.perf_counter()

        if not self.json_mode and sys.stdout.isatty():
            try:
                from rich.console import Console
                from rich.progress import (
                    BarColumn,
                    DownloadColumn,
                    Progress,
                    SpinnerColumn,
                    TextColumn,
                    TimeElapsedColumn,
                    TimeRemainingColumn,
                    TransferSpeedColumn,
                )

                console = Console(file=sys.stderr, force_terminal=None)

                self._progress = Progress(
                    SpinnerColumn(),
                    TextColumn(f"[bold blue]{self.description}"),
                    BarColumn(),
                    TextColumn("[progress.percentage]{task.percentage:>3.0f}%"),
                    DownloadColumn(),
                    TransferSpeedColumn(),
                    TimeElapsedColumn(),
                    TimeRemainingColumn(),
                    transient=True,
                    console=console,
                )
                self._progress.__enter__()
                self._task_id = self._progress.add_task(
                    self.description,
                    total=self.total_bytes if self.total_bytes > 0 else self.total_files,
                )
            except ImportError:
                self._progress = None

        return self

    async def __aexit__(
        self,
        exc_type: type[BaseException] | None,
        exc_val: BaseException | None,
        exc_tb: TracebackType | None,
    ) -> None:
        """Exit the async context and cleanup progress display."""
        if self._start_time is not None:
            self.elapsed_seconds = time.perf_counter() - self._start_time

        if self._progress is not None:
            self._progress.__exit__(exc_type, exc_val, exc_tb)

    def advance(self, bytes_uploaded: int = 0) -> None:
        """Advance the progress by one file and the given bytes.

        Thread-safe for use from async callbacks.

        Args:
            bytes_uploaded: Number of bytes processed for this file.
        """
        with self._lock:
            self.files_completed += 1
            self.bytes_completed += bytes_uploaded

            if self._progress is not None and self._task_id is not None:
                advance_amount = bytes_uploaded if self.total_bytes > 0 else 1
                self._progress.advance(self._task_id, advance=advance_amount)

    @property
    def average_speed(self) -> float:
        """Average speed in bytes per second."""
        if self.elapsed_seconds == 0:
            return 0.0
        return self.bytes_completed / self.elapsed_seconds


# =============================================================================
# Utility Functions
# =============================================================================


def run_async(coro: Coroutine[None, None, T]) -> T:
    """Run an async coroutine from sync code.

    Handles the common case of calling async code from a sync CLI entry point.
    Creates a new event loop.

    Args:
        coro: The coroutine to run.

    Returns:
        The coroutine's result.
    """
    return asyncio.run(coro)
