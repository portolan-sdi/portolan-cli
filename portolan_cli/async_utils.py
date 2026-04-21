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
# Default Configuration (Issue #344: Conservative defaults for home networks)
# =============================================================================

# Maximum concurrent HTTP connections considered safe for consumer networks.
# Consumer routers typically cap NAT sessions around 1k-4k, but connection-
# tracking pressure appears well before that. 100 is a safe upper bound.
MAX_SAFE_CONNECTIONS = 100


def get_default_concurrency() -> int:
    """Get default concurrency limit for file-level async operations.

    Returns a conservative default that won't overwhelm home networks.
    The total connection footprint is: file_concurrency × chunk_concurrency.

    Issue #344: Lowered from 50 to 8 to prevent NAT table exhaustion.
    With default chunk_concurrency=4, this yields 32 concurrent connections.

    Returns:
        Default file concurrency limit (8).
    """
    return 8


def get_default_chunk_concurrency() -> int:
    """Get default concurrency limit for per-file multipart chunks.

    Each file upload can use multiple concurrent HTTP connections for
    multipart upload chunks. This multiplies with file concurrency.

    Issue #344: Lowered from 12 to 4 to prevent connection storms.
    With default file_concurrency=8, this yields 32 concurrent connections.

    Returns:
        Default chunk concurrency limit (4).
    """
    return 4


def calculate_connection_footprint(
    file_concurrency: int,
    chunk_concurrency: int,
    workers: int = 1,
) -> int:
    """Calculate total concurrent HTTP connections for given settings.

    The connection footprint is: workers × file_concurrency × chunk_concurrency.

    Args:
        file_concurrency: Concurrent file uploads per worker.
        chunk_concurrency: Concurrent chunks per file.
        workers: Number of parallel workers (for catalog-wide operations).

    Returns:
        Maximum concurrent HTTP connections.

    Example:
        >>> calculate_connection_footprint(8, 4)  # Default
        32
        >>> calculate_connection_footprint(50, 12, workers=4)  # Old defaults
        2400
    """
    return workers * file_concurrency * chunk_concurrency


def adjust_concurrency_for_max_connections(
    file_concurrency: int,
    chunk_concurrency: int,
    max_connections: int,
) -> tuple[int, int]:
    """Adjust concurrency values to stay within max_connections limit.

    Reduces file_concurrency first (larger impact), then chunk_concurrency
    if needed. Never reduces either below 1.

    Args:
        file_concurrency: Desired file concurrency.
        chunk_concurrency: Desired chunk concurrency.
        max_connections: Maximum allowed concurrent connections.

    Returns:
        Tuple of (adjusted_file_concurrency, adjusted_chunk_concurrency).

    Example:
        >>> adjust_concurrency_for_max_connections(50, 4, 32)
        (8, 4)
    """
    # If already under limit, no adjustment needed
    if file_concurrency * chunk_concurrency <= max_connections:
        return file_concurrency, chunk_concurrency

    # Try reducing file concurrency first (it has larger impact)
    adjusted_file = max(1, max_connections // chunk_concurrency)
    if adjusted_file * chunk_concurrency <= max_connections:
        return adjusted_file, chunk_concurrency

    # Need to reduce chunk concurrency too
    adjusted_chunk = max(1, max_connections // adjusted_file)

    # Final adjustment to ensure we're under limit
    while adjusted_file * adjusted_chunk > max_connections:
        if adjusted_file > adjusted_chunk and adjusted_file > 1:
            adjusted_file -= 1
        elif adjusted_chunk > 1:
            adjusted_chunk -= 1
        else:
            break

    return max(1, adjusted_file), max(1, adjusted_chunk)


# =============================================================================
# Adaptive Concurrency Manager (Issue #344: Slow-start)
# =============================================================================


@dataclass
class AdaptiveConcurrencyManager:
    """Adaptive concurrency manager with slow-start and backoff.

    Starts with low concurrency and ramps up on success, backs off on errors.
    This prevents overwhelming home networks during initial connection burst.

    Attributes:
        max_concurrency: Maximum concurrency to ramp up to.
        initial_concurrency: Starting concurrency (default: 2).
        current_concurrency: Current active concurrency level.
        ramp_up_factor: Multiplier for increasing concurrency (default: 1.5).
        backoff_factor: Multiplier for decreasing concurrency (default: 0.5).
        success_window: Successes needed before ramping up (default: 5).

    Example:
        >>> manager = AdaptiveConcurrencyManager(max_concurrency=50)
        >>> manager.current_concurrency  # Starts low
        2
        >>> for _ in range(10):
        ...     manager.record_success()
        >>> manager.current_concurrency  # Ramped up
        4
    """

    max_concurrency: int
    initial_concurrency: int = 2
    ramp_up_factor: float = 1.5
    backoff_factor: float = 0.5
    success_window: int = 5
    current_concurrency: int = field(init=False)
    _success_count: int = field(default=0, repr=False)
    _consecutive_errors: int = field(default=0, repr=False)
    _lock: threading.Lock = field(default_factory=threading.Lock, repr=False)

    def __post_init__(self) -> None:
        """Initialize current_concurrency from initial_concurrency."""
        self.current_concurrency = min(self.initial_concurrency, self.max_concurrency)

    def record_success(self) -> None:
        """Record a successful operation, potentially ramping up concurrency."""
        with self._lock:
            self._consecutive_errors = 0
            self._success_count += 1

            # Ramp up after success_window consecutive successes
            if self._success_count >= self.success_window:
                new_concurrency = int(self.current_concurrency * self.ramp_up_factor)
                self.current_concurrency = min(new_concurrency, self.max_concurrency)
                # Ensure we always increase by at least 1 if below max
                if self.current_concurrency < self.max_concurrency:
                    self.current_concurrency = max(
                        self.current_concurrency,
                        min(self.current_concurrency + 1, self.max_concurrency),
                    )
                self._success_count = 0

    def record_error(self) -> None:
        """Record an error, backing off concurrency."""
        with self._lock:
            self._success_count = 0
            self._consecutive_errors += 1

            # More aggressive backoff on consecutive errors
            backoff = self.backoff_factor ** min(self._consecutive_errors, 3)
            new_concurrency = int(self.current_concurrency * backoff)
            self.current_concurrency = max(1, new_concurrency)

    def record_timeout(self) -> None:
        """Record a timeout (treated same as error for backoff)."""
        self.record_error()


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
        adaptive_manager: AdaptiveConcurrencyManager | None = None,
    ) -> None:
        """Initialize the executor.

        Args:
            concurrency: Maximum concurrent operations.
            circuit_breaker_threshold: Failures before circuit trips.
            adaptive_manager: Optional adaptive concurrency manager for slow-start.
                When provided, concurrency will be adjusted dynamically based on
                success/failure rates.
        """
        self.concurrency = concurrency
        self._semaphore: asyncio.Semaphore | None = None
        self._circuit_breaker = CircuitBreaker(failure_threshold=circuit_breaker_threshold)
        self._adaptive_manager = adaptive_manager

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
                    if self._adaptive_manager:
                        self._adaptive_manager.record_success()
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
                    if self._adaptive_manager:
                        self._adaptive_manager.record_error()
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
