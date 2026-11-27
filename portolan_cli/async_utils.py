"""Async utilities for concurrent I/O operations."""

from __future__ import annotations

import asyncio
import sys
import threading
import time
from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Generic, Self, TypeVar

if TYPE_CHECKING:
    from collections.abc import Awaitable, Callable
    from types import TracebackType

    from rich.progress import Progress, TaskID

# Generic type for operation results
T = TypeVar("T")


# Safe upper bound: consumer routers hit connection-tracking pressure well before 1k NAT sessions.
MAX_SAFE_CONNECTIONS = 100


def get_default_concurrency() -> int:
    """Get default concurrency limit for file-level async operations."""
    return 8


def get_default_chunk_concurrency() -> int:
    """Get default concurrency limit for per-file multipart chunks."""
    return 4


def calculate_connection_footprint(
    file_concurrency: int,
    chunk_concurrency: int,
    workers: int = 1,
) -> int:
    """Calculate total concurrent HTTP connections for given settings."""
    return workers * file_concurrency * chunk_concurrency


def adjust_concurrency_for_max_connections(
    file_concurrency: int,
    chunk_concurrency: int,
    max_connections: int,
) -> tuple[int, int]:
    """Adjust concurrency values to stay within max_connections limit."""
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


@dataclass
class AdaptiveConcurrencyManager:
    """Adaptive concurrency manager with slow-start and backoff."""

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


class CircuitBreakerError(Exception):
    """Raised when circuit breaker trips due to consecutive failures."""


@dataclass
class CircuitBreaker:
    """Circuit breaker for failing fast on repeated errors."""

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
        """Check if circuit is open, raising if so."""
        with self._lock:
            if self.is_open:
                raise CircuitBreakerError(
                    f"Circuit breaker open after {self.failure_threshold} consecutive failures"
                )


@dataclass
class AsyncExecutionResult(Generic[T]):
    """Result of executing an async operation on a single item."""

    item: str
    result: T | None
    error: str | None
    duration_seconds: float = 0.0


class AsyncIOExecutor(Generic[T]):
    """Semaphore-bounded async executor for concurrent I/O operations."""

    def __init__(
        self,
        concurrency: int = 8,
        *,
        circuit_breaker_threshold: int = 5,
        adaptive_manager: AdaptiveConcurrencyManager | None = None,
    ) -> None:
        """Initialize the executor."""
        self.concurrency = concurrency
        self._semaphore: asyncio.Semaphore | None = None
        self._circuit_breaker = CircuitBreaker(failure_threshold=circuit_breaker_threshold)
        self._adaptive_manager = adaptive_manager
        # For adaptive mode: track in-flight operations
        self._in_flight = 0
        self._in_flight_lock: asyncio.Lock | None = None

    async def _acquire_adaptive_permit(self) -> None:
        """Acquire permission to start work under adaptive concurrency."""
        if self._adaptive_manager is None or self._in_flight_lock is None:
            return

        while True:
            async with self._in_flight_lock:
                if self._in_flight < self._adaptive_manager.current_concurrency:
                    self._in_flight += 1
                    return
            # Wait a bit before checking again (adaptive backpressure)
            await asyncio.sleep(0.01)

    async def _release_adaptive_permit(self) -> None:
        """Release an adaptive permit after work completes."""
        if self._adaptive_manager is None or self._in_flight_lock is None:
            return

        async with self._in_flight_lock:
            self._in_flight -= 1

    async def execute(
        self,
        items: list[str],
        operation: Callable[[str], Awaitable[T]],
        *,
        on_complete: Callable[[str, T | None, str | None, int, int], None] | None = None,
    ) -> list[AsyncExecutionResult[T]]:
        """Execute operation on all items with bounded concurrency."""
        if not items:
            return []

        # For adaptive mode, use dynamic permit system
        # For fixed mode, use traditional semaphore
        if self._adaptive_manager is not None:
            self._in_flight_lock = asyncio.Lock()
            self._in_flight = 0
        else:
            self._semaphore = asyncio.Semaphore(self.concurrency)

        total = len(items)
        completed = 0
        completed_lock = asyncio.Lock()

        async def execute_one(item: str) -> AsyncExecutionResult[T]:
            """Execute operation on a single item with concurrency control."""
            nonlocal completed

            # Check circuit breaker before attempting
            self._circuit_breaker.check()

            # Gate admission based on mode
            if self._adaptive_manager is not None:
                await self._acquire_adaptive_permit()
                try:
                    return await _do_operation(item)
                finally:
                    await self._release_adaptive_permit()
            else:
                async with self._semaphore:  # type: ignore[union-attr]
                    return await _do_operation(item)

        async def _do_operation(item: str) -> AsyncExecutionResult[T]:
            """Execute the actual operation (shared by both modes)."""
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
        return await asyncio.gather(*tasks, return_exceptions=False)


class AsyncProgressReporter:
    """Thread-safe progress reporter for async operations."""

    def __init__(
        self,
        total_files: int,
        total_bytes: int = 0,
        *,
        json_mode: bool = False,
        description: str = "Uploading",
    ) -> None:
        """Initialize the progress reporter."""
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

    async def __aenter__(self) -> Self:
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
        """Advance the progress by one file and the given bytes."""
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
