"""Parallel execution utilities for catalog operations.

This module provides common infrastructure for parallel collection processing:
- Worker count auto-detection
- Thread-safe progress tracking
- Parallel/sequential execution abstraction

Used by push_all_collections() and pull_all_collections() to avoid code duplication.
"""

from __future__ import annotations

import os
import threading
from collections.abc import Callable
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass
from typing import Generic, TypeVar

from portolan_cli.output import detail, info

# Generic type for operation results
T = TypeVar("T")


def get_default_workers() -> int:
    """Auto-detect number of workers for parallel operations.

    Uses CPU count doubled for I/O-bound operations (network uploads/downloads).
    No artificial cap - let the user's system capabilities determine the limit.

    Returns:
        Number of workers to use (minimum 1).
    """
    try:
        cpu_count = os.cpu_count()
    except Exception:
        # Exotic platforms might raise; fall back gracefully
        return 4
    if cpu_count is None:
        return 4  # Sensible fallback
    # For I/O-bound operations, use 2x CPU count
    return max(cpu_count * 2, 1)


@dataclass
class ParallelProgress:
    """Thread-safe progress tracker for parallel operations."""

    total: int
    _completed: int = 0
    _lock: threading.Lock = None  # type: ignore[assignment]

    def __post_init__(self) -> None:
        """Initialize the lock after dataclass creation."""
        self._lock = threading.Lock()

    def increment(self) -> int:
        """Atomically increment completed count and return new value."""
        with self._lock:
            self._completed += 1
            return self._completed

    @property
    def completed(self) -> int:
        """Get current completed count (thread-safe read)."""
        with self._lock:
            return self._completed


@dataclass
class ExecutionResult(Generic[T]):
    """Result of executing an operation on a single item.

    Attributes:
        item: The item that was processed (e.g., collection name).
        result: The operation result, or None if an exception occurred.
        error: Error message if an exception occurred, or None on success.
    """

    item: str
    result: T | None
    error: str | None


def execute_parallel(
    items: list[str],
    operation: Callable[[str], T],
    *,
    workers: int | None = None,
    on_complete: Callable[[str, T | None, str | None, int, int], None] | None = None,
    verbose: bool = True,
) -> list[ExecutionResult[T]]:
    """Execute an operation on multiple items with configurable parallelism.

    This function abstracts the common parallel/sequential execution pattern:
    - workers=1: Sequential execution (preserves order)
    - workers>1: Parallel execution with ThreadPoolExecutor

    Args:
        items: List of item identifiers to process.
        operation: Callable that takes an item and returns a result.
        workers: Number of parallel workers. None = auto-detect, 1 = sequential.
        on_complete: Optional callback called after each item completes.
            Signature: (item, result, error, completed_count, total)
        verbose: If True (default), print progress messages. If False, only
            failures are reported via on_complete callback.

    Returns:
        List of ExecutionResult objects, one per item.
    """
    total = len(items)
    if total == 0:
        return []

    # Determine number of workers
    if workers is None:
        workers = get_default_workers()
    # Cap workers at item count (no wasted threads)
    workers = min(workers, total)

    results: list[ExecutionResult[T]] = []
    progress = ParallelProgress(total=total)

    def execute_one(item: str) -> ExecutionResult[T]:
        """Execute operation on a single item and return result."""
        try:
            result = operation(item)
            return ExecutionResult(item=item, result=result, error=None)
        except Exception as e:
            # Catch all exceptions to ensure resilient parallel execution
            return ExecutionResult(item=item, result=None, error=f"{type(e).__name__}: {e}")

    if workers == 1:
        # Sequential execution (preserves order)
        for i, item in enumerate(items, 1):
            if verbose:
                info(f"-> Processing {i}/{total}: {item}")
            exec_result = execute_one(item)
            completed = progress.increment()
            if on_complete:
                on_complete(item, exec_result.result, exec_result.error, completed, total)
            results.append(exec_result)
    else:
        # Parallel execution with ThreadPoolExecutor
        if verbose:
            info(f"Using {workers} parallel worker(s)")
        with ThreadPoolExecutor(max_workers=workers) as executor:
            # Submit all items
            futures = {executor.submit(execute_one, item): item for item in items}

            # Process results as they complete
            for future in as_completed(futures):
                exec_result = future.result()
                completed = progress.increment()
                if verbose:
                    detail(f"[{completed}/{total}] Completed: {exec_result.item}")
                if on_complete:
                    on_complete(
                        exec_result.item,
                        exec_result.result,
                        exec_result.error,
                        completed,
                        total,
                    )
                results.append(exec_result)

    return results
