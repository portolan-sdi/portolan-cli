"""Tests for parallel push functionality (issue #229).

This module tests the ability to push collections in parallel using
the --workers flag for catalog-wide push operations.
"""

from __future__ import annotations

import json
import threading
import time
from pathlib import Path
from unittest.mock import MagicMock, patch

from portolan_cli.push import (
    PushResult,
    get_default_workers,
    push_all_collections,
)


def _setup_valid_catalog(catalog_root: Path) -> None:
    """Helper to create a valid catalog with .portolan/config.yaml."""
    portolan_dir = catalog_root / ".portolan"
    portolan_dir.mkdir(parents=True, exist_ok=True)
    (portolan_dir / "config.yaml").write_text("version: '1.0'\n")


def _create_collection(catalog_root: Path, name: str) -> None:
    """Helper to create a collection with versions.json."""
    collection_dir = catalog_root / name
    collection_dir.mkdir()
    (collection_dir / "versions.json").write_text(json.dumps({"versions": []}))


# =============================================================================
# Tests for get_default_workers()
# =============================================================================


class TestGetDefaultWorkers:
    """Tests for get_default_workers() auto-detection function."""

    def test_returns_positive_integer(self) -> None:
        """get_default_workers returns a positive integer."""
        result = get_default_workers()
        assert isinstance(result, int)
        assert result > 0

    def test_respects_max_cap(self) -> None:
        """get_default_workers caps at 8 workers maximum."""
        result = get_default_workers()
        assert result <= 8

    @patch("os.cpu_count")
    def test_uses_cpu_count_when_available(self, mock_cpu_count: MagicMock) -> None:
        """get_default_workers uses CPU count when available."""
        mock_cpu_count.return_value = 4
        result = get_default_workers()
        assert result == 4

    @patch("os.cpu_count")
    def test_caps_high_cpu_count(self, mock_cpu_count: MagicMock) -> None:
        """get_default_workers caps CPU count at 8."""
        mock_cpu_count.return_value = 32
        result = get_default_workers()
        assert result == 8

    @patch("os.cpu_count")
    def test_fallback_when_cpu_count_unavailable(self, mock_cpu_count: MagicMock) -> None:
        """get_default_workers returns 4 when CPU count unavailable."""
        mock_cpu_count.return_value = None
        result = get_default_workers()
        assert result == 4


# =============================================================================
# Tests for parallel push_all_collections()
# =============================================================================


class TestPushAllCollectionsParallel:
    """Tests for parallel execution in push_all_collections()."""

    @patch("portolan_cli.push.push")
    def test_accepts_workers_parameter(self, mock_push: MagicMock, tmp_path: Path) -> None:
        """push_all_collections accepts workers parameter."""
        _setup_valid_catalog(tmp_path)
        _create_collection(tmp_path, "col1")

        mock_push.return_value = PushResult(
            success=True, files_uploaded=1, versions_pushed=1, conflicts=[], errors=[]
        )

        # Should not raise - workers parameter is accepted
        result = push_all_collections(
            catalog_root=tmp_path,
            destination="s3://bucket/catalog",
            workers=4,
        )

        assert result.success is True

    @patch("portolan_cli.push.push")
    def test_workers_1_is_sequential(self, mock_push: MagicMock, tmp_path: Path) -> None:
        """workers=1 executes sequentially (same as current behavior)."""
        _setup_valid_catalog(tmp_path)
        for name in ["col1", "col2", "col3"]:
            _create_collection(tmp_path, name)

        call_order: list[str] = []

        def track_calls(**kwargs):  # type: ignore[no-untyped-def]
            call_order.append(kwargs["collection"])
            return PushResult(
                success=True, files_uploaded=1, versions_pushed=1, conflicts=[], errors=[]
            )

        mock_push.side_effect = track_calls

        result = push_all_collections(
            catalog_root=tmp_path,
            destination="s3://bucket/catalog",
            workers=1,
        )

        assert result.success is True
        # Sequential execution should maintain sorted order
        assert call_order == ["col1", "col2", "col3"]

    @patch("portolan_cli.push.push")
    def test_workers_greater_than_1_executes_in_parallel(
        self, mock_push: MagicMock, tmp_path: Path
    ) -> None:
        """workers > 1 executes collections in parallel using ThreadPoolExecutor."""
        _setup_valid_catalog(tmp_path)
        for name in ["col1", "col2", "col3", "col4"]:
            _create_collection(tmp_path, name)

        # Track concurrent execution
        active_threads: list[str] = []
        max_concurrent = 0
        lock = threading.Lock()

        def track_concurrent(**kwargs):  # type: ignore[no-untyped-def]
            nonlocal max_concurrent
            collection = kwargs["collection"]

            with lock:
                active_threads.append(collection)
                max_concurrent = max(max_concurrent, len(active_threads))

            # Simulate some work
            time.sleep(0.05)

            with lock:
                active_threads.remove(collection)

            return PushResult(
                success=True, files_uploaded=1, versions_pushed=1, conflicts=[], errors=[]
            )

        mock_push.side_effect = track_concurrent

        result = push_all_collections(
            catalog_root=tmp_path,
            destination="s3://bucket/catalog",
            workers=4,
        )

        assert result.success is True
        assert result.total_collections == 4
        # With 4 workers and 4 collections, should see concurrent execution
        assert max_concurrent > 1, (
            f"Expected parallel execution, but max concurrent was {max_concurrent}"
        )

    @patch("portolan_cli.push.push")
    def test_workers_none_uses_default(self, mock_push: MagicMock, tmp_path: Path) -> None:
        """workers=None uses get_default_workers() for auto-detection."""
        _setup_valid_catalog(tmp_path)
        _create_collection(tmp_path, "col1")

        mock_push.return_value = PushResult(
            success=True, files_uploaded=1, versions_pushed=1, conflicts=[], errors=[]
        )

        # workers=None should auto-detect
        result = push_all_collections(
            catalog_root=tmp_path,
            destination="s3://bucket/catalog",
            workers=None,
        )

        assert result.success is True
        mock_push.assert_called_once()

    @patch("portolan_cli.push.push")
    def test_parallel_aggregates_results_correctly(
        self, mock_push: MagicMock, tmp_path: Path
    ) -> None:
        """Parallel execution correctly aggregates results from all workers."""
        _setup_valid_catalog(tmp_path)
        for name in ["col1", "col2", "col3"]:
            _create_collection(tmp_path, name)

        def return_results(**kwargs):  # type: ignore[no-untyped-def]
            # Each collection pushes different amounts
            files = {"col1": 2, "col2": 3, "col3": 5}
            return PushResult(
                success=True,
                files_uploaded=files[kwargs["collection"]],
                versions_pushed=1,
                conflicts=[],
                errors=[],
            )

        mock_push.side_effect = return_results

        result = push_all_collections(
            catalog_root=tmp_path,
            destination="s3://bucket/catalog",
            workers=3,
        )

        assert result.success is True
        assert result.total_collections == 3
        assert result.successful_collections == 3
        assert result.total_files_uploaded == 10  # 2 + 3 + 5
        assert result.total_versions_pushed == 3

    @patch("portolan_cli.push.push")
    def test_parallel_handles_individual_failures(
        self, mock_push: MagicMock, tmp_path: Path
    ) -> None:
        """Parallel execution continues and reports failures from individual collections."""
        _setup_valid_catalog(tmp_path)
        for name in ["col1", "col2", "col3"]:
            _create_collection(tmp_path, name)

        def mixed_results(**kwargs):  # type: ignore[no-untyped-def]
            if kwargs["collection"] == "col2":
                return PushResult(
                    success=False,
                    files_uploaded=0,
                    versions_pushed=0,
                    conflicts=[],
                    errors=["Network error"],
                )
            return PushResult(
                success=True, files_uploaded=1, versions_pushed=1, conflicts=[], errors=[]
            )

        mock_push.side_effect = mixed_results

        result = push_all_collections(
            catalog_root=tmp_path,
            destination="s3://bucket/catalog",
            workers=3,
        )

        assert result.success is False
        assert result.total_collections == 3
        assert result.successful_collections == 2
        assert result.failed_collections == 1
        assert "col2" in result.collection_errors

    @patch("portolan_cli.push.push")
    def test_parallel_handles_exceptions(self, mock_push: MagicMock, tmp_path: Path) -> None:
        """Parallel execution catches and reports exceptions from workers."""
        _setup_valid_catalog(tmp_path)
        for name in ["col1", "col2", "col3"]:
            _create_collection(tmp_path, name)

        def raise_on_col2(**kwargs):  # type: ignore[no-untyped-def]
            if kwargs["collection"] == "col2":
                raise ValueError("Something went wrong")
            return PushResult(
                success=True, files_uploaded=1, versions_pushed=1, conflicts=[], errors=[]
            )

        mock_push.side_effect = raise_on_col2

        result = push_all_collections(
            catalog_root=tmp_path,
            destination="s3://bucket/catalog",
            workers=3,
        )

        assert result.success is False
        assert result.successful_collections == 2
        assert result.failed_collections == 1
        assert "col2" in result.collection_errors

    @patch("portolan_cli.push.push")
    def test_workers_capped_at_collection_count(self, mock_push: MagicMock, tmp_path: Path) -> None:
        """Workers are capped at the number of collections (no wasted threads)."""
        _setup_valid_catalog(tmp_path)
        # Only 2 collections
        _create_collection(tmp_path, "col1")
        _create_collection(tmp_path, "col2")

        mock_push.return_value = PushResult(
            success=True, files_uploaded=1, versions_pushed=1, conflicts=[], errors=[]
        )

        # Request 10 workers, but only 2 collections
        result = push_all_collections(
            catalog_root=tmp_path,
            destination="s3://bucket/catalog",
            workers=10,
        )

        assert result.success is True
        assert mock_push.call_count == 2

    @patch("portolan_cli.push.push")
    def test_dry_run_with_parallel(self, mock_push: MagicMock, tmp_path: Path) -> None:
        """Dry run works correctly with parallel execution."""
        _setup_valid_catalog(tmp_path)
        for name in ["col1", "col2"]:
            _create_collection(tmp_path, name)

        mock_push.return_value = PushResult(
            success=True,
            files_uploaded=0,
            versions_pushed=0,
            conflicts=[],
            errors=[],
            dry_run=True,
            would_push_versions=2,
        )

        result = push_all_collections(
            catalog_root=tmp_path,
            destination="s3://bucket/catalog",
            workers=2,
            dry_run=True,
        )

        assert result.success is True
        # Verify dry_run was passed to individual push calls
        for call in mock_push.call_args_list:
            assert call.kwargs["dry_run"] is True
