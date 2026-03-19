"""Tests for parallel pull functionality.

This module tests the ability to pull collections in parallel using
the --workers flag for catalog-wide pull operations.
"""

from __future__ import annotations

import json
import threading
import time
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest

from portolan_cli.pull import (
    PullResult,
    pull_all_collections,
)

# Mark all tests in this module as unit tests
pytestmark = pytest.mark.unit


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
# Tests for parallel pull_all_collections()
# =============================================================================


class TestPullAllCollectionsParallel:
    """Tests for parallel execution in pull_all_collections()."""

    @patch("portolan_cli.pull.pull")
    def test_accepts_workers_parameter(self, mock_pull: MagicMock, tmp_path: Path) -> None:
        """pull_all_collections accepts workers parameter."""
        _setup_valid_catalog(tmp_path)
        _create_collection(tmp_path, "col1")

        mock_pull.return_value = PullResult(
            success=True,
            files_downloaded=1,
            files_skipped=0,
            local_version="1.0.0",
            remote_version="1.0.1",
        )

        # Should not raise - workers parameter is accepted
        result = pull_all_collections(
            remote_url="s3://bucket/catalog",
            local_root=tmp_path,
            workers=4,
        )

        assert result.success is True

    @patch("portolan_cli.pull.pull")
    def test_workers_1_is_sequential(self, mock_pull: MagicMock, tmp_path: Path) -> None:
        """workers=1 executes sequentially."""
        _setup_valid_catalog(tmp_path)
        for name in ["col1", "col2", "col3"]:
            _create_collection(tmp_path, name)

        call_order: list[str] = []

        def track_calls(**kwargs):  # type: ignore[no-untyped-def]
            call_order.append(kwargs["collection"])
            return PullResult(
                success=True,
                files_downloaded=1,
                files_skipped=0,
                local_version="1.0.0",
                remote_version="1.0.1",
            )

        mock_pull.side_effect = track_calls

        result = pull_all_collections(
            remote_url="s3://bucket/catalog",
            local_root=tmp_path,
            workers=1,
        )

        assert result.success is True
        # Sequential execution should maintain sorted order
        assert call_order == ["col1", "col2", "col3"]

    @patch("portolan_cli.pull.pull")
    def test_workers_greater_than_1_executes_in_parallel(
        self, mock_pull: MagicMock, tmp_path: Path
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

            return PullResult(
                success=True,
                files_downloaded=1,
                files_skipped=0,
                local_version="1.0.0",
                remote_version="1.0.1",
            )

        mock_pull.side_effect = track_concurrent

        result = pull_all_collections(
            remote_url="s3://bucket/catalog",
            local_root=tmp_path,
            workers=4,
        )

        assert result.success is True
        assert result.total_collections == 4
        # With 4 workers and 4 collections, should see concurrent execution
        assert max_concurrent > 1, (
            f"Expected parallel execution, but max concurrent was {max_concurrent}"
        )

    @patch("portolan_cli.pull.pull")
    def test_workers_none_uses_default(self, mock_pull: MagicMock, tmp_path: Path) -> None:
        """workers=None uses get_default_workers() for auto-detection."""
        _setup_valid_catalog(tmp_path)
        _create_collection(tmp_path, "col1")

        mock_pull.return_value = PullResult(
            success=True,
            files_downloaded=1,
            files_skipped=0,
            local_version="1.0.0",
            remote_version="1.0.1",
        )

        # workers=None should auto-detect
        result = pull_all_collections(
            remote_url="s3://bucket/catalog",
            local_root=tmp_path,
            workers=None,
        )

        assert result.success is True
        mock_pull.assert_called_once()

    @patch("portolan_cli.pull.pull")
    def test_parallel_aggregates_results_correctly(
        self, mock_pull: MagicMock, tmp_path: Path
    ) -> None:
        """Parallel execution correctly aggregates results from all workers."""
        _setup_valid_catalog(tmp_path)
        for name in ["col1", "col2", "col3"]:
            _create_collection(tmp_path, name)

        def return_results(**kwargs):  # type: ignore[no-untyped-def]
            # Each collection downloads different amounts
            files = {"col1": 2, "col2": 3, "col3": 5}
            return PullResult(
                success=True,
                files_downloaded=files[kwargs["collection"]],
                files_skipped=0,
                local_version="1.0.0",
                remote_version="1.0.1",
            )

        mock_pull.side_effect = return_results

        result = pull_all_collections(
            remote_url="s3://bucket/catalog",
            local_root=tmp_path,
            workers=3,
        )

        assert result.success is True
        assert result.total_collections == 3
        assert result.successful_collections == 3
        assert result.total_files_downloaded == 10  # 2 + 3 + 5

    @patch("portolan_cli.pull.pull")
    def test_parallel_handles_individual_failures(
        self, mock_pull: MagicMock, tmp_path: Path
    ) -> None:
        """Parallel execution continues and reports failures from individual collections."""
        _setup_valid_catalog(tmp_path)
        for name in ["col1", "col2", "col3"]:
            _create_collection(tmp_path, name)

        def mixed_results(**kwargs):  # type: ignore[no-untyped-def]
            if kwargs["collection"] == "col2":
                return PullResult(
                    success=False,
                    files_downloaded=0,
                    files_skipped=0,
                    local_version="1.0.0",
                    remote_version=None,
                    uncommitted_changes=["file.txt"],
                )
            return PullResult(
                success=True,
                files_downloaded=1,
                files_skipped=0,
                local_version="1.0.0",
                remote_version="1.0.1",
            )

        mock_pull.side_effect = mixed_results

        result = pull_all_collections(
            remote_url="s3://bucket/catalog",
            local_root=tmp_path,
            workers=3,
        )

        assert result.success is False
        assert result.total_collections == 3
        assert result.successful_collections == 2
        assert result.failed_collections == 1
        assert "col2" in result.collection_errors

    @patch("portolan_cli.pull.pull")
    def test_parallel_handles_exceptions(self, mock_pull: MagicMock, tmp_path: Path) -> None:
        """Parallel execution catches and reports exceptions from workers."""
        _setup_valid_catalog(tmp_path)
        for name in ["col1", "col2", "col3"]:
            _create_collection(tmp_path, name)

        def raise_on_col2(**kwargs):  # type: ignore[no-untyped-def]
            if kwargs["collection"] == "col2":
                raise ValueError("Something went wrong")
            return PullResult(
                success=True,
                files_downloaded=1,
                files_skipped=0,
                local_version="1.0.0",
                remote_version="1.0.1",
            )

        mock_pull.side_effect = raise_on_col2

        result = pull_all_collections(
            remote_url="s3://bucket/catalog",
            local_root=tmp_path,
            workers=3,
        )

        assert result.success is False
        assert result.successful_collections == 2
        assert result.failed_collections == 1
        assert "col2" in result.collection_errors

    @patch("portolan_cli.pull.pull")
    def test_workers_capped_at_collection_count(self, mock_pull: MagicMock, tmp_path: Path) -> None:
        """Workers are capped at the number of collections (no wasted threads)."""
        _setup_valid_catalog(tmp_path)
        # Only 2 collections
        _create_collection(tmp_path, "col1")
        _create_collection(tmp_path, "col2")

        mock_pull.return_value = PullResult(
            success=True,
            files_downloaded=1,
            files_skipped=0,
            local_version="1.0.0",
            remote_version="1.0.1",
        )

        # Request 10 workers, but only 2 collections
        result = pull_all_collections(
            remote_url="s3://bucket/catalog",
            local_root=tmp_path,
            workers=10,
        )

        assert result.success is True
        assert mock_pull.call_count == 2

    @patch("portolan_cli.pull.pull")
    def test_dry_run_with_parallel(self, mock_pull: MagicMock, tmp_path: Path) -> None:
        """Dry run works correctly with parallel execution."""
        _setup_valid_catalog(tmp_path)
        for name in ["col1", "col2"]:
            _create_collection(tmp_path, name)

        mock_pull.return_value = PullResult(
            success=True,
            files_downloaded=0,
            files_skipped=0,
            local_version="1.0.0",
            remote_version=None,
            dry_run=True,
        )

        result = pull_all_collections(
            remote_url="s3://bucket/catalog",
            local_root=tmp_path,
            workers=2,
            dry_run=True,
        )

        assert result.success is True
        # Verify dry_run was passed to individual pull calls
        for call in mock_pull.call_args_list:
            assert call.kwargs["dry_run"] is True

    @patch("portolan_cli.pull.pull")
    def test_parallel_handles_unexpected_exception_types(
        self, mock_pull: MagicMock, tmp_path: Path
    ) -> None:
        """Parallel execution catches unexpected exception types."""
        _setup_valid_catalog(tmp_path)
        for name in ["col1", "col2", "col3"]:
            _create_collection(tmp_path, name)

        def raise_unexpected(**kwargs):  # type: ignore[no-untyped-def]
            if kwargs["collection"] == "col2":
                raise KeyError("unexpected_key")
            return PullResult(
                success=True,
                files_downloaded=1,
                files_skipped=0,
                local_version="1.0.0",
                remote_version="1.0.1",
            )

        mock_pull.side_effect = raise_unexpected

        result = pull_all_collections(
            remote_url="s3://bucket/catalog",
            local_root=tmp_path,
            workers=3,
        )

        assert result.success is False
        assert result.successful_collections == 2
        assert result.failed_collections == 1
        assert "col2" in result.collection_errors
        # Verify error message includes exception type name
        assert "KeyError" in result.collection_errors["col2"][0]
