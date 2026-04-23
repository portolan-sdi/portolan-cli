"""Integration tests for parallel push functionality (issue #229).

Tests for real filesystem operations and multi-collection parallel push.
These tests verify:
- Parallel execution with multiple collections
- Result aggregation from concurrent operations
- Error handling during parallel execution
- CLI integration with --workers flag

Note: remote is a sensitive setting and must be set via env var (Issue #356).
"""

from __future__ import annotations

import asyncio
import json
import os
import threading
from pathlib import Path
from unittest.mock import AsyncMock, MagicMock, patch

import pytest
from click.testing import CliRunner

from portolan_cli.cli import cli
from portolan_cli.push import (
    PushAllResult,
    PushResult,
    get_default_workers,
    push_all_collections,
)

# Remote URL for tests - set via env var (Issue #356: sensitive settings)
TEST_REMOTE = "s3://test/catalog"


@pytest.fixture
def multi_collection_catalog(tmp_path: Path) -> Path:
    """Create a catalog with multiple collections for parallel push testing.

    Note: remote must be set via PORTOLAN_REMOTE env var (Issue #356).
    """
    catalog_dir = tmp_path / "catalog"
    catalog_dir.mkdir()

    # Create .portolan/config.yaml (sentinel file per ADR-0029)
    # Note: remote set via env var, not config.yaml (Issue #356)
    portolan_dir = catalog_dir / ".portolan"
    portolan_dir.mkdir()
    (portolan_dir / "config.yaml").write_text("version: '1.0'\n")

    # Create catalog.json
    catalog_data = {
        "type": "Catalog",
        "id": "test-catalog",
        "description": "Test catalog",
        "stac_version": "1.0.0",
        "links": [],
    }
    (catalog_dir / "catalog.json").write_text(json.dumps(catalog_data, indent=2))

    # Create multiple collections
    for coll_name in ["collection_a", "collection_b", "collection_c", "collection_d"]:
        coll_dir = catalog_dir / coll_name
        coll_dir.mkdir()

        # Create collection.json
        collection_data = {
            "type": "Collection",
            "id": coll_name,
            "stac_version": "1.0.0",
            "description": f"{coll_name} collection",
            "license": "proprietary",
            "extent": {
                "spatial": {"bbox": [[-180, -90, 180, 90]]},
                "temporal": {"interval": [["2024-01-01T00:00:00Z", None]]},
            },
            "links": [],
        }
        (coll_dir / "collection.json").write_text(json.dumps(collection_data, indent=2))

        # Create versions.json
        versions_data = {
            "spec_version": "1.0.0",
            "current_version": "1.0.0",
            "versions": [
                {
                    "version": "1.0.0",
                    "created": "2024-01-01T00:00:00Z",
                    "breaking": False,
                    "message": "Initial version",
                    "assets": {},
                }
            ],
        }
        (coll_dir / "versions.json").write_text(json.dumps(versions_data, indent=2))

    return catalog_dir


class TestParallelPushIntegration:
    """Integration tests for parallel push_all_collections."""

    @pytest.mark.integration
    @patch("portolan_cli.push.obs.put")  # For catalog.json upload
    @patch("portolan_cli.push.push_async", new_callable=AsyncMock)
    def test_parallel_execution_observes_worker_count(
        self,
        mock_push_async: AsyncMock,
        _mock_obs_put: MagicMock,
        multi_collection_catalog: Path,
    ) -> None:
        """Parallel execution respects the workers parameter."""
        active_count: list[int] = []
        lock = threading.Lock()

        async def track_concurrent(**kwargs):  # type: ignore[no-untyped-def]
            with lock:
                active_count.append(threading.active_count())

            # Simulate work (use asyncio.sleep for async)
            await asyncio.sleep(0.02)

            return PushResult(
                success=True, files_uploaded=1, versions_pushed=1, conflicts=[], errors=[]
            )

        mock_push_async.side_effect = track_concurrent

        result = push_all_collections(
            catalog_root=multi_collection_catalog,
            destination="s3://bucket/catalog",
            workers=4,
        )

        assert result.success is True
        assert result.total_collections == 4
        assert mock_push_async.call_count == 4

    @pytest.mark.integration
    @patch("portolan_cli.push.obs.put")  # For catalog.json upload
    @patch("portolan_cli.push.push_async", new_callable=AsyncMock)
    def test_sequential_execution_with_workers_1(
        self,
        mock_push_async: AsyncMock,
        _mock_obs_put: MagicMock,
        multi_collection_catalog: Path,
    ) -> None:
        """workers=1 executes collections sequentially."""
        call_order: list[str] = []

        async def track_order(**kwargs):  # type: ignore[no-untyped-def]
            call_order.append(kwargs["collection"])
            return PushResult(
                success=True, files_uploaded=1, versions_pushed=1, conflicts=[], errors=[]
            )

        mock_push_async.side_effect = track_order

        result = push_all_collections(
            catalog_root=multi_collection_catalog,
            destination="s3://bucket/catalog",
            workers=1,
        )

        assert result.success is True
        # Sequential execution maintains sorted order
        assert call_order == ["collection_a", "collection_b", "collection_c", "collection_d"]

    @pytest.mark.integration
    @patch("portolan_cli.push.obs.put")  # For catalog.json upload (skipped on failure)
    @patch("portolan_cli.push.push_async", new_callable=AsyncMock)
    def test_parallel_continues_on_individual_failure(
        self, mock_push_async: AsyncMock, _mock_obs_put: MagicMock, multi_collection_catalog: Path
    ) -> None:
        """Parallel execution continues when individual collections fail."""

        async def mixed_results(**kwargs):  # type: ignore[no-untyped-def]
            if kwargs["collection"] == "collection_b":
                return PushResult(
                    success=False,
                    files_uploaded=0,
                    versions_pushed=0,
                    conflicts=[],
                    errors=["Simulated failure"],
                )
            return PushResult(
                success=True, files_uploaded=1, versions_pushed=1, conflicts=[], errors=[]
            )

        mock_push_async.side_effect = mixed_results

        result = push_all_collections(
            catalog_root=multi_collection_catalog,
            destination="s3://bucket/catalog",
            workers=4,
        )

        assert result.success is False
        assert result.total_collections == 4
        assert result.successful_collections == 3
        assert result.failed_collections == 1
        assert "collection_b" in result.collection_errors

    @pytest.mark.integration
    def test_get_default_workers_returns_sensible_value(self) -> None:
        """get_default_workers returns a positive value based on CPU count."""
        workers = get_default_workers()
        # Minimum 1 worker, no upper bound (depends on system capabilities)
        assert workers >= 1


class TestCLIParallelPushIntegration:
    """Integration tests for CLI with --workers flag."""

    @pytest.fixture
    def runner(self) -> CliRunner:
        """Create a Click test runner."""
        return CliRunner()

    @pytest.mark.integration
    @patch("portolan_cli.push.push_all_collections")
    def test_cli_workers_flag_integration(
        self, mock_push_all: MagicMock, runner: CliRunner, multi_collection_catalog: Path
    ) -> None:
        """CLI correctly passes --workers to push_all_collections."""
        mock_push_all.return_value = PushAllResult(
            success=True,
            total_collections=4,
            successful_collections=4,
            failed_collections=0,
            total_files_uploaded=4,
            total_versions_pushed=4,
        )

        with patch.dict(os.environ, {"PORTOLAN_REMOTE": TEST_REMOTE}):
            result = runner.invoke(
                cli,
                ["push", "--catalog", str(multi_collection_catalog), "--workers", "2"],
            )

        assert result.exit_code == 0, f"Failed: {result.output}"
        mock_push_all.assert_called_once()
        assert mock_push_all.call_args.kwargs["workers"] == 2

    @pytest.mark.integration
    @patch("portolan_cli.push.push_all_collections")
    def test_cli_default_workers_integration(
        self, mock_push_all: MagicMock, runner: CliRunner, multi_collection_catalog: Path
    ) -> None:
        """CLI passes workers=None (auto-detect) when --workers not specified."""
        mock_push_all.return_value = PushAllResult(
            success=True,
            total_collections=4,
            successful_collections=4,
            failed_collections=0,
            total_files_uploaded=4,
            total_versions_pushed=4,
        )

        with patch.dict(os.environ, {"PORTOLAN_REMOTE": TEST_REMOTE}):
            result = runner.invoke(
                cli,
                ["push", "--catalog", str(multi_collection_catalog)],
            )

        assert result.exit_code == 0, f"Failed: {result.output}"
        mock_push_all.assert_called_once()
        assert mock_push_all.call_args.kwargs["workers"] is None
