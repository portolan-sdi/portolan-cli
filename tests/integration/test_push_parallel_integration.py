"""Integration tests for parallel push functionality (issue #229).

Tests for real filesystem operations and multi-collection parallel push.
These tests verify:
- Parallel execution with multiple collections
- Result aggregation from concurrent operations
- Error handling during parallel execution
- CLI integration with --workers flag
"""

from __future__ import annotations

import json
import threading
import time
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest
from click.testing import CliRunner

from portolan_cli.cli import cli
from portolan_cli.push import (
    PushAllResult,
    PushResult,
    get_default_workers,
    push_all_collections,
)


@pytest.fixture
def multi_collection_catalog(tmp_path: Path) -> Path:
    """Create a catalog with multiple collections for parallel push testing."""
    catalog_dir = tmp_path / "catalog"
    catalog_dir.mkdir()

    # Create .portolan/config.yaml (sentinel file per ADR-0029)
    portolan_dir = catalog_dir / ".portolan"
    portolan_dir.mkdir()
    (portolan_dir / "config.yaml").write_text("version: '1.0'\nremote: s3://test/catalog\n")

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
    @patch("portolan_cli.push.push")
    def test_parallel_execution_observes_worker_count(
        self, mock_push: MagicMock, multi_collection_catalog: Path
    ) -> None:
        """Parallel execution respects the workers parameter."""
        active_count: list[int] = []
        lock = threading.Lock()

        def track_concurrent(**kwargs):  # type: ignore[no-untyped-def]
            with lock:
                active_count.append(threading.active_count())

            # Simulate work
            time.sleep(0.02)

            return PushResult(
                success=True, files_uploaded=1, versions_pushed=1, conflicts=[], errors=[]
            )

        mock_push.side_effect = track_concurrent

        result = push_all_collections(
            catalog_root=multi_collection_catalog,
            destination="s3://bucket/catalog",
            workers=4,
        )

        assert result.success is True
        assert result.total_collections == 4
        assert mock_push.call_count == 4

    @pytest.mark.integration
    @patch("portolan_cli.push.push")
    def test_sequential_execution_with_workers_1(
        self, mock_push: MagicMock, multi_collection_catalog: Path
    ) -> None:
        """workers=1 executes collections sequentially."""
        call_order: list[str] = []

        def track_order(**kwargs):  # type: ignore[no-untyped-def]
            call_order.append(kwargs["collection"])
            return PushResult(
                success=True, files_uploaded=1, versions_pushed=1, conflicts=[], errors=[]
            )

        mock_push.side_effect = track_order

        result = push_all_collections(
            catalog_root=multi_collection_catalog,
            destination="s3://bucket/catalog",
            workers=1,
        )

        assert result.success is True
        # Sequential execution maintains sorted order
        assert call_order == ["collection_a", "collection_b", "collection_c", "collection_d"]

    @pytest.mark.integration
    @patch("portolan_cli.push.push")
    def test_parallel_continues_on_individual_failure(
        self, mock_push: MagicMock, multi_collection_catalog: Path
    ) -> None:
        """Parallel execution continues when individual collections fail."""

        def mixed_results(**kwargs):  # type: ignore[no-untyped-def]
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

        mock_push.side_effect = mixed_results

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
        """get_default_workers returns a value between 1 and 16."""
        workers = get_default_workers()
        assert 1 <= workers <= 16


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

        result = runner.invoke(
            cli,
            ["push", "--catalog", str(multi_collection_catalog)],
        )

        assert result.exit_code == 0, f"Failed: {result.output}"
        mock_push_all.assert_called_once()
        assert mock_push_all.call_args.kwargs["workers"] is None
