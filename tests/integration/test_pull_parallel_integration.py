"""Integration tests for parallel pull functionality.

Tests the pull_all_collections() function with real filesystem operations.
"""

from __future__ import annotations

import json
from pathlib import Path

import pytest

from portolan_cli.pull import pull_all_collections

# Mark all tests in this module as integration tests
pytestmark = pytest.mark.integration


def _setup_catalog(catalog_root: Path, collections: list[str]) -> None:
    """Helper to create a catalog with multiple collections."""
    portolan_dir = catalog_root / ".portolan"
    portolan_dir.mkdir(parents=True, exist_ok=True)
    (portolan_dir / "config.yaml").write_text("version: '1.0'\n")

    for name in collections:
        collection_dir = catalog_root / name
        collection_dir.mkdir()
        # Simple versions.json with no assets (for dry-run testing)
        versions_data = {
            "spec_version": "1.0.0",
            "current_version": "1.0.0",
            "versions": [],
        }
        (collection_dir / "versions.json").write_text(json.dumps(versions_data, indent=2))


class TestPullAllCollectionsIntegration:
    """Integration tests for pull_all_collections()."""

    def test_empty_catalog_returns_failure(self, tmp_path: Path) -> None:
        """pull_all_collections with no collections returns failure."""
        catalog_root = tmp_path / "catalog"
        catalog_root.mkdir()
        portolan_dir = catalog_root / ".portolan"
        portolan_dir.mkdir()
        (portolan_dir / "config.yaml").write_text("version: '1.0'\n")

        result = pull_all_collections(
            remote_url="s3://bucket/catalog",
            local_root=catalog_root,
            workers=1,
        )

        assert result.success is False
        assert result.total_collections == 0

    def test_discovers_all_collections(self, tmp_path: Path) -> None:
        """pull_all_collections discovers all collections in catalog."""
        catalog_root = tmp_path / "catalog"
        catalog_root.mkdir()
        _setup_catalog(catalog_root, ["climate", "environment", "water"])

        # Use dry_run to avoid network calls
        result = pull_all_collections(
            remote_url="s3://bucket/catalog",
            local_root=catalog_root,
            workers=1,
            dry_run=True,
        )

        # Dry run succeeds (no network)
        assert result.total_collections == 3

    def test_workers_parameter_accepted(self, tmp_path: Path) -> None:
        """CLI --workers parameter is correctly passed through."""
        catalog_root = tmp_path / "catalog"
        catalog_root.mkdir()
        _setup_catalog(catalog_root, ["col1", "col2"])

        # Different worker counts should not error
        for workers in [1, 2, 4]:
            result = pull_all_collections(
                remote_url="s3://bucket/catalog",
                local_root=catalog_root,
                workers=workers,
                dry_run=True,
            )
            assert result.total_collections == 2

    def test_invalid_catalog_raises_error(self, tmp_path: Path) -> None:
        """pull_all_collections with invalid catalog raises ValueError."""
        # Directory without .portolan/config.yaml
        invalid_catalog = tmp_path / "not_a_catalog"
        invalid_catalog.mkdir()

        with pytest.raises(ValueError, match="Not a portolan catalog"):
            pull_all_collections(
                remote_url="s3://bucket/catalog",
                local_root=invalid_catalog,
                workers=1,
            )


class TestPullCommandIntegration:
    """Integration tests for CLI pull command with --workers."""

    def test_pull_all_collections_cli(self, tmp_path: Path) -> None:
        """portolan pull without --collection pulls all."""
        from click.testing import CliRunner

        from portolan_cli.cli import cli

        catalog_root = tmp_path / "catalog"
        catalog_root.mkdir()
        _setup_catalog(catalog_root, ["col1", "col2"])

        runner = CliRunner()
        result = runner.invoke(
            cli,
            [
                "pull",
                "s3://bucket/catalog",
                "--catalog",
                str(catalog_root),
                "--dry-run",
            ],
        )

        # Should attempt to pull all collections
        assert "Found 2 collection(s)" in result.output

    def test_pull_workers_flag(self, tmp_path: Path) -> None:
        """portolan pull --workers flag is accepted."""
        from click.testing import CliRunner

        from portolan_cli.cli import cli

        catalog_root = tmp_path / "catalog"
        catalog_root.mkdir()
        _setup_catalog(catalog_root, ["col1", "col2"])

        runner = CliRunner()
        result = runner.invoke(
            cli,
            [
                "pull",
                "s3://bucket/catalog",
                "--catalog",
                str(catalog_root),
                "--workers",
                "2",
                "--dry-run",
            ],
        )

        assert result.exit_code == 0
        assert "Using 2 parallel worker(s)" in result.output

    def test_pull_workers_validation(self, tmp_path: Path) -> None:
        """portolan pull --workers rejects invalid values."""
        from click.testing import CliRunner

        from portolan_cli.cli import cli

        catalog_root = tmp_path / "catalog"
        catalog_root.mkdir()
        _setup_catalog(catalog_root, ["col1"])

        runner = CliRunner()

        # workers=0 should be rejected
        result = runner.invoke(
            cli,
            [
                "pull",
                "s3://bucket/catalog",
                "--catalog",
                str(catalog_root),
                "--workers",
                "0",
            ],
        )
        assert result.exit_code != 0
        assert "0 is not in the range" in result.output

        # Negative workers should be rejected
        result = runner.invoke(
            cli,
            [
                "pull",
                "s3://bucket/catalog",
                "--catalog",
                str(catalog_root),
                "--workers",
                "-1",
            ],
        )
        assert result.exit_code != 0

    def test_pull_single_collection_ignores_workers(self, tmp_path: Path) -> None:
        """portolan pull --collection ignores --workers flag."""
        from click.testing import CliRunner

        from portolan_cli.cli import cli

        catalog_root = tmp_path / "catalog"
        catalog_root.mkdir()
        _setup_catalog(catalog_root, ["col1", "col2"])

        runner = CliRunner()
        result = runner.invoke(
            cli,
            [
                "pull",
                "s3://bucket/catalog",
                "--catalog",
                str(catalog_root),
                "--collection",
                "col1",
                "--workers",
                "4",  # Should be ignored for single collection
                "--dry-run",
            ],
        )

        # Should NOT mention "parallel workers" since single collection
        assert "parallel worker" not in result.output.lower()
