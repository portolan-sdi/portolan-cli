"""Integration tests for portolan status CLI command."""

from __future__ import annotations

import json
from pathlib import Path

import pytest
from click.testing import CliRunner

from portolan_cli.cli import cli


@pytest.fixture
def initialized_catalog(tmp_path: Path) -> Path:
    """Create an initialized catalog with a collection and versions.json."""
    # Create .portolan/config.yaml (catalog marker)
    portolan_dir = tmp_path / ".portolan"
    portolan_dir.mkdir()
    (portolan_dir / "config.yaml").write_text("version: 1\n")

    # Create a collection with versions.json
    collection = tmp_path / "demographics"
    collection.mkdir()

    # Create versions.json
    versions_data = {
        "spec_version": "1.0.0",
        "current_version": "1.2.0",
        "versions": [
            {
                "version": "1.0.0",
                "created": "2024-01-15T10:30:00Z",
                "breaking": False,
                "assets": {},
                "changes": [],
            },
            {
                "version": "1.2.0",
                "created": "2024-02-01T10:30:00Z",
                "breaking": False,
                "assets": {
                    "data.parquet": {
                        "sha256": "abc123",
                        "size_bytes": 1024,
                        "href": "demographics/data.parquet",
                    }
                },
                "changes": ["data.parquet"],
            },
        ],
    }
    (collection / "versions.json").write_text(json.dumps(versions_data))

    return tmp_path


class TestStatusCLI:
    """Integration tests for portolan status command."""

    @pytest.mark.integration
    def test_status_shows_local_version(self, initialized_catalog: Path, tmp_path: Path) -> None:
        """Status command shows local version from versions.json."""
        runner = CliRunner()
        result = runner.invoke(
            cli,
            ["status", "--catalog", str(initialized_catalog), "--offline"],
        )

        assert result.exit_code == 0
        assert "demographics" in result.output
        assert "1.2.0" in result.output

    @pytest.mark.integration
    def test_status_json_output(self, initialized_catalog: Path, tmp_path: Path) -> None:
        """Status command outputs valid JSON with --json flag."""
        runner = CliRunner()
        result = runner.invoke(
            cli,
            ["status", "--catalog", str(initialized_catalog), "--offline", "--json"],
        )

        assert result.exit_code == 0
        data = json.loads(result.output)
        assert data["success"] is True
        assert "collections" in data["data"]
        assert len(data["data"]["collections"]) == 1
        assert data["data"]["collections"][0]["collection"] == "demographics"
        assert data["data"]["collections"][0]["local_version"] == "1.2.0"

    @pytest.mark.integration
    def test_status_collection_filter(self, initialized_catalog: Path, tmp_path: Path) -> None:
        """Status command filters by collection with -c flag."""
        runner = CliRunner()
        result = runner.invoke(
            cli,
            [
                "status",
                "--catalog",
                str(initialized_catalog),
                "-c",
                "demographics",
                "--offline",
            ],
        )

        assert result.exit_code == 0
        assert "demographics" in result.output

    @pytest.mark.integration
    def test_status_no_collections(self, tmp_path: Path) -> None:
        """Status command handles empty catalog gracefully."""
        # Create empty catalog
        portolan_dir = tmp_path / ".portolan"
        portolan_dir.mkdir()
        (portolan_dir / "config.yaml").write_text("version: 1\n")

        runner = CliRunner()
        result = runner.invoke(
            cli,
            ["status", "--catalog", str(tmp_path), "--offline"],
        )

        assert result.exit_code == 0
        assert "No collections found" in result.output

    @pytest.mark.integration
    def test_status_detects_deleted_files(self, initialized_catalog: Path, tmp_path: Path) -> None:
        """Status shows files in versions.json but missing from disk."""
        runner = CliRunner()
        result = runner.invoke(
            cli,
            ["status", "--catalog", str(initialized_catalog), "--offline", "--json"],
        )

        assert result.exit_code == 0
        data = json.loads(result.output)
        # data.parquet is in versions.json but doesn't exist on disk
        assert "data.parquet" in data["data"]["collections"][0]["deleted_files"]

    @pytest.mark.integration
    def test_status_offline_skips_remote(self, initialized_catalog: Path, tmp_path: Path) -> None:
        """Offline mode sets remote_version to None."""
        runner = CliRunner()
        result = runner.invoke(
            cli,
            ["status", "--catalog", str(initialized_catalog), "--offline", "--json"],
        )

        assert result.exit_code == 0
        data = json.loads(result.output)
        assert data["data"]["collections"][0]["remote_version"] is None
        assert data["data"]["collections"][0]["sync_state"] == "unknown"
