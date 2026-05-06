"""Integration tests for version commands with file backend."""

from __future__ import annotations

import json
from pathlib import Path

import pytest
from click.testing import CliRunner

from portolan_cli.cli import cli


@pytest.fixture
def catalog_with_versions(tmp_path: Path) -> Path:
    """Create a catalog with versions.json for testing."""
    # Create .portolan/config.yaml (catalog marker)
    portolan_dir = tmp_path / ".portolan"
    portolan_dir.mkdir()
    (portolan_dir / "config.yaml").write_text("version: 1\n")

    # Create a collection with versions.json
    collection = tmp_path / "boundaries"
    collection.mkdir()

    versions_data = {
        "spec_version": "1.0.0",
        "current_version": "2.1.0",
        "versions": [
            {
                "version": "1.0.0",
                "created": "2024-01-15T10:30:00Z",
                "breaking": False,
                "assets": {
                    "data.parquet": {
                        "sha256": "abc",
                        "size_bytes": 100,
                        "href": "boundaries/data.parquet",
                    }
                },
                "changes": ["data.parquet"],
                "message": "Initial release",
            },
            {
                "version": "2.0.0",
                "created": "2024-02-01T10:30:00Z",
                "breaking": True,
                "assets": {
                    "data.parquet": {
                        "sha256": "def",
                        "size_bytes": 200,
                        "href": "boundaries/data.parquet",
                    }
                },
                "changes": ["data.parquet"],
                "message": "Schema change",
            },
            {
                "version": "2.1.0",
                "created": "2024-03-01T10:30:00Z",
                "breaking": False,
                "assets": {
                    "data.parquet": {
                        "sha256": "ghi",
                        "size_bytes": 300,
                        "href": "boundaries/data.parquet",
                    }
                },
                "changes": ["data.parquet"],
                "message": "Data update",
            },
        ],
    }
    (collection / "versions.json").write_text(json.dumps(versions_data))

    return tmp_path


class TestVersionCurrentFileBackend:
    """Tests for 'portolan version current' with file backend."""

    @pytest.mark.integration
    def test_current_shows_version(self, catalog_with_versions: Path) -> None:
        """version current shows the current version."""
        runner = CliRunner()
        result = runner.invoke(
            cli,
            ["version", "current", "boundaries", "--catalog", str(catalog_with_versions)],
        )

        assert result.exit_code == 0
        assert "2.1.0" in result.output
        assert "boundaries" in result.output

    @pytest.mark.integration
    def test_current_json_output(self, catalog_with_versions: Path) -> None:
        """version current outputs valid JSON."""
        runner = CliRunner()
        result = runner.invoke(
            cli,
            ["version", "current", "boundaries", "--catalog", str(catalog_with_versions), "--json"],
        )

        assert result.exit_code == 0
        data = json.loads(result.output)
        assert data["success"] is True
        assert data["data"]["version"] == "2.1.0"
        assert data["data"]["collection"] == "boundaries"


class TestVersionListFileBackend:
    """Tests for 'portolan version list' with file backend."""

    @pytest.mark.integration
    def test_list_shows_all_versions(self, catalog_with_versions: Path) -> None:
        """version list shows all versions in history."""
        runner = CliRunner()
        result = runner.invoke(
            cli,
            ["version", "list", "boundaries", "--catalog", str(catalog_with_versions)],
        )

        assert result.exit_code == 0
        assert "1.0.0" in result.output
        assert "2.0.0" in result.output
        assert "2.1.0" in result.output
        assert "3 total" in result.output

    @pytest.mark.integration
    def test_list_json_output(self, catalog_with_versions: Path) -> None:
        """version list outputs valid JSON with all versions."""
        runner = CliRunner()
        result = runner.invoke(
            cli,
            ["version", "list", "boundaries", "--catalog", str(catalog_with_versions), "--json"],
        )

        assert result.exit_code == 0
        data = json.loads(result.output)
        assert data["success"] is True
        assert len(data["data"]["versions"]) == 3
        versions = [v["version"] for v in data["data"]["versions"]]
        assert "1.0.0" in versions
        assert "2.0.0" in versions
        assert "2.1.0" in versions

    @pytest.mark.integration
    def test_list_shows_breaking_flag(self, catalog_with_versions: Path) -> None:
        """version list indicates breaking changes."""
        runner = CliRunner()
        result = runner.invoke(
            cli,
            ["version", "list", "boundaries", "--catalog", str(catalog_with_versions)],
        )

        assert result.exit_code == 0
        assert "[BREAKING]" in result.output


class TestVersionErrorHandling:
    """Tests for error handling in version commands."""

    @pytest.mark.integration
    def test_current_missing_collection(self, catalog_with_versions: Path) -> None:
        """version current handles missing collection gracefully."""
        runner = CliRunner()
        result = runner.invoke(
            cli,
            ["version", "current", "nonexistent", "--catalog", str(catalog_with_versions)],
        )

        assert result.exit_code == 1
        assert "not found" in result.output.lower() or "error" in result.output.lower()

    @pytest.mark.integration
    def test_list_missing_collection_json(self, catalog_with_versions: Path) -> None:
        """version list returns empty versions for nonexistent collection."""
        runner = CliRunner()
        result = runner.invoke(
            cli,
            ["version", "list", "nonexistent", "--catalog", str(catalog_with_versions), "--json"],
        )

        # File backend returns empty list for nonexistent collection (valid state)
        assert result.exit_code == 0
        data = json.loads(result.output)
        assert data["success"] is True
        assert data["data"]["versions"] == []
