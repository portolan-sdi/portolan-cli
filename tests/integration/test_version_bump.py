"""Integration tests for portolan version bump command."""

from __future__ import annotations

import hashlib
import json
from pathlib import Path

import pytest
from click.testing import CliRunner

from portolan_cli.cli import cli


def _sha256(content: bytes) -> str:
    """Compute SHA256 hash of content."""
    return hashlib.sha256(content).hexdigest()


@pytest.fixture
def catalog_with_tracked_file(tmp_path: Path) -> Path:
    """Create a catalog with a tracked file that can be modified."""
    # Create .portolan/config.yaml (catalog marker)
    portolan_dir = tmp_path / ".portolan"
    portolan_dir.mkdir()
    (portolan_dir / "config.yaml").write_text("version: 1\n")

    # Create a collection with a file
    collection = tmp_path / "demographics"
    collection.mkdir()

    # Create the actual file
    data_file = collection / "data.parquet"
    original_content = b"original content"
    data_file.write_bytes(original_content)

    # Create versions.json tracking this file
    checksum = _sha256(original_content)
    versions_data = {
        "spec_version": "1.0.0",
        "current_version": "1.0.0",
        "versions": [
            {
                "version": "1.0.0",
                "created": "2024-01-15T10:30:00Z",
                "breaking": False,
                "assets": {
                    "data.parquet": {
                        "sha256": checksum,
                        "size_bytes": len(original_content),
                        "href": "demographics/data.parquet",
                    }
                },
                "changes": ["data.parquet"],
                "message": "Initial release",
            }
        ],
    }
    (collection / "versions.json").write_text(json.dumps(versions_data))

    return tmp_path


class TestVersionBump:
    """Integration tests for 'portolan version bump'."""

    @pytest.mark.integration
    def test_bump_detects_no_changes(self, catalog_with_tracked_file: Path) -> None:
        """Bump reports no changes when file is unchanged."""
        runner = CliRunner()
        result = runner.invoke(
            cli,
            [
                "version",
                "bump",
                "demographics",
                "1.1.0",
                "--catalog",
                str(catalog_with_tracked_file),
                "-y",
            ],
        )

        assert result.exit_code == 0
        assert "No changes detected" in result.output

    @pytest.mark.integration
    def test_bump_detects_modified_file(self, catalog_with_tracked_file: Path) -> None:
        """Bump detects when a tracked file has changed."""
        # Modify the file
        data_file = catalog_with_tracked_file / "demographics" / "data.parquet"
        data_file.write_bytes(b"modified content")

        runner = CliRunner()
        result = runner.invoke(
            cli,
            [
                "version",
                "bump",
                "demographics",
                "1.1.0",
                "--catalog",
                str(catalog_with_tracked_file),
                "-y",
                "-m",
                "Updated data",
            ],
        )

        assert result.exit_code == 0
        assert "Created version 1.1.0" in result.output

    @pytest.mark.integration
    def test_bump_json_output(self, catalog_with_tracked_file: Path) -> None:
        """Bump outputs valid JSON with --json flag."""
        # Modify the file
        data_file = catalog_with_tracked_file / "demographics" / "data.parquet"
        data_file.write_bytes(b"modified content")

        runner = CliRunner()
        result = runner.invoke(
            cli,
            [
                "version",
                "bump",
                "demographics",
                "1.1.0",
                "--catalog",
                str(catalog_with_tracked_file),
                "-y",
                "--json",
            ],
        )

        assert result.exit_code == 0
        data = json.loads(result.output)
        assert data["success"] is True
        assert data["data"]["version"] == "1.1.0"
        assert data["data"]["previous_version"] == "1.0.0"
        assert "data.parquet" in data["data"]["modified_files"]

    @pytest.mark.integration
    def test_bump_with_breaking_flag(self, catalog_with_tracked_file: Path) -> None:
        """Bump records breaking change flag."""
        # Modify the file
        data_file = catalog_with_tracked_file / "demographics" / "data.parquet"
        data_file.write_bytes(b"breaking change content")

        runner = CliRunner()
        result = runner.invoke(
            cli,
            [
                "version",
                "bump",
                "demographics",
                "2.0.0",
                "--catalog",
                str(catalog_with_tracked_file),
                "-y",
                "--breaking",
                "-m",
                "Schema change",
                "--json",
            ],
        )

        assert result.exit_code == 0
        data = json.loads(result.output)
        assert data["data"]["breaking"] is True
        assert data["data"]["message"] == "Schema change"

    @pytest.mark.integration
    def test_bump_detects_deleted_file(self, catalog_with_tracked_file: Path) -> None:
        """Bump detects when a tracked file has been deleted."""
        # Delete the tracked file
        data_file = catalog_with_tracked_file / "demographics" / "data.parquet"
        data_file.unlink()

        runner = CliRunner()
        result = runner.invoke(
            cli,
            [
                "version",
                "bump",
                "demographics",
                "1.1.0",
                "--catalog",
                str(catalog_with_tracked_file),
                "-y",
                "--json",
            ],
        )

        assert result.exit_code == 0
        data = json.loads(result.output)
        assert "data.parquet" in data["data"]["deleted_files"]

    @pytest.mark.integration
    def test_bump_requires_versions_json(self, tmp_path: Path) -> None:
        """Bump fails if collection has no versions.json."""
        # Create minimal catalog without versions.json
        portolan_dir = tmp_path / ".portolan"
        portolan_dir.mkdir()
        (portolan_dir / "config.yaml").write_text("version: 1\n")
        (tmp_path / "empty_collection").mkdir()

        runner = CliRunner()
        result = runner.invoke(
            cli,
            [
                "version",
                "bump",
                "empty_collection",
                "1.0.0",
                "--catalog",
                str(tmp_path),
                "-y",
            ],
        )

        assert result.exit_code == 1
        assert "no versions.json" in result.output.lower()


class TestVersionBumpConfirmation:
    """Tests for bump confirmation prompt."""

    @pytest.mark.integration
    def test_bump_aborts_on_no(self, catalog_with_tracked_file: Path) -> None:
        """Bump aborts when user says no to confirmation."""
        # Modify the file
        data_file = catalog_with_tracked_file / "demographics" / "data.parquet"
        data_file.write_bytes(b"modified content")

        runner = CliRunner()
        result = runner.invoke(
            cli,
            [
                "version",
                "bump",
                "demographics",
                "1.1.0",
                "--catalog",
                str(catalog_with_tracked_file),
            ],
            input="n\n",  # Answer "no" to confirmation
        )

        assert result.exit_code == 0
        assert "Aborted" in result.output

    @pytest.mark.integration
    def test_bump_proceeds_with_yes_flag(self, catalog_with_tracked_file: Path) -> None:
        """Bump skips confirmation with -y flag."""
        # Modify the file
        data_file = catalog_with_tracked_file / "demographics" / "data.parquet"
        data_file.write_bytes(b"modified content")

        runner = CliRunner()
        result = runner.invoke(
            cli,
            [
                "version",
                "bump",
                "demographics",
                "1.1.0",
                "--catalog",
                str(catalog_with_tracked_file),
                "-y",
            ],
        )

        assert result.exit_code == 0
        assert "Created version 1.1.0" in result.output
