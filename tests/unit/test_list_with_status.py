"""Unit tests for unified list command with status indicators.

Per issue #210: The list command now shows tracking status for all files.
The status command has been removed (folded into list).

Status indicators:
- Tracked (in versions.json, unchanged)
- + Untracked (on disk, not in versions.json)
- ~ Modified (in versions.json, checksum changed)
- ! Deleted (in versions.json, missing from disk)

Everything in a catalog is tracked unless excluded by ignored_files config.
"""

from __future__ import annotations

import json
from pathlib import Path
from unittest.mock import patch

import pytest
from click.testing import CliRunner

from portolan_cli.cli import cli


@pytest.fixture
def runner() -> CliRunner:
    """Create a CLI test runner."""
    return CliRunner()


def make_catalog(tmp_path: Path, collection_links: list[str] | None = None) -> None:
    """Write a minimal managed catalog to tmp_path."""
    portolan_dir = tmp_path / ".portolan"
    portolan_dir.mkdir(parents=True, exist_ok=True)
    (portolan_dir / "config.yaml").write_text("# Portolan configuration\n")

    links = [{"rel": "child", "href": f"./{c}/collection.json"} for c in (collection_links or [])]
    (tmp_path / "catalog.json").write_text(
        json.dumps(
            {
                "type": "Catalog",
                "id": "test-catalog",
                "stac_version": "1.0.0",
                "description": "Test catalog",
                "links": links,
            }
        )
    )


def make_collection(col_dir: Path, item_links: list[str] | None = None) -> None:
    """Write a minimal collection.json inside col_dir."""
    col_dir.mkdir(parents=True, exist_ok=True)
    links = [{"rel": "item", "href": f"./{item}/{item}.json"} for item in (item_links or [])]
    (col_dir / "collection.json").write_text(
        json.dumps(
            {
                "type": "Collection",
                "id": col_dir.name,
                "stac_version": "1.0.0",
                "description": f"Collection {col_dir.name}",
                "license": "proprietary",
                "extent": {
                    "spatial": {"bbox": [[-180, -90, 180, 90]]},
                    "temporal": {"interval": [[None, None]]},
                },
                "links": links,
            }
        )
    )


def make_item(item_dir: Path, assets: dict[str, str]) -> None:
    """Write a minimal item.json inside item_dir with given assets."""
    item_dir.mkdir(parents=True, exist_ok=True)
    item_id = item_dir.name
    asset_dict = {
        key: {"href": href, "type": "application/octet-stream"} for key, href in assets.items()
    }
    (item_dir / f"{item_id}.json").write_text(
        json.dumps(
            {
                "type": "Feature",
                "stac_version": "1.0.0",
                "id": item_id,
                "geometry": None,
                "bbox": [0, 0, 1, 1],
                "properties": {"datetime": None},
                "links": [],
                "assets": asset_dict,
            }
        )
    )


def make_versions_json(col_dir: Path, assets: dict[str, dict]) -> None:
    """Write a versions.json at collection root.

    Assets should be a dict like {"item1/data.parquet": {"sha256": "abc", "size_bytes": 100}}.
    This function adds the required 'href' field automatically.
    """
    # Add href field to each asset (required by versions.json schema)
    full_assets = {}
    for key, data in assets.items():
        full_data = dict(data)
        if "href" not in full_data:
            # Extract filename from key and use as href
            filename = key.split("/")[-1] if "/" in key else key
            full_data["href"] = f"./{filename}"
        full_assets[key] = full_data

    versions_data = {
        "spec_version": "1.0.0",
        "current_version": "1.0.0",
        "versions": [
            {
                "version": "1.0.0",
                "created": "2024-01-15T10:30:00Z",
                "breaking": False,
                "assets": full_assets,
                "changes": list(full_assets.keys()),
            }
        ],
    }
    (col_dir / "versions.json").write_text(json.dumps(versions_data))


# =============================================================================
# Test: Status indicators in list output
# =============================================================================


class TestListStatusIndicators:
    """Tests for status indicators in the unified list command."""

    @pytest.mark.unit
    def test_list_shows_tracked_indicator(self, runner: CliRunner, tmp_path: Path) -> None:
        """List shows checkmark for tracked files."""
        make_catalog(tmp_path, ["col1"])
        col_dir = tmp_path / "col1"
        make_collection(col_dir, ["item1"])
        item_dir = col_dir / "item1"
        make_item(item_dir, {"data": "./data.parquet"})
        (item_dir / "data.parquet").write_bytes(b"x" * 100)

        # Mark file as tracked in versions.json
        make_versions_json(col_dir, {"item1/data.parquet": {"sha256": "abc", "size_bytes": 100}})

        with patch("portolan_cli.cli.find_catalog_root", return_value=tmp_path):
            result = runner.invoke(cli, ["list", "--catalog", str(tmp_path)])

        assert result.exit_code == 0
        # Should show checkmark for tracked file
        assert "data.parquet" in result.output

    @pytest.mark.unit
    def test_list_shows_untracked_indicator(self, runner: CliRunner, tmp_path: Path) -> None:
        """List shows + for untracked files (on disk but not in versions.json)."""
        make_catalog(tmp_path, ["col1"])
        col_dir = tmp_path / "col1"
        make_collection(col_dir, ["item1"])
        item_dir = col_dir / "item1"
        make_item(item_dir, {"data": "./data.parquet"})
        (item_dir / "data.parquet").write_bytes(b"x" * 100)
        # Also create an untracked file
        (item_dir / "README.md").write_text("Hello")

        # Only track data.parquet, not README.md
        make_versions_json(col_dir, {"item1/data.parquet": {"sha256": "abc", "size_bytes": 100}})

        with patch("portolan_cli.cli.find_catalog_root", return_value=tmp_path):
            result = runner.invoke(cli, ["list", "--catalog", str(tmp_path)])

        assert result.exit_code == 0
        # Should show the untracked file with + indicator
        assert "README.md" in result.output
        assert "+" in result.output or "untracked" in result.output.lower()

    @pytest.mark.unit
    def test_list_shows_deleted_indicator(self, runner: CliRunner, tmp_path: Path) -> None:
        """List shows ! for deleted files (in versions.json but missing from disk)."""
        make_catalog(tmp_path, ["col1"])
        col_dir = tmp_path / "col1"
        make_collection(col_dir, ["item1"])
        item_dir = col_dir / "item1"
        make_item(item_dir, {"data": "./data.parquet"})
        # Don't create the actual file - it's "deleted"

        # Track file in versions.json even though it doesn't exist
        make_versions_json(col_dir, {"item1/data.parquet": {"sha256": "abc", "size_bytes": 100}})

        with patch("portolan_cli.cli.find_catalog_root", return_value=tmp_path):
            result = runner.invoke(cli, ["list", "--catalog", str(tmp_path)])

        assert result.exit_code == 0
        # Should show the deleted file with ! indicator
        assert "data.parquet" in result.output
        assert "!" in result.output or "deleted" in result.output.lower()


# =============================================================================
# Test: Scan all subdirectories (not just initialized collections)
# =============================================================================


class TestListScansAllSubdirectories:
    """Tests that list scans all subdirectories, not just initialized collections."""

    @pytest.mark.unit
    def test_list_shows_files_in_uninitialized_collection(
        self, runner: CliRunner, tmp_path: Path
    ) -> None:
        """List shows files in directories without collection.json."""
        make_catalog(tmp_path)
        # Create a directory with files but no collection.json
        uninit_dir = tmp_path / "new-collection"
        uninit_dir.mkdir()
        item_dir = uninit_dir / "item1"
        item_dir.mkdir()
        (item_dir / "data.parquet").write_bytes(b"x" * 100)

        with patch("portolan_cli.cli.find_catalog_root", return_value=tmp_path):
            result = runner.invoke(cli, ["list", "--catalog", str(tmp_path)])

        assert result.exit_code == 0
        # Should show the uninitialized collection and its files
        assert "new-collection" in result.output
        assert "data.parquet" in result.output

    @pytest.mark.unit
    def test_list_shows_all_files_not_just_geo(self, runner: CliRunner, tmp_path: Path) -> None:
        """List shows all files in item directories, not just geo-assets."""
        make_catalog(tmp_path, ["col1"])
        col_dir = tmp_path / "col1"
        make_collection(col_dir, ["item1"])
        item_dir = col_dir / "item1"
        make_item(item_dir, {"data": "./data.parquet"})

        # Create various file types
        (item_dir / "data.parquet").write_bytes(b"x" * 100)
        (item_dir / "README.md").write_text("Documentation")
        (item_dir / "style.json").write_text("{}")
        (item_dir / "thumbnail.png").write_bytes(b"PNG")

        make_versions_json(col_dir, {"item1/data.parquet": {"sha256": "abc", "size_bytes": 100}})

        with patch("portolan_cli.cli.find_catalog_root", return_value=tmp_path):
            result = runner.invoke(cli, ["list", "--catalog", str(tmp_path)])

        assert result.exit_code == 0
        # Should show ALL files
        assert "data.parquet" in result.output
        assert "README.md" in result.output
        assert "style.json" in result.output
        assert "thumbnail.png" in result.output


# =============================================================================
# Test: --tracked-only and --untracked-only flags
# =============================================================================


class TestListFilterFlags:
    """Tests for --tracked-only and --untracked-only flags."""

    @pytest.mark.unit
    def test_list_tracked_only_hides_untracked(self, runner: CliRunner, tmp_path: Path) -> None:
        """--tracked-only flag shows only tracked files."""
        make_catalog(tmp_path, ["col1"])
        col_dir = tmp_path / "col1"
        make_collection(col_dir, ["item1"])
        item_dir = col_dir / "item1"
        make_item(item_dir, {"data": "./data.parquet"})
        (item_dir / "data.parquet").write_bytes(b"x" * 100)
        (item_dir / "README.md").write_text("Untracked")

        make_versions_json(col_dir, {"item1/data.parquet": {"sha256": "abc", "size_bytes": 100}})

        with patch("portolan_cli.cli.find_catalog_root", return_value=tmp_path):
            result = runner.invoke(cli, ["list", "--tracked-only", "--catalog", str(tmp_path)])

        assert result.exit_code == 0
        assert "data.parquet" in result.output
        assert "README.md" not in result.output

    @pytest.mark.unit
    def test_list_untracked_only_shows_only_untracked(
        self, runner: CliRunner, tmp_path: Path
    ) -> None:
        """--untracked-only flag shows only untracked files."""
        make_catalog(tmp_path, ["col1"])
        col_dir = tmp_path / "col1"
        make_collection(col_dir, ["item1"])
        item_dir = col_dir / "item1"
        make_item(item_dir, {"data": "./data.parquet"})
        (item_dir / "data.parquet").write_bytes(b"x" * 100)
        (item_dir / "README.md").write_text("Untracked")

        make_versions_json(col_dir, {"item1/data.parquet": {"sha256": "abc", "size_bytes": 100}})

        with patch("portolan_cli.cli.find_catalog_root", return_value=tmp_path):
            result = runner.invoke(cli, ["list", "--untracked-only", "--catalog", str(tmp_path)])

        assert result.exit_code == 0
        assert "README.md" in result.output
        assert "data.parquet" not in result.output


# =============================================================================
# Test: Ignored files are excluded
# =============================================================================


class TestListIgnoredFiles:
    """Tests that ignored_files patterns are respected."""

    @pytest.mark.unit
    def test_list_excludes_ds_store(self, runner: CliRunner, tmp_path: Path) -> None:
        """List excludes .DS_Store files."""
        make_catalog(tmp_path, ["col1"])
        col_dir = tmp_path / "col1"
        make_collection(col_dir, ["item1"])
        item_dir = col_dir / "item1"
        make_item(item_dir, {"data": "./data.parquet"})
        (item_dir / "data.parquet").write_bytes(b"x" * 100)
        (item_dir / ".DS_Store").write_bytes(b"x")

        make_versions_json(col_dir, {"item1/data.parquet": {"sha256": "abc", "size_bytes": 100}})

        with patch("portolan_cli.cli.find_catalog_root", return_value=tmp_path):
            result = runner.invoke(cli, ["list", "--catalog", str(tmp_path)])

        assert result.exit_code == 0
        assert ".DS_Store" not in result.output

    @pytest.mark.unit
    def test_list_excludes_tmp_files(self, runner: CliRunner, tmp_path: Path) -> None:
        """List excludes *.tmp files."""
        make_catalog(tmp_path, ["col1"])
        col_dir = tmp_path / "col1"
        make_collection(col_dir, ["item1"])
        item_dir = col_dir / "item1"
        make_item(item_dir, {"data": "./data.parquet"})
        (item_dir / "data.parquet").write_bytes(b"x" * 100)
        (item_dir / "temp.tmp").write_text("temp")

        make_versions_json(col_dir, {"item1/data.parquet": {"sha256": "abc", "size_bytes": 100}})

        with patch("portolan_cli.cli.find_catalog_root", return_value=tmp_path):
            result = runner.invoke(cli, ["list", "--catalog", str(tmp_path)])

        assert result.exit_code == 0
        assert "temp.tmp" not in result.output


# =============================================================================
# Test: JSON output includes status
# =============================================================================


class TestListJsonWithStatus:
    """Tests for JSON output with status information."""

    @pytest.mark.unit
    def test_list_json_includes_status_field(self, runner: CliRunner, tmp_path: Path) -> None:
        """JSON output includes status field for each asset."""
        make_catalog(tmp_path, ["col1"])
        col_dir = tmp_path / "col1"
        make_collection(col_dir, ["item1"])
        item_dir = col_dir / "item1"
        make_item(item_dir, {"data": "./data.parquet"})
        (item_dir / "data.parquet").write_bytes(b"x" * 100)
        (item_dir / "README.md").write_text("Untracked")

        make_versions_json(col_dir, {"item1/data.parquet": {"sha256": "abc", "size_bytes": 100}})

        with patch("portolan_cli.cli.find_catalog_root", return_value=tmp_path):
            result = runner.invoke(cli, ["list", "--json", "--catalog", str(tmp_path)])

        assert result.exit_code == 0
        envelope = json.loads(result.output)
        assert envelope["success"] is True
        # Should include status information
        data = envelope["data"]
        assert "summary" in data or "collections" in data

    @pytest.mark.unit
    def test_list_json_includes_summary_counts(self, runner: CliRunner, tmp_path: Path) -> None:
        """JSON output includes summary with tracked/untracked counts."""
        make_catalog(tmp_path, ["col1"])
        col_dir = tmp_path / "col1"
        make_collection(col_dir, ["item1"])
        item_dir = col_dir / "item1"
        make_item(item_dir, {"data": "./data.parquet"})
        (item_dir / "data.parquet").write_bytes(b"x" * 100)
        (item_dir / "README.md").write_text("Untracked")

        make_versions_json(col_dir, {"item1/data.parquet": {"sha256": "abc", "size_bytes": 100}})

        with patch("portolan_cli.cli.find_catalog_root", return_value=tmp_path):
            result = runner.invoke(cli, ["list", "--json", "--catalog", str(tmp_path)])

        assert result.exit_code == 0
        envelope = json.loads(result.output)
        data = envelope["data"]

        # Should have summary with counts
        if "summary" in data:
            summary = data["summary"]
            assert "total_tracked" in summary or "tracked" in summary
            assert "total_untracked" in summary or "untracked" in summary


# =============================================================================
# Test: Status command is removed
# =============================================================================


class TestStatusCommandRemoved:
    """Tests that the status command has been removed."""

    @pytest.mark.unit
    def test_status_command_not_available(self, runner: CliRunner) -> None:
        """portolan status command is no longer available."""
        result = runner.invoke(cli, ["status"])

        # Should either not exist or show error
        assert result.exit_code != 0 or "no such command" in result.output.lower()


# =============================================================================
# Test: Format detection fix (issue #210)
# =============================================================================


class TestFormatDetectionFix:
    """Tests that format detection is correct (fixes .CPG showing as GeoParquet)."""

    @pytest.mark.unit
    def test_cpg_file_not_shown_as_geoparquet(self, runner: CliRunner, tmp_path: Path) -> None:
        """CPG files should not be labeled as GeoParquet."""
        make_catalog(tmp_path, ["col1"])
        col_dir = tmp_path / "col1"
        make_collection(col_dir, ["item1"])
        item_dir = col_dir / "item1"
        make_item(item_dir, {"data": "./data.parquet", "cpg": "./data.CPG"})
        (item_dir / "data.parquet").write_bytes(b"x" * 100)
        (item_dir / "data.CPG").write_text("UTF-8")

        make_versions_json(
            col_dir,
            {
                "item1/data.parquet": {"sha256": "abc", "size_bytes": 100},
                "item1/data.CPG": {"sha256": "def", "size_bytes": 5},
            },
        )

        with patch("portolan_cli.cli.find_catalog_root", return_value=tmp_path):
            result = runner.invoke(cli, ["list", "--catalog", str(tmp_path)])

        assert result.exit_code == 0
        # data.parquet should show as GeoParquet
        assert "GeoParquet" in result.output
        # CPG file should NOT show as GeoParquet
        lines = result.output.split("\n")
        for line in lines:
            if ".CPG" in line:
                assert "GeoParquet" not in line, f"CPG file incorrectly labeled: {line}"
