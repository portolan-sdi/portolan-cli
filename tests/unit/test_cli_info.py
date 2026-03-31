"""Tests for the CLI info command.

TDD-first tests for the top-level `portolan info` command.
Tests file-level, collection-level, and catalog-level info display.
"""

from __future__ import annotations

import json
from pathlib import Path
from typing import TYPE_CHECKING

import pytest
from click.testing import CliRunner

if TYPE_CHECKING:
    pass


# =============================================================================
# Fixtures
# =============================================================================


@pytest.fixture
def cli_runner() -> CliRunner:
    """Create a Click CLI test runner."""
    return CliRunner()


@pytest.fixture
def catalog_with_tracked_file(tmp_path: Path, valid_points_parquet: Path) -> Path:
    """Create a catalog with a tracked GeoParquet file."""
    import shutil

    # Create catalog structure
    catalog_root = tmp_path / "catalog"
    catalog_root.mkdir()

    # Create .portolan directory (for MANAGED state per ADR-0027)
    portolan_dir = catalog_root / ".portolan"
    portolan_dir.mkdir()
    (portolan_dir / "config.yaml").write_text("# Portolan config\n")
    (portolan_dir / "state.json").write_text("{}\n")

    # Create catalog.json
    catalog_json = {
        "type": "Catalog",
        "stac_version": "1.0.0",
        "id": "test-catalog",
        "description": "Test catalog for info command",
        "links": [
            {"rel": "root", "href": "./catalog.json"},
            {"rel": "child", "href": "./demographics/collection.json"},
        ],
    }
    (catalog_root / "catalog.json").write_text(json.dumps(catalog_json, indent=2))

    # Create collection directory
    collection_dir = catalog_root / "demographics"
    collection_dir.mkdir()

    # Create collection.json
    collection_json = {
        "type": "Collection",
        "stac_version": "1.0.0",
        "id": "demographics",
        "description": "Demographics data collection",
        "title": "Demographics",
        "license": "CC-BY-4.0",
        "extent": {
            "spatial": {"bbox": [[-122.5, 37.7, -122.3, 37.9]]},
            "temporal": {"interval": [["2024-01-01T00:00:00Z", None]]},
        },
        "links": [
            {"rel": "root", "href": "../catalog.json"},
            {"rel": "self", "href": "./collection.json"},
            {"rel": "item", "href": "./census/census.json"},
        ],
    }
    (collection_dir / "collection.json").write_text(json.dumps(collection_json, indent=2))

    # Create item directory
    item_dir = collection_dir / "census"
    item_dir.mkdir()

    # Copy test parquet file
    dest_parquet = item_dir / "census.parquet"
    shutil.copy2(valid_points_parquet, dest_parquet)

    # Create item.json
    item_json = {
        "type": "Feature",
        "stac_version": "1.0.0",
        "id": "census",
        "geometry": {"type": "Point", "coordinates": [-122.4, 37.8]},
        "bbox": [-122.5, 37.7, -122.3, 37.9],
        "properties": {"datetime": "2024-01-01T00:00:00Z", "title": "Census Data"},
        "assets": {
            "data": {
                "href": "./census.parquet",
                "type": "application/x-parquet",
                "roles": ["data"],
            }
        },
        "links": [],
    }
    (item_dir / "census.json").write_text(json.dumps(item_json, indent=2))

    # Create versions.json
    versions_json = {
        "spec_version": "1.0.0",
        "current_version": "1.2.0",
        "versions": [
            {
                "version": "1.2.0",
                "created": "2024-01-15T00:00:00Z",
                "breaking": False,
                "assets": {
                    "census.parquet": {
                        "sha256": "abc123",
                        "size_bytes": 1500,
                        "href": "demographics/census/census.parquet",
                    }
                },
                "changes": ["census.parquet"],
            }
        ],
    }
    (collection_dir / "versions.json").write_text(json.dumps(versions_json, indent=2))

    return catalog_root


@pytest.fixture
def catalog_with_subcatalog(tmp_path: Path, valid_points_parquet: Path) -> Path:
    """Create a catalog with a subcatalog (nested catalog structure per ADR-0032)."""

    # Create root catalog structure
    catalog_root = tmp_path / "catalog"
    catalog_root.mkdir()

    # Create .portolan directory (for MANAGED state per ADR-0027)
    portolan_dir = catalog_root / ".portolan"
    portolan_dir.mkdir()
    (portolan_dir / "config.yaml").write_text("# Portolan config\n")
    (portolan_dir / "state.json").write_text("{}\n")

    # Create root catalog.json
    root_catalog_json = {
        "type": "Catalog",
        "stac_version": "1.0.0",
        "id": "root-catalog",
        "description": "Root catalog with subcatalogs",
        "links": [
            {"rel": "root", "href": "./catalog.json"},
            {"rel": "child", "href": "./climate/catalog.json"},
        ],
    }
    (catalog_root / "catalog.json").write_text(json.dumps(root_catalog_json, indent=2))

    # Create subcatalog directory
    subcatalog_dir = catalog_root / "climate"
    subcatalog_dir.mkdir()

    # Create subcatalog's .portolan directory (per ADR-0039)
    subcatalog_portolan = subcatalog_dir / ".portolan"
    subcatalog_portolan.mkdir()
    (subcatalog_portolan / "config.yaml").write_text("# Subcatalog config\n")

    # Create subcatalog's catalog.json
    subcatalog_json = {
        "type": "Catalog",
        "stac_version": "1.0.0",
        "id": "climate-catalog",
        "description": "Climate data subcatalog",
        "links": [
            {"rel": "root", "href": "../catalog.json"},
            {"rel": "parent", "href": "../catalog.json"},
            {"rel": "self", "href": "./catalog.json"},
            {"rel": "child", "href": "./temperature/collection.json"},
        ],
    }
    (subcatalog_dir / "catalog.json").write_text(json.dumps(subcatalog_json, indent=2))

    # Create collection inside subcatalog
    collection_dir = subcatalog_dir / "temperature"
    collection_dir.mkdir()

    collection_json = {
        "type": "Collection",
        "stac_version": "1.0.0",
        "id": "temperature",
        "description": "Temperature measurements",
        "title": "Temperature",
        "license": "CC-BY-4.0",
        "extent": {
            "spatial": {"bbox": [[4.0, 52.0, 5.0, 53.0]]},
            "temporal": {"interval": [["2024-01-01T00:00:00Z", None]]},
        },
        "links": [
            {"rel": "root", "href": "../../catalog.json"},
            {"rel": "parent", "href": "../catalog.json"},
            {"rel": "self", "href": "./collection.json"},
        ],
    }
    (collection_dir / "collection.json").write_text(json.dumps(collection_json, indent=2))

    # Create an empty directory (neither catalog nor collection)
    empty_dir = catalog_root / "empty_dir"
    empty_dir.mkdir()

    return catalog_root


# =============================================================================
# Test: File Info Command
# =============================================================================


class TestInfoCommandFile:
    """Tests for `portolan info <file>` command."""

    @pytest.mark.unit
    def test_info_file_shows_metadata(
        self, cli_runner: CliRunner, catalog_with_tracked_file: Path
    ) -> None:
        """Test that info command displays file metadata."""
        from portolan_cli.cli import cli

        file_path = catalog_with_tracked_file / "demographics" / "census" / "census.parquet"

        result = cli_runner.invoke(
            cli,
            ["info", str(file_path), "--catalog", str(catalog_with_tracked_file)],
        )

        assert result.exit_code == 0
        assert "Format:" in result.output or "GeoParquet" in result.output
        assert "CRS:" in result.output
        assert "Bbox:" in result.output

    @pytest.mark.unit
    def test_info_file_shows_version_when_tracked(
        self, cli_runner: CliRunner, catalog_with_tracked_file: Path
    ) -> None:
        """Test that info command shows version for tracked files."""
        from portolan_cli.cli import cli

        file_path = catalog_with_tracked_file / "demographics" / "census" / "census.parquet"

        result = cli_runner.invoke(
            cli,
            ["info", str(file_path), "--catalog", str(catalog_with_tracked_file)],
        )

        assert result.exit_code == 0
        assert "Version:" in result.output
        assert "v1.2.0" in result.output

    @pytest.mark.unit
    def test_info_file_json_output(
        self, cli_runner: CliRunner, catalog_with_tracked_file: Path
    ) -> None:
        """Test that info command produces valid JSON with --json flag."""
        from portolan_cli.cli import cli

        file_path = catalog_with_tracked_file / "demographics" / "census" / "census.parquet"

        result = cli_runner.invoke(
            cli,
            ["info", str(file_path), "--catalog", str(catalog_with_tracked_file), "--json"],
        )

        assert result.exit_code == 0
        data = json.loads(result.output)
        assert data["success"] is True
        assert data["command"] == "info"
        assert "format" in data["data"]

    @pytest.mark.unit
    def test_info_nonexistent_file_fails(
        self, cli_runner: CliRunner, catalog_with_tracked_file: Path
    ) -> None:
        """Test that info command fails for non-existent file."""
        from portolan_cli.cli import cli

        result = cli_runner.invoke(
            cli,
            [
                "info",
                str(catalog_with_tracked_file / "nonexistent.parquet"),
                "--catalog",
                str(catalog_with_tracked_file),
            ],
        )

        assert result.exit_code != 0


# =============================================================================
# Test: Collection Info Command
# =============================================================================


class TestInfoCommandCollection:
    """Tests for `portolan info <collection/>` command."""

    @pytest.mark.unit
    def test_info_collection_shows_metadata(
        self, cli_runner: CliRunner, catalog_with_tracked_file: Path
    ) -> None:
        """Test that info command displays collection metadata."""
        from portolan_cli.cli import cli

        collection_path = catalog_with_tracked_file / "demographics"

        result = cli_runner.invoke(
            cli,
            ["info", str(collection_path), "--catalog", str(catalog_with_tracked_file)],
        )

        assert result.exit_code == 0
        assert "Collection:" in result.output or "demographics" in result.output
        assert "Description:" in result.output or "Items:" in result.output

    @pytest.mark.unit
    def test_info_collection_json_output(
        self, cli_runner: CliRunner, catalog_with_tracked_file: Path
    ) -> None:
        """Test that info command produces valid JSON for collections."""
        from portolan_cli.cli import cli

        collection_path = catalog_with_tracked_file / "demographics"

        result = cli_runner.invoke(
            cli,
            ["info", str(collection_path), "--catalog", str(catalog_with_tracked_file), "--json"],
        )

        assert result.exit_code == 0
        data = json.loads(result.output)
        assert data["success"] is True
        assert "collection_id" in data["data"]


# =============================================================================
# Test: Catalog Info Command (no argument)
# =============================================================================


class TestInfoCommandCatalog:
    """Tests for `portolan info` (catalog-level) command."""

    @pytest.mark.unit
    def test_info_catalog_shows_metadata(
        self, cli_runner: CliRunner, catalog_with_tracked_file: Path
    ) -> None:
        """Test that info command displays catalog metadata when no arg given."""
        from portolan_cli.cli import cli

        result = cli_runner.invoke(
            cli,
            ["info", "--catalog", str(catalog_with_tracked_file)],
        )

        assert result.exit_code == 0
        assert "Catalog:" in result.output or "test-catalog" in result.output

    @pytest.mark.unit
    def test_info_catalog_json_output(
        self, cli_runner: CliRunner, catalog_with_tracked_file: Path
    ) -> None:
        """Test that info command produces valid JSON for catalog."""
        from portolan_cli.cli import cli

        result = cli_runner.invoke(
            cli,
            ["info", "--catalog", str(catalog_with_tracked_file), "--json"],
        )

        assert result.exit_code == 0
        data = json.loads(result.output)
        assert data["success"] is True
        assert "catalog_id" in data["data"]


# =============================================================================
# Test: Info with "." argument (GitHub Issues #291, #292)
# =============================================================================


class TestInfoDotArgument:
    """Tests for `portolan info .` to show catalog info when at catalog root.

    Fixes GitHub issues #291 and #292 where `portolan info .` failed with
    "Collection not found" instead of showing catalog-level information.
    """

    @pytest.mark.unit
    def test_info_dot_at_catalog_root_shows_catalog_info(
        self, cli_runner: CliRunner, catalog_with_tracked_file: Path
    ) -> None:
        """Test that `portolan info .` at catalog root shows catalog info."""
        from portolan_cli.cli import cli

        # Use the catalog root path as the target (simulating `info .` from root)
        result = cli_runner.invoke(
            cli,
            ["info", str(catalog_with_tracked_file), "--catalog", str(catalog_with_tracked_file)],
        )

        assert result.exit_code == 0, f"Expected success, got: {result.output}"
        # Should show catalog info, not collection error
        assert "Collection not found" not in result.output
        assert "Catalog:" in result.output or "test-catalog" in result.output

    @pytest.mark.unit
    def test_info_dot_at_catalog_root_json_output(
        self, cli_runner: CliRunner, catalog_with_tracked_file: Path
    ) -> None:
        """Test that `portolan info . --json` produces catalog JSON."""
        from portolan_cli.cli import cli

        result = cli_runner.invoke(
            cli,
            [
                "info",
                str(catalog_with_tracked_file),
                "--catalog",
                str(catalog_with_tracked_file),
                "--json",
            ],
        )

        assert result.exit_code == 0, f"Expected success, got: {result.output}"
        data = json.loads(result.output)
        assert data["success"] is True
        assert "catalog_id" in data["data"]  # Catalog info, not collection

    @pytest.mark.unit
    def test_info_subcatalog_shows_subcatalog_info(
        self, cli_runner: CliRunner, catalog_with_subcatalog: Path
    ) -> None:
        """Test that `portolan info <subcatalog>` shows subcatalog info."""
        from portolan_cli.cli import cli

        subcatalog_path = catalog_with_subcatalog / "climate"

        result = cli_runner.invoke(
            cli,
            ["info", str(subcatalog_path), "--catalog", str(catalog_with_subcatalog)],
        )

        assert result.exit_code == 0, f"Expected success, got: {result.output}"
        # Should show climate subcatalog info
        assert "Collection not found" not in result.output
        assert "Catalog:" in result.output or "climate-catalog" in result.output

    @pytest.mark.unit
    def test_info_subcatalog_json_output(
        self, cli_runner: CliRunner, catalog_with_subcatalog: Path
    ) -> None:
        """Test that `portolan info <subcatalog> --json` produces catalog JSON."""
        from portolan_cli.cli import cli

        subcatalog_path = catalog_with_subcatalog / "climate"

        result = cli_runner.invoke(
            cli,
            [
                "info",
                str(subcatalog_path),
                "--catalog",
                str(catalog_with_subcatalog),
                "--json",
            ],
        )

        assert result.exit_code == 0, f"Expected success, got: {result.output}"
        data = json.loads(result.output)
        assert data["success"] is True
        assert "catalog_id" in data["data"]
        assert data["data"]["catalog_id"] == "climate-catalog"

    @pytest.mark.unit
    def test_info_empty_directory_fails_with_clear_error(
        self, cli_runner: CliRunner, catalog_with_subcatalog: Path
    ) -> None:
        """Test that info on a directory without catalog.json or collection.json fails."""
        from portolan_cli.cli import cli

        empty_dir = catalog_with_subcatalog / "empty_dir"

        result = cli_runner.invoke(
            cli,
            ["info", str(empty_dir), "--catalog", str(catalog_with_subcatalog)],
        )

        assert result.exit_code != 0
        # Should have a clear error message about not being a catalog or collection
        assert (
            "not a catalog or collection" in result.output.lower()
            or "collection not found" in result.output.lower()
            or "catalog not found" in result.output.lower()
        )

    @pytest.mark.unit
    def test_info_directory_with_both_catalog_and_collection_prefers_catalog(
        self, cli_runner: CliRunner, tmp_path: Path
    ) -> None:
        """Test that catalog.json takes precedence when both files exist.

        Per ADR-0032 Pattern 2, a directory CAN have both catalog.json and
        collection.json (e.g., a collection with sub-catalogs organizing items).
        When both exist, we prefer catalog.json since it represents the
        organizational structure.
        """
        from portolan_cli.cli import cli

        # Create a directory with BOTH catalog.json and collection.json
        dual_dir = tmp_path / "dual"
        dual_dir.mkdir()

        # Create .portolan for root detection
        portolan_dir = tmp_path / ".portolan"
        portolan_dir.mkdir()
        (portolan_dir / "config.yaml").write_text("# config\n")

        # Root catalog.json
        root_catalog = {
            "type": "Catalog",
            "stac_version": "1.0.0",
            "id": "root",
            "description": "Root",
            "links": [{"rel": "child", "href": "./dual/catalog.json"}],
        }
        (tmp_path / "catalog.json").write_text(json.dumps(root_catalog))

        # Dual directory has BOTH files
        dual_catalog = {
            "type": "Catalog",
            "stac_version": "1.0.0",
            "id": "dual-as-catalog",
            "description": "This directory has both catalog.json and collection.json",
            "links": [],
        }
        (dual_dir / "catalog.json").write_text(json.dumps(dual_catalog))

        dual_collection = {
            "type": "Collection",
            "stac_version": "1.0.0",
            "id": "dual-as-collection",
            "description": "This would be used if collection.json took precedence",
            "license": "CC-BY-4.0",
            "extent": {
                "spatial": {"bbox": [[0, 0, 1, 1]]},
                "temporal": {"interval": [[None, None]]},
            },
            "links": [],
        }
        (dual_dir / "collection.json").write_text(json.dumps(dual_collection))

        # Run info on the dual directory
        result = cli_runner.invoke(
            cli,
            ["info", str(dual_dir), "--catalog", str(tmp_path), "--json"],
        )

        assert result.exit_code == 0, f"Expected success, got: {result.output}"
        data = json.loads(result.output)
        assert data["success"] is True
        # Should return catalog info, NOT collection info
        assert "catalog_id" in data["data"], "Expected catalog info, got collection"
        assert data["data"]["catalog_id"] == "dual-as-catalog"
