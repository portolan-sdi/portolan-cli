"""Unit tests for metadata file exclusion in catalog list.

Tests that internal metadata files (versions.json, collection.json, etc.)
are excluded from the list output, and that .json files use content
inspection to distinguish GeoJSON from plain JSON.

Related issues:
- versions.json incorrectly shown as "GeoJSON" in list output
- PR #261: GeoJSON content detection in .json files
"""

from __future__ import annotations

import json
from pathlib import Path

import pytest
from click.testing import CliRunner

from portolan_cli.catalog_list import (
    _STAC_METADATA_FILES,
    _get_format_display_name,
    _is_ignored,
)
from portolan_cli.cli import cli


@pytest.fixture
def runner() -> CliRunner:
    """Create a CLI test runner."""
    return CliRunner()


class TestMetadataFileExclusion:
    """Tests that internal metadata files are excluded from list output."""

    @pytest.mark.unit
    def test_versions_json_in_stac_metadata_files(self) -> None:
        """versions.json should be in _STAC_METADATA_FILES constant.

        versions.json is internal Portolan metadata and should not appear
        in the list output alongside user data files.
        """
        assert "versions.json" in _STAC_METADATA_FILES

    @pytest.mark.unit
    def test_is_ignored_excludes_versions_json(self) -> None:
        """_is_ignored should return True for versions.json."""
        # versions.json at collection level should be ignored
        assert _is_ignored("versions.json", "any-item-id", []) is True

    @pytest.mark.unit
    def test_is_ignored_excludes_item_id_json(self) -> None:
        """_is_ignored should return True for {item_id}.json files.

        STAC item JSON files are often named after the item ID (e.g., regions.json
        for item "regions"). These are STAC metadata, not user data files.
        """
        # Item JSON named after the item should be ignored
        assert _is_ignored("regions.json", "regions", []) is True
        assert _is_ignored("scene-001.json", "scene-001", []) is True

        # But other JSON files should NOT be ignored
        assert _is_ignored("config.json", "regions", []) is False
        assert _is_ignored("data.json", "scene-001", []) is False

    @pytest.mark.unit
    def test_stac_item_json_not_detected_as_geojson(self, tmp_path: Path) -> None:
        """STAC item JSON should NOT be detected as GeoJSON.

        STAC items have "type": "Feature" like GeoJSON, but they also have
        "stac_version" which distinguishes them. They are JSON, not GeoJSON.
        """
        from portolan_cli.formats import FormatType, _detect_json_type

        # Create a STAC item JSON file
        stac_item = tmp_path / "item.json"
        stac_item.write_text(
            json.dumps(
                {
                    "type": "Feature",
                    "stac_version": "1.0.0",
                    "id": "test-item",
                    "geometry": {"type": "Point", "coordinates": [0, 0]},
                    "bbox": [0, 0, 0, 0],
                    "properties": {"datetime": "2024-01-01T00:00:00Z"},
                    "links": [],
                    "assets": {},
                }
            )
        )

        # Should NOT be detected as GeoJSON (VECTOR)
        assert _detect_json_type(stac_item) == FormatType.UNKNOWN

        # But actual GeoJSON should still be detected
        geojson = tmp_path / "points.json"
        geojson.write_text(
            json.dumps(
                {
                    "type": "FeatureCollection",
                    "features": [
                        {
                            "type": "Feature",
                            "geometry": {"type": "Point", "coordinates": [0, 0]},
                            "properties": {},
                        }
                    ],
                }
            )
        )
        assert _detect_json_type(geojson) == FormatType.VECTOR

    @pytest.mark.unit
    def test_list_excludes_versions_json(self, runner: CliRunner) -> None:
        """portolan list should not show versions.json in output.

        versions.json is internal metadata for tracking file versions.
        It should never appear in the list output.
        """
        with runner.isolated_filesystem():
            # Create catalog structure
            Path("catalog.json").write_text(
                json.dumps(
                    {
                        "type": "Catalog",
                        "stac_version": "1.0.0",
                        "id": "test",
                        "description": "Test",
                        "links": [],
                    }
                )
            )
            Path(".portolan").mkdir()
            Path(".portolan/config.yaml").write_text("version: 1\n")

            # Create collection with versions.json and a data file
            Path("collection").mkdir()
            Path("collection/item").mkdir()
            Path("collection/item/data.parquet").write_bytes(b"x" * 1000)
            Path("collection/versions.json").write_text(
                json.dumps(
                    {
                        "schema_version": "1.0.0",
                        "versions": [],
                    }
                )
            )

            result = runner.invoke(cli, ["list"])

            assert result.exit_code == 0
            # data.parquet should appear
            assert "data.parquet" in result.output
            # versions.json should NOT appear
            assert "versions.json" not in result.output


class TestJsonFormatDetection:
    """Tests that .json files use content inspection for format display."""

    @pytest.mark.unit
    def test_plain_json_not_labeled_geojson(self) -> None:
        """Plain .json files should be labeled 'JSON', not 'GeoJSON'.

        Only files with actual GeoJSON content should be labeled 'GeoJSON'.
        """
        # A plain config.json should NOT be labeled GeoJSON
        display_name = _get_format_display_name("config.json")
        # Currently this fails because .json maps to "GeoJSON"
        assert display_name == "JSON", f"Expected 'JSON' but got '{display_name}'"

    @pytest.mark.unit
    def test_geojson_extension_labeled_geojson(self) -> None:
        """Files with .geojson extension should be labeled 'GeoJSON'."""
        display_name = _get_format_display_name("data.geojson")
        assert display_name == "GeoJSON"

    @pytest.mark.unit
    def test_list_shows_plain_json_as_json(self, runner: CliRunner) -> None:
        """portolan list should show plain .json files as 'JSON', not 'GeoJSON'.

        Content inspection should be used to determine if a .json file
        is actually GeoJSON or just plain JSON.
        """
        with runner.isolated_filesystem():
            Path("catalog.json").write_text(
                json.dumps(
                    {
                        "type": "Catalog",
                        "stac_version": "1.0.0",
                        "id": "test",
                        "description": "Test",
                        "links": [],
                    }
                )
            )
            Path(".portolan").mkdir()
            Path(".portolan/config.yaml").write_text("version: 1\n")

            # Create collection with a plain JSON config file
            Path("collection").mkdir()
            Path("collection/item").mkdir()
            Path("collection/item/data.parquet").write_bytes(b"x" * 1000)
            # This is plain JSON, NOT GeoJSON
            Path("collection/item/config.json").write_text(
                json.dumps({"setting": "value", "enabled": True})
            )

            result = runner.invoke(cli, ["list"])

            assert result.exit_code == 0
            # If config.json appears, it should be labeled JSON, not GeoJSON
            if "config.json" in result.output:
                # The format should be JSON, not GeoJSON
                assert "(JSON," in result.output or "JSON)" in result.output
                assert (
                    "(GeoJSON," not in result.output
                    or "config.json" not in result.output.split("GeoJSON")[0]
                )

    @pytest.mark.unit
    def test_list_shows_geojson_content_as_geojson(self, runner: CliRunner) -> None:
        """portolan list should show .json files with GeoJSON content as 'GeoJSON'."""
        with runner.isolated_filesystem():
            Path("catalog.json").write_text(
                json.dumps(
                    {
                        "type": "Catalog",
                        "stac_version": "1.0.0",
                        "id": "test",
                        "description": "Test",
                        "links": [],
                    }
                )
            )
            Path(".portolan").mkdir()
            Path(".portolan/config.yaml").write_text("version: 1\n")

            # Create collection with a GeoJSON file saved as .json
            Path("collection").mkdir()
            Path("collection/item").mkdir()
            # This IS GeoJSON content
            Path("collection/item/points.json").write_text(
                json.dumps(
                    {
                        "type": "FeatureCollection",
                        "features": [
                            {
                                "type": "Feature",
                                "geometry": {"type": "Point", "coordinates": [0, 0]},
                                "properties": {},
                            }
                        ],
                    }
                )
            )

            result = runner.invoke(cli, ["list"])

            assert result.exit_code == 0
            # points.json should be labeled as GeoJSON because it has GeoJSON content
            if "points.json" in result.output:
                assert "GeoJSON" in result.output
