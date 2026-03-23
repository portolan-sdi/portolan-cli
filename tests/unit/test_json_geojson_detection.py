"""Tests for Issue #256: Detect if .json files are valid GeoJSON.

GeoJSON files are often saved with .json extension rather than .geojson.
Portolan should detect GeoJSON content by inspecting the file, not just
relying on extension.

See: https://github.com/portolan-sdi/portolan-cli/issues/256
"""

from __future__ import annotations

from pathlib import Path

import pytest


@pytest.mark.unit
class TestJsonGeoJsonDetectionInScan:
    """Tests for .json file detection in scan_directory."""

    def test_json_with_geojson_content_is_detected_as_ready(self, fixtures_dir: Path) -> None:
        """A .json file containing valid GeoJSON should be in ready list.

        This is the core fix for Issue #256: rec_centers.json contains
        a valid FeatureCollection but has .json extension instead of .geojson.
        """
        from portolan_cli.scan import scan_directory

        scan_path = fixtures_dir / "scan" / "json_geojson"
        result = scan_directory(scan_path)

        # rec_centers.json should be detected as a ready file
        ready_names = [f.path.name for f in result.ready]
        assert "rec_centers.json" in ready_names, (
            f"rec_centers.json should be detected as GeoJSON. "
            f"Ready files: {ready_names}, Skipped: {[s.path.name for s in result.skipped]}"
        )

    def test_json_with_geojson_content_has_vector_format_type(self, fixtures_dir: Path) -> None:
        """A .json file with GeoJSON content should have VECTOR format type."""
        from portolan_cli.scan import FormatType, scan_directory

        scan_path = fixtures_dir / "scan" / "json_geojson"
        result = scan_directory(scan_path)

        # Find rec_centers.json in ready list
        rec_centers = next((f for f in result.ready if f.path.name == "rec_centers.json"), None)
        assert rec_centers is not None, "rec_centers.json not found in ready list"
        assert rec_centers.format_type == FormatType.VECTOR

    def test_json_with_geojson_content_has_json_extension(self, fixtures_dir: Path) -> None:
        """A .json file with GeoJSON content should keep .json extension."""
        from portolan_cli.scan import scan_directory

        scan_path = fixtures_dir / "scan" / "json_geojson"
        result = scan_directory(scan_path)

        rec_centers = next((f for f in result.ready if f.path.name == "rec_centers.json"), None)
        assert rec_centers is not None
        assert rec_centers.extension == ".json"

    def test_plain_json_is_skipped(self, fixtures_dir: Path) -> None:
        """A .json file without GeoJSON content should be in skipped list.

        config.json is a plain settings file, not GeoJSON. It should not
        be added to the ready list.
        """
        from portolan_cli.scan import scan_directory

        scan_path = fixtures_dir / "scan" / "json_geojson"
        result = scan_directory(scan_path)

        # config.json should be skipped (not geospatial)
        ready_names = [f.path.name for f in result.ready]
        skipped_names = [s.path.name for s in result.skipped]

        assert "config.json" not in ready_names, (
            f"config.json should NOT be in ready list (it's not GeoJSON). Ready: {ready_names}"
        )
        assert "config.json" in skipped_names, (
            f"config.json should be in skipped list. Skipped: {skipped_names}"
        )


@pytest.mark.unit
class TestJsonGeoJsonDetectionEdgeCases:
    """Edge case tests for .json GeoJSON detection."""

    def test_json_with_feature_type_is_detected(self, tmp_path: Path) -> None:
        """A .json file with Feature type (not FeatureCollection) is detected."""
        from portolan_cli.scan import scan_directory

        # Create a single Feature (not FeatureCollection)
        json_file = tmp_path / "single_feature.json"
        json_file.write_text(
            '{"type": "Feature", "geometry": {"type": "Point", "coordinates": [0, 0]}, "properties": {}}'
        )

        result = scan_directory(tmp_path)
        ready_names = [f.path.name for f in result.ready]
        assert "single_feature.json" in ready_names

    def test_json_with_geometry_type_is_detected(self, tmp_path: Path) -> None:
        """A .json file with bare geometry (Point, Polygon, etc.) is detected."""
        from portolan_cli.scan import scan_directory

        # Create a bare geometry (no Feature wrapper)
        json_file = tmp_path / "bare_point.json"
        json_file.write_text('{"type": "Point", "coordinates": [0, 0]}')

        result = scan_directory(tmp_path)
        ready_names = [f.path.name for f in result.ready]
        assert "bare_point.json" in ready_names

    def test_json_with_multipolygon_is_detected(self, tmp_path: Path) -> None:
        """A .json file with MultiPolygon geometry is detected."""
        from portolan_cli.scan import scan_directory

        json_file = tmp_path / "multipolygon.json"
        json_file.write_text(
            '{"type": "MultiPolygon", "coordinates": [[[[0,0],[1,0],[1,1],[0,1],[0,0]]]]}'
        )

        result = scan_directory(tmp_path)
        ready_names = [f.path.name for f in result.ready]
        assert "multipolygon.json" in ready_names

    def test_json_array_is_skipped(self, tmp_path: Path) -> None:
        """A .json file containing an array (not object) is skipped."""
        from portolan_cli.scan import scan_directory

        json_file = tmp_path / "array.json"
        json_file.write_text("[1, 2, 3, 4]")

        result = scan_directory(tmp_path)
        skipped_names = [s.path.name for s in result.skipped]
        assert "array.json" in skipped_names

    def test_json_with_type_key_but_not_geojson_is_skipped(self, tmp_path: Path) -> None:
        """A .json with 'type' key but non-GeoJSON value is skipped."""
        from portolan_cli.scan import scan_directory

        json_file = tmp_path / "not_geo.json"
        json_file.write_text('{"type": "something_else", "data": [1, 2, 3]}')

        result = scan_directory(tmp_path)
        skipped_names = [s.path.name for s in result.skipped]
        assert "not_geo.json" in skipped_names

    def test_large_json_geojson_is_detected(self, tmp_path: Path) -> None:
        """A large .json file with GeoJSON content early is detected.

        Detection reads only first 8KB, so GeoJSON type token must appear early.
        """
        from portolan_cli.scan import scan_directory

        # Create a file with FeatureCollection type early, then padding
        json_file = tmp_path / "large.json"
        content = '{"type": "FeatureCollection", "features": ['
        content += (
            '{"type": "Feature", "geometry": {"type": "Point", "coordinates": [0, 0]}, "properties": {"padding": "'
            + "x" * 10000
            + '"}}]}'
        )
        json_file.write_text(content)

        result = scan_directory(tmp_path)
        ready_names = [f.path.name for f in result.ready]
        assert "large.json" in ready_names


@pytest.mark.unit
class TestJsonGeoJsonMixedDirectory:
    """Tests for directories with mixed .json and .geojson files."""

    def test_mixed_json_and_geojson_both_detected(self, tmp_path: Path) -> None:
        """Both .json and .geojson files with GeoJSON content are detected."""
        from portolan_cli.scan import scan_directory

        # Create .geojson file
        geojson_file = tmp_path / "standard.geojson"
        geojson_file.write_text('{"type": "FeatureCollection", "features": []}')

        # Create .json file with GeoJSON content
        json_file = tmp_path / "alternate.json"
        json_file.write_text('{"type": "FeatureCollection", "features": []}')

        result = scan_directory(tmp_path)
        ready_names = [f.path.name for f in result.ready]

        assert "standard.geojson" in ready_names
        assert "alternate.json" in ready_names
        assert len(result.ready) == 2

    def test_json_geojson_and_plain_json_mixed(self, tmp_path: Path) -> None:
        """Directory with GeoJSON .json and plain .json handles both correctly."""
        from portolan_cli.scan import scan_directory

        # GeoJSON in .json
        geo_json = tmp_path / "places.json"
        geo_json.write_text('{"type": "FeatureCollection", "features": []}')

        # Plain JSON
        plain_json = tmp_path / "settings.json"
        plain_json.write_text('{"key": "value"}')

        result = scan_directory(tmp_path)
        ready_names = [f.path.name for f in result.ready]
        skipped_names = [s.path.name for s in result.skipped]

        assert "places.json" in ready_names
        assert "settings.json" in skipped_names


@pytest.mark.unit
class TestJsonGeoJsonRobustness:
    """Robustness tests for .json GeoJSON detection edge cases.

    These tests cover error handling paths that could cause crashes or
    incorrect behavior in production.
    """

    def test_binary_file_with_json_extension_is_skipped(self, tmp_path: Path) -> None:
        """A binary file with .json extension should be skipped, not crash.

        This tests the UnicodeDecodeError handling path. Binary files
        cannot be decoded as UTF-8 and should be gracefully skipped.
        """
        from portolan_cli.scan import scan_directory

        # Create a binary file with .json extension
        binary_json = tmp_path / "binary.json"
        # Write actual binary data that will fail UTF-8 decoding
        binary_json.write_bytes(b"\x89PNG\r\n\x1a\n\x00\x00\x00\rIHDR\x00\x00")

        result = scan_directory(tmp_path)

        # Should be skipped, not raise an exception
        ready_names = [f.path.name for f in result.ready]
        skipped_names = [s.path.name for s in result.skipped]

        assert "binary.json" not in ready_names
        assert "binary.json" in skipped_names

    def test_geojson_token_after_8kb_is_skipped(self, tmp_path: Path) -> None:
        """A .json with GeoJSON type token AFTER 8KB should be skipped.

        The detection reads only the first 8KB for performance.
        If the type token appears later, the file is not detected as GeoJSON.
        This is a known limitation documented in the code.
        """
        from portolan_cli.scan import scan_directory

        # Create a file with GeoJSON type token appearing after 8KB
        json_file = tmp_path / "late_token.json"
        # 9000 bytes of padding before the GeoJSON structure
        padding = "x" * 9000
        content = f'{{"data": "{padding}", "type": "FeatureCollection", "features": []}}'
        json_file.write_text(content)

        result = scan_directory(tmp_path)
        skipped_names = [s.path.name for s in result.skipped]

        # Should be skipped because token is beyond 8KB detection window
        assert "late_token.json" in skipped_names

    def test_empty_json_file_is_flagged_as_issue(self, tmp_path: Path) -> None:
        """An empty .json file should be flagged as a zero-byte issue."""
        from portolan_cli.scan import IssueType, scan_directory

        # Create an empty file
        empty_json = tmp_path / "empty.json"
        empty_json.write_text("")

        result = scan_directory(tmp_path)

        # Empty files are caught by zero-byte check and added to issues
        issue_files = [i.path.name for i in result.issues]
        assert "empty.json" in issue_files

        # Verify it's a zero-byte issue
        empty_issues = [i for i in result.issues if i.path.name == "empty.json"]
        assert len(empty_issues) == 1
        assert empty_issues[0].issue_type == IssueType.ZERO_BYTE_FILE

    def test_stac_catalog_json_is_skipped_as_metadata(self, tmp_path: Path) -> None:
        """STAC catalog.json should be skipped as metadata, not inspected for GeoJSON.

        This verifies that STAC filenames are handled before GeoJSON content inspection.
        """
        from portolan_cli.scan import scan_directory
        from portolan_cli.scan_classify import FileCategory

        # Create a STAC catalog file
        catalog_json = tmp_path / "catalog.json"
        catalog_json.write_text('{"type": "Catalog", "id": "test"}')

        result = scan_directory(tmp_path)
        skipped = [s for s in result.skipped if s.path.name == "catalog.json"]

        assert len(skipped) == 1
        assert skipped[0].category == FileCategory.STAC_METADATA

    def test_stac_collection_json_is_skipped_as_metadata(self, tmp_path: Path) -> None:
        """STAC collection.json should be skipped as metadata."""
        from portolan_cli.scan import scan_directory
        from portolan_cli.scan_classify import FileCategory

        # Create a STAC collection file
        collection_json = tmp_path / "collection.json"
        collection_json.write_text('{"type": "Collection", "id": "test"}')

        result = scan_directory(tmp_path)
        skipped = [s for s in result.skipped if s.path.name == "collection.json"]

        assert len(skipped) == 1
        assert skipped[0].category == FileCategory.STAC_METADATA

    def test_json_with_null_bytes_is_skipped(self, tmp_path: Path) -> None:
        """A .json file with embedded null bytes should be skipped."""
        from portolan_cli.scan import scan_directory

        # Create a file with null bytes (corrupted JSON)
        corrupt_json = tmp_path / "corrupt.json"
        corrupt_json.write_bytes(b'{"type": "\x00FeatureCollection"}')

        result = scan_directory(tmp_path)
        skipped_names = [s.path.name for s in result.skipped]

        # Should be skipped (null bytes don't match GeoJSON tokens)
        assert "corrupt.json" in skipped_names

    def test_json_with_bom_is_detected(self, tmp_path: Path) -> None:
        """A GeoJSON .json file with UTF-8 BOM should still be detected.

        Some editors add a BOM (Byte Order Mark) to UTF-8 files.
        The detection should still work.
        """
        from portolan_cli.scan import scan_directory

        # Create a GeoJSON file with UTF-8 BOM
        bom_json = tmp_path / "with_bom.json"
        # UTF-8 BOM followed by valid GeoJSON
        content = '\ufeff{"type": "FeatureCollection", "features": []}'
        bom_json.write_text(content, encoding="utf-8")

        result = scan_directory(tmp_path)
        ready_names = [f.path.name for f in result.ready]

        # BOM should not interfere with detection
        assert "with_bom.json" in ready_names
