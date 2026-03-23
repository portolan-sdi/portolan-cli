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
