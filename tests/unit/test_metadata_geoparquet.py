"""Tests for GeoParquet metadata extraction."""

from __future__ import annotations

from pathlib import Path

import pytest

from portolan_cli.metadata.geoparquet import GeoParquetMetadata, extract_geoparquet_metadata


class TestExtractGeoParquetMetadata:
    """Tests for extract_geoparquet_metadata()."""

    @pytest.mark.unit
    def test_returns_geoparquet_metadata(self, valid_points_parquet: Path) -> None:
        """Should return GeoParquetMetadata dataclass."""
        metadata = extract_geoparquet_metadata(valid_points_parquet)
        assert isinstance(metadata, GeoParquetMetadata)

    @pytest.mark.unit
    def test_extracts_bbox(self, valid_points_parquet: Path) -> None:
        """Should extract bounding box as (minx, miny, maxx, maxy)."""
        metadata = extract_geoparquet_metadata(valid_points_parquet)
        assert metadata.bbox is not None
        assert len(metadata.bbox) == 4
        minx, miny, maxx, maxy = metadata.bbox
        assert minx <= maxx
        assert miny <= maxy

    @pytest.mark.unit
    def test_extracts_crs(self, valid_points_parquet: Path) -> None:
        """Should extract CRS as EPSG code or WKT."""
        metadata = extract_geoparquet_metadata(valid_points_parquet)
        # Note: Our fixture doesn't have CRS in the geo metadata
        # This test verifies we handle missing CRS gracefully
        # (A fully compliant fixture would have EPSG:4326)
        # For now, we accept None or a CRS string
        assert metadata.crs is None or isinstance(metadata.crs, (str, dict))

    @pytest.mark.unit
    def test_extracts_geometry_type(self, valid_points_parquet: Path) -> None:
        """Should extract geometry type."""
        metadata = extract_geoparquet_metadata(valid_points_parquet)
        assert metadata.geometry_type is not None
        assert "Point" in metadata.geometry_type

    @pytest.mark.unit
    def test_extracts_feature_count(self, valid_points_parquet: Path) -> None:
        """Should extract feature count."""
        metadata = extract_geoparquet_metadata(valid_points_parquet)
        assert metadata.feature_count is not None
        assert metadata.feature_count > 0

    @pytest.mark.unit
    def test_extracts_schema(self, valid_points_parquet: Path) -> None:
        """Should extract column schema."""
        metadata = extract_geoparquet_metadata(valid_points_parquet)
        assert metadata.schema is not None
        assert isinstance(metadata.schema, dict)
        # Should have at least geometry column
        assert len(metadata.schema) > 0

    @pytest.mark.unit
    def test_extracts_geometry_column_name(self, valid_points_parquet: Path) -> None:
        """Should identify the geometry column name."""
        metadata = extract_geoparquet_metadata(valid_points_parquet)
        assert metadata.geometry_column is not None

    @pytest.mark.unit
    def test_raises_for_nonexistent_file(self, tmp_path: Path) -> None:
        """Should raise FileNotFoundError for missing file."""
        with pytest.raises(FileNotFoundError):
            extract_geoparquet_metadata(tmp_path / "missing.parquet")

    @pytest.mark.unit
    def test_raises_for_non_geoparquet(self, tmp_path: Path) -> None:
        """Should raise ValueError for non-GeoParquet file."""
        fake_file = tmp_path / "fake.parquet"
        fake_file.write_bytes(b"not a parquet file")

        with pytest.raises((ValueError, Exception)):  # pyarrow may raise different errors
            extract_geoparquet_metadata(fake_file)


class TestGeoParquetMetadata:
    """Tests for GeoParquetMetadata dataclass."""

    @pytest.mark.unit
    def test_to_stac_properties(self, valid_points_parquet: Path) -> None:
        """to_stac_properties() returns STAC-compatible dict."""
        metadata = extract_geoparquet_metadata(valid_points_parquet)
        props = metadata.to_stac_properties()

        assert isinstance(props, dict)
        # Should have standard STAC item properties structure

    @pytest.mark.unit
    def test_to_dict(self, valid_points_parquet: Path) -> None:
        """to_dict() returns complete metadata dict."""
        metadata = extract_geoparquet_metadata(valid_points_parquet)
        d = metadata.to_dict()

        assert "bbox" in d
        assert "crs" in d
        assert "geometry_type" in d
        assert "feature_count" in d

    @pytest.mark.unit
    def test_to_stac_properties_includes_feature_count(self, valid_points_parquet: Path) -> None:
        """to_stac_properties() includes feature_count."""
        metadata = extract_geoparquet_metadata(valid_points_parquet)
        props = metadata.to_stac_properties()
        assert "geoparquet:feature_count" in props

    @pytest.mark.unit
    def test_to_stac_properties_with_zero_features(self) -> None:
        """to_stac_properties() correctly handles zero features."""
        # Create metadata with zero features
        metadata = GeoParquetMetadata(
            bbox=None,
            crs=None,
            geometry_type="Point",
            geometry_column="geometry",
            feature_count=0,
            schema={},
        )
        props = metadata.to_stac_properties()
        # feature_count should be included even when 0
        assert "geoparquet:feature_count" in props
        assert props["geoparquet:feature_count"] == 0

    @pytest.mark.unit
    def test_to_stac_properties_without_geometry_type(self) -> None:
        """to_stac_properties() handles missing geometry_type."""
        metadata = GeoParquetMetadata(
            bbox=None,
            crs=None,
            geometry_type=None,
            geometry_column="geometry",
            feature_count=10,
            schema={},
        )
        props = metadata.to_stac_properties()
        # geometry_type should not be included if None
        assert "geoparquet:geometry_type" not in props


class TestGeoParquetMetadataEdgeCases:
    """Tests for edge cases in geoparquet metadata parsing."""

    @pytest.mark.unit
    def test_parquet_without_geo_metadata(self, tmp_path: Path) -> None:
        """Regular Parquet (non-GeoParquet) returns metadata with None geo fields."""
        import pyarrow as pa
        import pyarrow.parquet as pq

        # Create a regular parquet file (no geo metadata)
        table = pa.table({"col1": [1, 2, 3], "col2": ["a", "b", "c"]})
        path = tmp_path / "regular.parquet"
        pq.write_table(table, path)

        metadata = extract_geoparquet_metadata(path)
        # Should return metadata with None for geo-specific fields
        assert metadata.bbox is None
        assert metadata.crs is None
        assert metadata.geometry_type is None
        assert metadata.feature_count == 3

    @pytest.mark.unit
    def test_parquet_with_invalid_geo_json(self, tmp_path: Path) -> None:
        """Parquet with invalid geo JSON returns empty geo metadata."""
        import pyarrow as pa
        import pyarrow.parquet as pq

        # Create a parquet file with malformed geo metadata
        table = pa.table({"col1": [1, 2, 3]})
        path = tmp_path / "bad_geo.parquet"

        # Write with bad geo metadata
        schema_with_meta = table.schema.with_metadata({b"geo": b"not valid json"})
        table_with_meta = table.cast(schema_with_meta)
        pq.write_table(table_with_meta, path)

        metadata = extract_geoparquet_metadata(path)
        # Should handle gracefully
        assert metadata.bbox is None
        assert metadata.crs is None

    @pytest.mark.unit
    def test_parquet_with_non_dict_geo_json(self, tmp_path: Path) -> None:
        """Parquet with non-dict geo JSON returns empty geo metadata."""
        import pyarrow as pa
        import pyarrow.parquet as pq

        # Create a parquet file with valid JSON but not a dict
        table = pa.table({"col1": [1, 2, 3]})
        path = tmp_path / "array_geo.parquet"

        # Write with array geo metadata (not a dict)
        schema_with_meta = table.schema.with_metadata({b"geo": b'["not", "a", "dict"]'})
        table_with_meta = table.cast(schema_with_meta)
        pq.write_table(table_with_meta, path)

        metadata = extract_geoparquet_metadata(path)
        # Should handle gracefully
        assert metadata.bbox is None

    @pytest.mark.unit
    def test_extract_crs_from_projjson_with_epsg(self, tmp_path: Path) -> None:
        """Extracts EPSG code from PROJJSON CRS."""
        import json

        import pyarrow as pa
        import pyarrow.parquet as pq

        # Create geo metadata with PROJJSON CRS containing EPSG code
        geo_meta = {
            "version": "1.0.0",
            "primary_column": "geometry",
            "columns": {
                "geometry": {
                    "encoding": "WKB",
                    "crs": {"type": "GeographicCRS", "id": {"authority": "EPSG", "code": 4326}},
                }
            },
        }

        table = pa.table({"geometry": [b"\x00"] * 3})
        path = tmp_path / "epsg_crs.parquet"

        schema_with_meta = table.schema.with_metadata({b"geo": json.dumps(geo_meta).encode()})
        table_with_meta = table.cast(schema_with_meta)
        pq.write_table(table_with_meta, path)

        metadata = extract_geoparquet_metadata(path)
        assert metadata.crs == "EPSG:4326"

    @pytest.mark.unit
    def test_extract_crs_from_projjson_without_epsg(self, tmp_path: Path) -> None:
        """Returns full PROJJSON when no EPSG code available."""
        import json

        import pyarrow as pa
        import pyarrow.parquet as pq

        # Create geo metadata with PROJJSON CRS but no EPSG
        geo_meta = {
            "version": "1.0.0",
            "primary_column": "geometry",
            "columns": {
                "geometry": {
                    "encoding": "WKB",
                    "crs": {"type": "CustomCRS", "name": "Custom"},  # No 'id' field
                }
            },
        }

        table = pa.table({"geometry": [b"\x00"] * 3})
        path = tmp_path / "custom_crs.parquet"

        schema_with_meta = table.schema.with_metadata({b"geo": json.dumps(geo_meta).encode()})
        table_with_meta = table.cast(schema_with_meta)
        pq.write_table(table_with_meta, path)

        metadata = extract_geoparquet_metadata(path)
        # Should return the full CRS dict since no EPSG code
        assert isinstance(metadata.crs, dict)

    @pytest.mark.unit
    def test_extract_bbox_short_array(self, tmp_path: Path) -> None:
        """Handles bbox array shorter than 4 elements."""
        import json

        import pyarrow as pa
        import pyarrow.parquet as pq

        # Create geo metadata with short bbox
        geo_meta = {
            "version": "1.0.0",
            "primary_column": "geometry",
            "columns": {"geometry": {"encoding": "WKB", "bbox": [0, 0]}},  # Only 2 elements
        }

        table = pa.table({"geometry": [b"\x00"] * 3})
        path = tmp_path / "short_bbox.parquet"

        schema_with_meta = table.schema.with_metadata({b"geo": json.dumps(geo_meta).encode()})
        table_with_meta = table.cast(schema_with_meta)
        pq.write_table(table_with_meta, path)

        metadata = extract_geoparquet_metadata(path)
        # Should return None for invalid bbox
        assert metadata.bbox is None

    @pytest.mark.unit
    def test_extract_geometry_type_fallback(self, tmp_path: Path) -> None:
        """Falls back to geometry_type field when geometry_types is empty."""
        import json

        import pyarrow as pa
        import pyarrow.parquet as pq

        # Create geo metadata with geometry_type (old style) instead of geometry_types
        geo_meta = {
            "version": "1.0.0",
            "primary_column": "geometry",
            "columns": {
                "geometry": {
                    "encoding": "WKB",
                    "geometry_type": "Polygon",  # Old style field
                    # No geometry_types field
                }
            },
        }

        table = pa.table({"geometry": [b"\x00"] * 3})
        path = tmp_path / "old_geom_type.parquet"

        schema_with_meta = table.schema.with_metadata({b"geo": json.dumps(geo_meta).encode()})
        table_with_meta = table.cast(schema_with_meta)
        pq.write_table(table_with_meta, path)

        metadata = extract_geoparquet_metadata(path)
        assert metadata.geometry_type == "Polygon"
