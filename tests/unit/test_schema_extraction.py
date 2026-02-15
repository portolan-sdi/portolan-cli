"""Unit tests for schema extraction from GeoParquet and COG files.

User Story 2: Add Dataset with Collection Metadata

Tests cover:
- SchemaModel extraction from GeoParquet
- SchemaModel extraction from COG
- Handling missing CRS (warning, not error)
- Column and band metadata
"""

from __future__ import annotations

from pathlib import Path

import pytest

from portolan_cli.models.schema import ColumnSchema, SchemaModel


class TestGeoParquetSchemaExtraction:
    """Tests for extracting SchemaModel from GeoParquet files."""

    @pytest.fixture
    def sample_geoparquet(self, tmp_path: Path) -> Path:
        """Create a sample GeoParquet file for testing."""
        import json

        import pyarrow as pa
        import pyarrow.parquet as pq

        # Create schema with geometry column
        schema = pa.schema(
            [
                ("id", pa.int64()),
                ("name", pa.string()),
                ("population", pa.int64()),
                ("geometry", pa.binary()),
            ]
        )

        # Add GeoParquet metadata
        geo_metadata = {
            "version": "1.0.0",
            "primary_column": "geometry",
            "columns": {
                "geometry": {
                    "encoding": "WKB",
                    "geometry_types": ["Point"],
                    "crs": {"id": {"authority": "EPSG", "code": 4326}},
                }
            },
        }

        # Create table with geo metadata
        table = pa.table(
            {
                "id": [1, 2, 3],
                "name": ["a", "b", "c"],
                "population": [100, 200, 300],
                "geometry": [b"WKB1", b"WKB2", b"WKB3"],
            },
            schema=schema,
        )

        # Add geo metadata
        new_metadata = {b"geo": json.dumps(geo_metadata).encode()}
        existing_metadata = table.schema.metadata or {}
        table = table.replace_schema_metadata({**existing_metadata, **new_metadata})

        file_path = tmp_path / "test.parquet"
        pq.write_table(table, file_path)
        return file_path

    @pytest.mark.unit
    def test_extract_schema_returns_schema_model(self, sample_geoparquet: Path) -> None:
        """extract_schema_from_geoparquet should return a SchemaModel."""
        from portolan_cli.metadata.geoparquet import extract_schema_from_geoparquet

        schema = extract_schema_from_geoparquet(sample_geoparquet)

        assert isinstance(schema, SchemaModel)
        assert schema.format == "geoparquet"

    @pytest.mark.unit
    def test_extract_schema_has_columns(self, sample_geoparquet: Path) -> None:
        """Extracted schema should have all columns."""
        from portolan_cli.metadata.geoparquet import extract_schema_from_geoparquet

        schema = extract_schema_from_geoparquet(sample_geoparquet)

        column_names = [c.name for c in schema.columns]
        assert "id" in column_names
        assert "name" in column_names
        assert "population" in column_names
        assert "geometry" in column_names

    @pytest.mark.unit
    def test_extract_schema_column_types(self, sample_geoparquet: Path) -> None:
        """Columns should have correct types."""
        from portolan_cli.metadata.geoparquet import extract_schema_from_geoparquet

        schema = extract_schema_from_geoparquet(sample_geoparquet)

        columns_by_name = {c.name: c for c in schema.columns}
        assert "int64" in columns_by_name["id"].type
        assert "string" in columns_by_name["name"].type

    @pytest.mark.unit
    def test_extract_schema_geometry_metadata(self, sample_geoparquet: Path) -> None:
        """Geometry column should have geometry_type and crs."""
        from portolan_cli.metadata.geoparquet import extract_schema_from_geoparquet

        schema = extract_schema_from_geoparquet(sample_geoparquet)

        columns_by_name = {c.name: c for c in schema.columns}
        geom = columns_by_name["geometry"]
        assert geom.geometry_type == "Point"
        assert "4326" in str(geom.crs)

    @pytest.mark.unit
    def test_extract_schema_version(self, sample_geoparquet: Path) -> None:
        """Schema should have version 1.0.0."""
        from portolan_cli.metadata.geoparquet import extract_schema_from_geoparquet

        schema = extract_schema_from_geoparquet(sample_geoparquet)

        assert schema.schema_version == "1.0.0"


class TestMissingCRSWarning:
    """Tests for handling missing CRS."""

    @pytest.fixture
    def geoparquet_no_crs(self, tmp_path: Path) -> Path:
        """Create a GeoParquet file without CRS."""
        import json

        import pyarrow as pa
        import pyarrow.parquet as pq

        schema = pa.schema(
            [
                ("id", pa.int64()),
                ("geometry", pa.binary()),
            ]
        )

        # GeoParquet metadata without CRS
        geo_metadata = {
            "version": "1.0.0",
            "primary_column": "geometry",
            "columns": {
                "geometry": {
                    "encoding": "WKB",
                    "geometry_types": ["Point"],
                    # No CRS field
                }
            },
        }

        table = pa.table(
            {"id": [1], "geometry": [b"WKB"]},
            schema=schema,
        )

        new_metadata = {b"geo": json.dumps(geo_metadata).encode()}
        table = table.replace_schema_metadata(new_metadata)

        file_path = tmp_path / "no_crs.parquet"
        pq.write_table(table, file_path)
        return file_path

    @pytest.mark.unit
    def test_missing_crs_does_not_raise(self, geoparquet_no_crs: Path) -> None:
        """Missing CRS should not raise an error."""
        from portolan_cli.metadata.geoparquet import extract_schema_from_geoparquet

        # Should not raise
        schema = extract_schema_from_geoparquet(geoparquet_no_crs)
        assert schema is not None

    @pytest.mark.unit
    def test_missing_crs_returns_none(self, geoparquet_no_crs: Path) -> None:
        """Missing CRS should result in None crs field."""
        from portolan_cli.metadata.geoparquet import extract_schema_from_geoparquet

        schema = extract_schema_from_geoparquet(geoparquet_no_crs)

        columns_by_name = {c.name: c for c in schema.columns}
        assert columns_by_name["geometry"].crs is None

    @pytest.mark.unit
    def test_missing_crs_returns_warning(self, geoparquet_no_crs: Path) -> None:
        """Missing CRS should return a warning when requested."""
        from portolan_cli.metadata.geoparquet import extract_schema_from_geoparquet

        schema, warnings = extract_schema_from_geoparquet(geoparquet_no_crs, return_warnings=True)

        assert any("crs" in w.lower() for w in warnings)


class TestCOGSchemaExtraction:
    """Tests for extracting SchemaModel from COG files."""

    @pytest.fixture
    def sample_cog(self) -> Path:
        """Get a sample COG from fixtures."""
        # Use existing fixture - rgb.tif exists
        return Path("tests/fixtures/raster/valid/rgb.tif")

    @pytest.mark.unit
    def test_extract_schema_from_cog_returns_schema_model(self, sample_cog: Path) -> None:
        """extract_schema_from_cog should return a SchemaModel."""
        if not sample_cog.exists():
            pytest.skip("Fixture not available")

        from portolan_cli.metadata.cog import extract_schema_from_cog

        schema = extract_schema_from_cog(sample_cog)

        assert isinstance(schema, SchemaModel)
        assert schema.format == "cog"

    @pytest.mark.unit
    def test_cog_schema_has_bands(self, sample_cog: Path) -> None:
        """COG schema should have band information."""
        if not sample_cog.exists():
            pytest.skip("Fixture not available")

        from portolan_cli.metadata.cog import extract_schema_from_cog

        schema = extract_schema_from_cog(sample_cog)

        # RGB should have 3 bands
        assert len(schema.columns) == 3


class TestSchemaRoundtrip:
    """Tests for SchemaModel serialization."""

    @pytest.mark.unit
    def test_schema_to_json_and_back(self) -> None:
        """SchemaModel should survive JSON roundtrip."""
        import json

        original = SchemaModel(
            schema_version="1.0.0",
            format="geoparquet",
            columns=[
                ColumnSchema(
                    name="geometry",
                    type="binary",
                    nullable=False,
                    geometry_type="Polygon",
                    crs="EPSG:4326",
                ),
                ColumnSchema(
                    name="area",
                    type="float64",
                    nullable=True,
                    description="Area in square meters",
                    unit="m^2",
                ),
            ],
        )

        # Serialize and deserialize
        json_str = json.dumps(original.to_dict())
        restored = SchemaModel.from_dict(json.loads(json_str))

        assert restored.format == original.format
        assert len(restored.columns) == len(original.columns)
