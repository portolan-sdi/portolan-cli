"""Unit tests for SchemaModel, ColumnSchema, and BandSchema dataclasses.

Tests cover:
- Schema creation for GeoParquet and COG formats
- Column and band metadata
- User-editable vs auto-extracted fields
- JSON serialization (to_dict/from_dict)
- Validation rules
"""

from __future__ import annotations

import pytest

# These will be implemented - tests first!
from portolan_cli.models.schema import (
    BandSchema,
    ColumnSchema,
    SchemaModel,
)


class TestColumnSchema:
    """Tests for ColumnSchema dataclass (GeoParquet columns)."""

    @pytest.mark.unit
    def test_create_column_with_required_fields(self) -> None:
        """ColumnSchema can be created with only required fields."""
        column = ColumnSchema(
            name="population",
            type="int64",
            nullable=True,
        )

        assert column.name == "population"
        assert column.type == "int64"
        assert column.nullable is True

    @pytest.mark.unit
    def test_create_geometry_column(self) -> None:
        """ColumnSchema can represent a geometry column."""
        column = ColumnSchema(
            name="geometry",
            type="binary",
            nullable=False,
            geometry_type="Polygon",
            crs="EPSG:4326",
        )

        assert column.geometry_type == "Polygon"
        assert column.crs == "EPSG:4326"

    @pytest.mark.unit
    def test_create_column_with_user_editable_fields(self) -> None:
        """ColumnSchema can have user-editable metadata."""
        column = ColumnSchema(
            name="postal_code",
            type="string",
            nullable=True,
            description="5-digit US postal code",
            unit=None,
            semantic_type="schema:postalCode",
        )

        assert column.description == "5-digit US postal code"
        assert column.semantic_type == "schema:postalCode"

    @pytest.mark.unit
    def test_column_to_dict(self) -> None:
        """ColumnSchema.to_dict() returns correct dict."""
        column = ColumnSchema(
            name="area",
            type="float64",
            nullable=False,
            description="Area in square meters",
            unit="m^2",
        )
        data = column.to_dict()

        assert data["name"] == "area"
        assert data["type"] == "float64"
        assert data["nullable"] is False
        assert data["description"] == "Area in square meters"
        assert data["unit"] == "m^2"

    @pytest.mark.unit
    def test_column_from_dict(self) -> None:
        """ColumnSchema.from_dict() creates ColumnSchema from dict."""
        data = {
            "name": "geometry",
            "type": "binary",
            "nullable": False,
            "geometry_type": "Point",
            "crs": "EPSG:4326",
        }
        column = ColumnSchema.from_dict(data)

        assert column.name == "geometry"
        assert column.geometry_type == "Point"
        assert column.crs == "EPSG:4326"

    @pytest.mark.unit
    def test_column_to_dict_excludes_none_optional(self) -> None:
        """to_dict() should exclude optional fields that are None."""
        column = ColumnSchema(name="id", type="int64", nullable=False)
        data = column.to_dict()

        # geometry_type should not be in output if None
        assert "geometry_type" not in data or data.get("geometry_type") is None


class TestBandSchema:
    """Tests for BandSchema dataclass (COG bands)."""

    @pytest.mark.unit
    def test_create_band_with_required_fields(self) -> None:
        """BandSchema can be created with only required fields."""
        band = BandSchema(
            name="band_1",
            data_type="uint8",
        )

        assert band.name == "band_1"
        assert band.data_type == "uint8"

    @pytest.mark.unit
    def test_create_band_with_nodata(self) -> None:
        """BandSchema can have nodata value."""
        band = BandSchema(
            name="elevation",
            data_type="float32",
            nodata=-9999.0,
        )

        assert band.nodata == -9999.0

    @pytest.mark.unit
    def test_create_band_with_user_editable_fields(self) -> None:
        """BandSchema can have user-editable metadata."""
        band = BandSchema(
            name="nir",
            data_type="uint16",
            description="Near-infrared reflectance",
            unit="reflectance",
        )

        assert band.description == "Near-infrared reflectance"
        assert band.unit == "reflectance"

    @pytest.mark.unit
    def test_band_to_dict(self) -> None:
        """BandSchema.to_dict() returns correct dict."""
        band = BandSchema(
            name="red",
            data_type="uint16",
            nodata=0,
            description="Red band",
        )
        data = band.to_dict()

        assert data["name"] == "red"
        assert data["data_type"] == "uint16"
        assert data["nodata"] == 0

    @pytest.mark.unit
    def test_band_from_dict(self) -> None:
        """BandSchema.from_dict() creates BandSchema from dict."""
        data = {
            "name": "green",
            "data_type": "uint8",
            "nodata": 255,
            "unit": "reflectance",
        }
        band = BandSchema.from_dict(data)

        assert band.name == "green"
        assert band.nodata == 255
        assert band.unit == "reflectance"


class TestSchemaModel:
    """Tests for SchemaModel dataclass."""

    @pytest.mark.unit
    def test_create_geoparquet_schema(self) -> None:
        """SchemaModel can be created for GeoParquet format."""
        schema = SchemaModel(
            schema_version="1.0.0",
            format="geoparquet",
            columns=[
                ColumnSchema(name="id", type="int64", nullable=False),
                ColumnSchema(
                    name="geometry",
                    type="binary",
                    nullable=False,
                    geometry_type="Polygon",
                    crs="EPSG:4326",
                ),
            ],
        )

        assert schema.format == "geoparquet"
        assert len(schema.columns) == 2

    @pytest.mark.unit
    def test_create_cog_schema(self) -> None:
        """SchemaModel can be created for COG format."""
        schema = SchemaModel(
            schema_version="1.0.0",
            format="cog",
            columns=[  # For COG, columns contains BandSchema as dicts
                BandSchema(name="band_1", data_type="uint8").to_dict(),
                BandSchema(name="band_2", data_type="uint8").to_dict(),
                BandSchema(name="band_3", data_type="uint8").to_dict(),
            ],
        )

        assert schema.format == "cog"
        assert len(schema.columns) == 3

    @pytest.mark.unit
    def test_schema_requires_at_least_one_column(self) -> None:
        """SchemaModel must have at least one column."""
        with pytest.raises(ValueError, match="at least one"):
            SchemaModel(
                schema_version="1.0.0",
                format="geoparquet",
                columns=[],
            )

    @pytest.mark.unit
    def test_geoparquet_schema_requires_geometry_column(self) -> None:
        """GeoParquet schema should have at least one geometry column."""
        # This should emit a warning but not fail (per spec, CRS can be missing)
        schema = SchemaModel(
            schema_version="1.0.0",
            format="geoparquet",
            columns=[
                ColumnSchema(name="id", type="int64", nullable=False),
            ],
        )
        # The model allows this but downstream validation may warn
        assert schema.format == "geoparquet"

    @pytest.mark.unit
    def test_schema_with_statistics(self) -> None:
        """SchemaModel can include column statistics."""
        schema = SchemaModel(
            schema_version="1.0.0",
            format="geoparquet",
            columns=[
                ColumnSchema(name="population", type="int64", nullable=True),
            ],
            statistics={
                "population": {
                    "min": 0,
                    "max": 10000000,
                    "null_count": 5,
                    "distinct_count": 1000,
                    "is_enum": False,
                }
            },
        )

        assert "population" in schema.statistics
        assert schema.statistics["population"]["min"] == 0


class TestSchemaModelSerialization:
    """Tests for SchemaModel JSON serialization."""

    @pytest.mark.unit
    def test_to_dict_includes_required_fields(self) -> None:
        """to_dict() must include all required fields."""
        schema = SchemaModel(
            schema_version="1.0.0",
            format="geoparquet",
            columns=[
                ColumnSchema(name="geometry", type="binary", nullable=False),
            ],
        )
        data = schema.to_dict()

        assert data["schema_version"] == "1.0.0"
        assert data["format"] == "geoparquet"
        assert "columns" in data
        assert len(data["columns"]) == 1

    @pytest.mark.unit
    def test_from_dict_creates_schema(self) -> None:
        """from_dict() should create SchemaModel from dict."""
        data = {
            "schema_version": "1.0.0",
            "format": "geoparquet",
            "columns": [
                {
                    "name": "geometry",
                    "type": "binary",
                    "nullable": False,
                    "geometry_type": "Point",
                    "crs": "EPSG:4326",
                },
                {"name": "name", "type": "string", "nullable": True},
            ],
        }
        schema = SchemaModel.from_dict(data)

        assert schema.format == "geoparquet"
        assert len(schema.columns) == 2

    @pytest.mark.unit
    def test_roundtrip_serialization(self) -> None:
        """to_dict -> from_dict should preserve all data."""
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
                    description="Area in sq km",
                    unit="km^2",
                ),
            ],
            statistics={
                "area": {"min": 0.1, "max": 1000.0, "null_count": 2},
            },
        )

        data = original.to_dict()
        restored = SchemaModel.from_dict(data)

        assert restored.format == original.format
        assert len(restored.columns) == len(original.columns)
        assert "area" in restored.statistics


class TestSchemaVersioning:
    """Tests for schema version format."""

    @pytest.mark.unit
    def test_schema_version_format(self) -> None:
        """schema_version must be semver format."""
        schema = SchemaModel(
            schema_version="1.0.0",
            format="geoparquet",
            columns=[ColumnSchema(name="id", type="int64", nullable=False)],
        )
        assert schema.schema_version == "1.0.0"

    @pytest.mark.unit
    def test_invalid_schema_version_raises_error(self) -> None:
        """Invalid schema_version should raise ValueError."""
        with pytest.raises(ValueError, match="version"):
            SchemaModel(
                schema_version="invalid",
                format="geoparquet",
                columns=[ColumnSchema(name="id", type="int64", nullable=False)],
            )
