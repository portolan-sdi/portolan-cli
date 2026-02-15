"""Unit tests for schema export functionality."""

from __future__ import annotations

from pathlib import Path

import pytest

from portolan_cli.models.schema import BandSchema, ColumnSchema, SchemaModel
from portolan_cli.schema.export import (
    export_schema_csv,
    export_schema_json,
    export_schema_parquet,
)


@pytest.fixture
def geoparquet_schema() -> SchemaModel:
    """Sample GeoParquet schema for testing."""
    return SchemaModel(
        schema_version="1.0.0",
        format="geoparquet",
        columns=[
            ColumnSchema(
                name="geometry",
                type="binary",
                nullable=False,
                geometry_type="Polygon",
                crs="EPSG:4326",
                description="County boundary",
            ),
            ColumnSchema(
                name="name",
                type="string",
                nullable=False,
                description="County name",
            ),
            ColumnSchema(
                name="population",
                type="int64",
                nullable=True,
                unit="people",
            ),
        ],
    )


@pytest.fixture
def cog_schema() -> SchemaModel:
    """Sample COG schema for testing."""
    return SchemaModel(
        schema_version="1.0.0",
        format="cog",
        crs="EPSG:32610",
        columns=[
            BandSchema(
                name="band_1",
                data_type="uint8",
                nodata=0,
                description="Red band",
            ),
            BandSchema(
                name="band_2",
                data_type="uint8",
                nodata=0,
                description="Green band",
            ),
        ],
    )


class TestExportSchemaJson:
    """Tests for export_schema_json."""

    @pytest.mark.unit
    def test_export_geoparquet_schema_to_json(
        self, tmp_path: Path, geoparquet_schema: SchemaModel
    ) -> None:
        """Can export GeoParquet schema to JSON."""
        output = tmp_path / "schema.json"
        result = export_schema_json(geoparquet_schema, output)

        assert result == output
        assert output.exists()

        # Verify content
        import json

        with open(output) as f:
            data = json.load(f)

        assert data["schema_version"] == "1.0.0"
        assert data["format"] == "geoparquet"
        assert len(data["columns"]) == 3

    @pytest.mark.unit
    def test_export_cog_schema_to_json(self, tmp_path: Path, cog_schema: SchemaModel) -> None:
        """Can export COG schema to JSON."""
        output = tmp_path / "schema.json"
        result = export_schema_json(cog_schema, output)

        assert result == output
        assert output.exists()


class TestExportSchemaCsv:
    """Tests for export_schema_csv."""

    @pytest.mark.unit
    def test_export_geoparquet_schema_to_csv(
        self, tmp_path: Path, geoparquet_schema: SchemaModel
    ) -> None:
        """Can export GeoParquet schema to CSV."""
        output = tmp_path / "schema.csv"
        result = export_schema_csv(geoparquet_schema, output)

        assert result == output
        assert output.exists()

        # Verify content
        import csv

        with open(output) as f:
            reader = csv.DictReader(f)
            rows = list(reader)

        assert len(rows) == 3
        assert rows[0]["name"] == "geometry"
        assert rows[0]["type"] == "binary"

    @pytest.mark.unit
    def test_export_cog_schema_to_csv(self, tmp_path: Path, cog_schema: SchemaModel) -> None:
        """Can export COG schema to CSV."""
        output = tmp_path / "schema.csv"
        result = export_schema_csv(cog_schema, output)

        assert result == output
        assert output.exists()

        import csv

        with open(output) as f:
            reader = csv.DictReader(f)
            rows = list(reader)

        assert len(rows) == 2
        assert rows[0]["name"] == "band_1"
        assert rows[0]["data_type"] == "uint8"


class TestExportSchemaParquet:
    """Tests for export_schema_parquet."""

    @pytest.mark.unit
    def test_export_geoparquet_schema_to_parquet(
        self, tmp_path: Path, geoparquet_schema: SchemaModel
    ) -> None:
        """Can export GeoParquet schema to Parquet."""
        output = tmp_path / "schema.parquet"
        result = export_schema_parquet(geoparquet_schema, output)

        assert result == output
        assert output.exists()

        # Verify content
        import pyarrow.parquet as pq

        table = pq.read_table(output)
        assert len(table) == 3

    @pytest.mark.unit
    def test_export_cog_schema_to_parquet(self, tmp_path: Path, cog_schema: SchemaModel) -> None:
        """Can export COG schema to Parquet."""
        output = tmp_path / "schema.parquet"
        result = export_schema_parquet(cog_schema, output)

        assert result == output
        assert output.exists()

        import pyarrow.parquet as pq

        table = pq.read_table(output)
        assert len(table) == 2
