"""Unit tests for schema import functionality."""

from __future__ import annotations

import csv
import json
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from portolan_cli.models.schema import BandSchema, ColumnSchema
from portolan_cli.schema.import_ import (
    import_schema_csv,
    import_schema_json,
    import_schema_parquet,
)


class TestImportSchemaJson:
    """Tests for import_schema_json."""

    @pytest.mark.unit
    def test_import_geoparquet_schema_from_json(self, tmp_path: Path) -> None:
        """Can import GeoParquet schema from JSON."""
        data = {
            "schema_version": "1.0.0",
            "format": "geoparquet",
            "columns": [
                {
                    "name": "geometry",
                    "type": "binary",
                    "nullable": False,
                    "geometry_type": "Polygon",
                    "crs": "EPSG:4326",
                },
                {"name": "name", "type": "string", "nullable": False},
            ],
        }
        path = tmp_path / "schema.json"
        with open(path, "w") as f:
            json.dump(data, f)

        schema = import_schema_json(path)

        assert schema.format == "geoparquet"
        assert len(schema.columns) == 2
        assert isinstance(schema.columns[0], ColumnSchema)

    @pytest.mark.unit
    def test_import_cog_schema_from_json(self, tmp_path: Path) -> None:
        """Can import COG schema from JSON."""
        data = {
            "schema_version": "1.0.0",
            "format": "cog",
            "crs": "EPSG:32610",
            "columns": [
                {"name": "band_1", "data_type": "uint8", "nodata": 0},
                {"name": "band_2", "data_type": "uint8"},
            ],
        }
        path = tmp_path / "schema.json"
        with open(path, "w") as f:
            json.dump(data, f)

        schema = import_schema_json(path)

        assert schema.format == "cog"
        assert schema.crs == "EPSG:32610"
        assert len(schema.columns) == 2
        assert isinstance(schema.columns[0], BandSchema)

    @pytest.mark.unit
    def test_import_json_file_not_found(self, tmp_path: Path) -> None:
        """Raises FileNotFoundError for missing file."""
        with pytest.raises(FileNotFoundError):
            import_schema_json(tmp_path / "nonexistent.json")


class TestImportSchemaCsv:
    """Tests for import_schema_csv."""

    @pytest.mark.unit
    def test_import_geoparquet_schema_from_csv(self, tmp_path: Path) -> None:
        """Can import GeoParquet schema from CSV."""
        path = tmp_path / "schema.csv"
        with open(path, "w", newline="") as f:
            writer = csv.DictWriter(
                f,
                fieldnames=[
                    "name",
                    "type",
                    "nullable",
                    "geometry_type",
                    "crs",
                    "description",
                ],
            )
            writer.writeheader()
            writer.writerow(
                {
                    "name": "geometry",
                    "type": "binary",
                    "nullable": "false",
                    "geometry_type": "Polygon",
                    "crs": "EPSG:4326",
                    "description": "Boundary",
                }
            )
            writer.writerow(
                {
                    "name": "name",
                    "type": "string",
                    "nullable": "true",
                    "geometry_type": "",
                    "crs": "",
                    "description": "Name field",
                }
            )

        schema = import_schema_csv(path, format="geoparquet")

        assert schema.format == "geoparquet"
        assert len(schema.columns) == 2
        assert isinstance(schema.columns[0], ColumnSchema)
        assert schema.columns[0].geometry_type == "Polygon"
        assert schema.columns[1].nullable is True

    @pytest.mark.unit
    def test_import_cog_schema_from_csv(self, tmp_path: Path) -> None:
        """Can import COG schema from CSV."""
        path = tmp_path / "schema.csv"
        with open(path, "w", newline="") as f:
            writer = csv.DictWriter(f, fieldnames=["name", "data_type", "nodata", "description"])
            writer.writeheader()
            writer.writerow(
                {"name": "band_1", "data_type": "uint8", "nodata": "0", "description": "Red"}
            )
            writer.writerow(
                {"name": "band_2", "data_type": "uint8", "nodata": "", "description": ""}
            )

        schema = import_schema_csv(path, format="cog")

        assert schema.format == "cog"
        assert len(schema.columns) == 2
        assert isinstance(schema.columns[0], BandSchema)
        assert schema.columns[0].nodata == 0.0
        assert schema.columns[1].nodata is None

    @pytest.mark.unit
    def test_import_csv_file_not_found(self, tmp_path: Path) -> None:
        """Raises FileNotFoundError for missing file."""
        with pytest.raises(FileNotFoundError):
            import_schema_csv(tmp_path / "nonexistent.csv", format="geoparquet")


class TestImportSchemaParquet:
    """Tests for import_schema_parquet."""

    @pytest.mark.unit
    def test_import_geoparquet_schema_from_parquet(self, tmp_path: Path) -> None:
        """Can import GeoParquet schema from Parquet."""
        rows = [
            {
                "name": "geometry",
                "type": "binary",
                "nullable": False,
                "geometry_type": "Polygon",
                "crs": "EPSG:4326",
            },
            {
                "name": "name",
                "type": "string",
                "nullable": True,
                "geometry_type": None,
                "crs": None,
            },
        ]
        table = pa.Table.from_pylist(rows)
        path = tmp_path / "schema.parquet"
        pq.write_table(table, path)

        schema = import_schema_parquet(path, format="geoparquet")

        assert schema.format == "geoparquet"
        assert len(schema.columns) == 2
        assert isinstance(schema.columns[0], ColumnSchema)

    @pytest.mark.unit
    def test_import_cog_schema_from_parquet(self, tmp_path: Path) -> None:
        """Can import COG schema from Parquet."""
        rows = [
            {"name": "band_1", "data_type": "uint8", "nodata": 0},
            {"name": "band_2", "data_type": "float32", "nodata": None},
        ]
        table = pa.Table.from_pylist(rows)
        path = tmp_path / "schema.parquet"
        pq.write_table(table, path)

        schema = import_schema_parquet(path, format="cog")

        assert schema.format == "cog"
        assert len(schema.columns) == 2
        assert isinstance(schema.columns[0], BandSchema)

    @pytest.mark.unit
    def test_import_parquet_file_not_found(self, tmp_path: Path) -> None:
        """Raises FileNotFoundError for missing file."""
        with pytest.raises(FileNotFoundError):
            import_schema_parquet(tmp_path / "nonexistent.parquet", format="geoparquet")
