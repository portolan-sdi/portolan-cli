"""Integration tests for schema round-trip editing.

Tests the full workflow:
1. Extract schema from data file
2. Export to JSON/CSV/Parquet
3. Edit (add descriptions, units)
4. Import back
5. Verify no breaking changes for metadata edits
"""

from __future__ import annotations

from pathlib import Path

import pytest

from portolan_cli.models.schema import ColumnSchema, SchemaModel
from portolan_cli.schema import (
    detect_breaking_changes,
    export_schema_csv,
    export_schema_json,
    export_schema_parquet,
    import_schema_csv,
    import_schema_json,
    import_schema_parquet,
    is_breaking,
)


class TestSchemaJsonRoundtrip:
    """Tests for JSON round-trip editing."""

    @pytest.mark.integration
    def test_json_roundtrip_preserves_data(self, tmp_path: Path) -> None:
        """Export -> Import preserves all schema data."""
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
                    description="County boundary",
                ),
                ColumnSchema(
                    name="name",
                    type="string",
                    nullable=False,
                    description="County name",
                    semantic_type="osi:Name",
                ),
            ],
        )

        # Export
        json_path = tmp_path / "schema.json"
        export_schema_json(original, json_path)

        # Import
        imported = import_schema_json(json_path)

        # Verify
        assert imported.schema_version == original.schema_version
        assert imported.format == original.format
        assert len(imported.columns) == len(original.columns)

        # Verify column details
        assert isinstance(imported.columns[0], ColumnSchema)
        assert imported.columns[0].description == "County boundary"
        assert imported.columns[1].semantic_type == "osi:Name"

    @pytest.mark.integration
    def test_metadata_edit_not_breaking(self, tmp_path: Path) -> None:
        """Editing description/unit/semantic_type is not breaking."""
        original = SchemaModel(
            schema_version="1.0.0",
            format="geoparquet",
            columns=[
                ColumnSchema(
                    name="population",
                    type="int64",
                    nullable=True,
                ),
            ],
        )

        # Export
        json_path = tmp_path / "schema.json"
        export_schema_json(original, json_path)

        # Simulate edit: add description and unit
        import json

        with open(json_path) as f:
            data = json.load(f)
        data["columns"][0]["description"] = "Total population count"
        data["columns"][0]["unit"] = "people"
        with open(json_path, "w") as f:
            json.dump(data, f)

        # Import edited schema
        edited = import_schema_json(json_path)

        # Verify not breaking
        assert not is_breaking(original, edited)
        changes = detect_breaking_changes(original, edited)
        assert len(changes) == 0


class TestSchemaCsvRoundtrip:
    """Tests for CSV round-trip editing."""

    @pytest.mark.integration
    def test_csv_roundtrip_preserves_data(self, tmp_path: Path) -> None:
        """Export -> Import preserves schema data via CSV."""
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
                    name="name",
                    type="string",
                    nullable=True,
                ),
            ],
        )

        # Export to CSV
        csv_path = tmp_path / "schema.csv"
        export_schema_csv(original, csv_path)

        # Import from CSV
        imported = import_schema_csv(csv_path, format="geoparquet")

        # Verify
        assert imported.format == original.format
        assert len(imported.columns) == len(original.columns)
        assert isinstance(imported.columns[0], ColumnSchema)
        assert imported.columns[0].geometry_type == "Polygon"


class TestSchemaParquetRoundtrip:
    """Tests for Parquet round-trip editing."""

    @pytest.mark.integration
    def test_parquet_roundtrip_preserves_data(self, tmp_path: Path) -> None:
        """Export -> Import preserves schema data via Parquet."""
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
                    name="value",
                    type="float64",
                    nullable=True,
                    description="Measurement value",
                ),
            ],
        )

        # Export to Parquet
        parquet_path = tmp_path / "schema.parquet"
        export_schema_parquet(original, parquet_path)

        # Import from Parquet
        imported = import_schema_parquet(parquet_path, format="geoparquet")

        # Verify core fields that Parquet reliably preserves
        assert imported.format == original.format
        assert len(imported.columns) == len(original.columns)
        assert isinstance(imported.columns[0], ColumnSchema)
        assert imported.columns[0].name == "geometry"
        assert imported.columns[0].type == "binary"
        assert imported.columns[1].name == "value"
        assert imported.columns[1].type == "float64"


class TestBreakingChangeWorkflow:
    """Tests for breaking change detection in editing workflow."""

    @pytest.mark.integration
    def test_type_change_detected_after_edit(self, tmp_path: Path) -> None:
        """Detects breaking changes when type is modified."""
        original = SchemaModel(
            schema_version="1.0.0",
            format="geoparquet",
            columns=[
                ColumnSchema(
                    name="count",
                    type="int64",
                    nullable=True,
                ),
            ],
        )

        # Export
        json_path = tmp_path / "schema.json"
        export_schema_json(original, json_path)

        # Simulate breaking edit: change type
        import json

        with open(json_path) as f:
            data = json.load(f)
        data["columns"][0]["type"] = "string"  # Breaking change!
        with open(json_path, "w") as f:
            json.dump(data, f)

        # Import edited schema
        edited = import_schema_json(json_path)

        # Verify breaking change detected
        assert is_breaking(original, edited)
        changes = detect_breaking_changes(original, edited)
        assert len(changes) == 1
        assert changes[0].change_type == "type_changed"

    @pytest.mark.integration
    def test_column_removal_detected_after_edit(self, tmp_path: Path) -> None:
        """Detects breaking changes when column is removed."""
        original = SchemaModel(
            schema_version="1.0.0",
            format="geoparquet",
            columns=[
                ColumnSchema(name="a", type="int64", nullable=False),
                ColumnSchema(name="b", type="string", nullable=True),
            ],
        )

        # Export
        json_path = tmp_path / "schema.json"
        export_schema_json(original, json_path)

        # Simulate breaking edit: remove column
        import json

        with open(json_path) as f:
            data = json.load(f)
        data["columns"] = [data["columns"][0]]  # Remove column b
        with open(json_path, "w") as f:
            json.dump(data, f)

        # Import edited schema
        edited = import_schema_json(json_path)

        # Verify breaking change detected
        assert is_breaking(original, edited)
        changes = detect_breaking_changes(original, edited)
        assert len(changes) == 1
        assert changes[0].change_type == "column_removed"
        assert changes[0].element == "b"
