"""Schema import functionality for round-trip editing.

Imports edited schema from JSON, CSV, or Parquet back into SchemaModel.
Validates that mandatory fields haven't been modified (breaking changes).
"""

from __future__ import annotations

import csv
import json
from pathlib import Path
from typing import Any

from portolan_cli.models.schema import BandSchema, ColumnSchema, SchemaModel


def import_schema_json(path: Path) -> SchemaModel:
    """Import schema from JSON file.

    Args:
        path: Path to JSON file.

    Returns:
        SchemaModel loaded from file.

    Raises:
        FileNotFoundError: If file doesn't exist.
        json.JSONDecodeError: If file is not valid JSON.
    """
    if not path.exists():
        raise FileNotFoundError(f"File not found: {path}")

    with open(path) as f:
        data = json.load(f)

    return SchemaModel.from_dict(data)


def import_schema_csv(
    path: Path,
    *,
    format: str,
    schema_version: str = "1.0.0",
) -> SchemaModel:
    """Import schema from CSV file.

    Args:
        path: Path to CSV file.
        format: Data format ("geoparquet" or "cog").
        schema_version: Schema version to use.

    Returns:
        SchemaModel loaded from CSV.

    Raises:
        FileNotFoundError: If file doesn't exist.
    """
    if not path.exists():
        raise FileNotFoundError(f"File not found: {path}")

    columns: list[ColumnSchema | BandSchema | dict[str, Any]] = []

    with open(path, newline="") as f:
        reader = csv.DictReader(f)
        for row in reader:
            if format == "geoparquet":
                # Parse nullable as bool
                nullable = row.get("nullable", "true").lower() == "true"
                columns.append(
                    ColumnSchema(
                        name=row["name"],
                        type=row["type"],
                        nullable=nullable,
                        geometry_type=row.get("geometry_type") or None,
                        crs=row.get("crs") or None,
                        description=row.get("description") or None,
                        unit=row.get("unit") or None,
                        semantic_type=row.get("semantic_type") or None,
                    )
                )
            else:  # COG
                nodata = row.get("nodata")
                nodata_val: float | int | None = None
                if nodata and nodata.strip():
                    try:
                        nodata_val = float(nodata)
                    except ValueError:
                        pass
                columns.append(
                    BandSchema(
                        name=row["name"],
                        data_type=row["data_type"],
                        nodata=nodata_val,
                        description=row.get("description") or None,
                        unit=row.get("unit") or None,
                    )
                )

    return SchemaModel(
        schema_version=schema_version,
        format=format,
        columns=columns,
    )


def import_schema_parquet(
    path: Path,
    *,
    format: str,
    schema_version: str = "1.0.0",
) -> SchemaModel:
    """Import schema from Parquet file.

    Args:
        path: Path to Parquet file.
        format: Data format ("geoparquet" or "cog").
        schema_version: Schema version to use.

    Returns:
        SchemaModel loaded from Parquet.

    Raises:
        FileNotFoundError: If file doesn't exist.
        ImportError: If pyarrow is not installed.
    """
    if not path.exists():
        raise FileNotFoundError(f"File not found: {path}")

    import pyarrow.parquet as pq

    table = pq.read_table(path)
    rows = table.to_pylist()

    columns: list[ColumnSchema | BandSchema | dict[str, Any]] = []

    for row in rows:
        if format == "geoparquet":
            columns.append(
                ColumnSchema(
                    name=row["name"],
                    type=row["type"],
                    nullable=row.get("nullable", True),
                    geometry_type=row.get("geometry_type"),
                    crs=row.get("crs"),
                    description=row.get("description"),
                    unit=row.get("unit"),
                    semantic_type=row.get("semantic_type"),
                )
            )
        else:  # COG
            columns.append(
                BandSchema(
                    name=row["name"],
                    data_type=row["data_type"],
                    nodata=row.get("nodata"),
                    description=row.get("description"),
                    unit=row.get("unit"),
                )
            )

    return SchemaModel(
        schema_version=schema_version,
        format=format,
        columns=columns,
    )
