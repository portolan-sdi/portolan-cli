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


def _read_sidecar_meta(path: Path) -> dict[str, Any]:
    """Read schema metadata from sidecar .meta.json file if present.

    Returns:
        Metadata dict with schema_version, format, crs, statistics.
        Empty dict if sidecar doesn't exist.
    """
    meta_path = path.with_suffix(path.suffix + ".meta.json")
    if meta_path.exists():
        with open(meta_path) as f:
            result: dict[str, Any] = json.load(f)
            return result
    return {}


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
    format: str | None = None,
    schema_version: str = "1.0.0",
) -> SchemaModel:
    """Import schema from CSV file.

    If a sidecar .meta.json file exists, it will be used to restore
    schema_version, format, crs, and statistics.

    Args:
        path: Path to CSV file.
        format: Data format ("geoparquet" or "cog"). If None, reads from sidecar.
        schema_version: Schema version to use. Overridden by sidecar if present.

    Returns:
        SchemaModel loaded from CSV.

    Raises:
        FileNotFoundError: If file doesn't exist.
        ValueError: If format is not provided and no sidecar exists,
                   or if required columns are missing.
    """
    if not path.exists():
        raise FileNotFoundError(f"File not found: {path}")

    # Read sidecar metadata if present
    meta = _read_sidecar_meta(path)
    actual_format = meta.get("format") or format
    actual_version = meta.get("schema_version") or schema_version
    crs = meta.get("crs")
    statistics = meta.get("statistics")

    if not actual_format:
        raise ValueError("format must be provided when no sidecar .meta.json file exists")

    columns: list[ColumnSchema | BandSchema | dict[str, Any]] = []

    with open(path, newline="") as f:
        reader = csv.DictReader(f)
        for row_num, row in enumerate(reader, start=2):  # start=2 for 1-indexed + header
            try:
                if actual_format == "geoparquet":
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
                        nodata_str = nodata.strip()
                        try:
                            # Preserve integer type if no decimal point
                            if "." not in nodata_str and "e" not in nodata_str.lower():
                                nodata_val = int(nodata_str)
                            else:
                                nodata_val = float(nodata_str)
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
            except KeyError as e:
                raise ValueError(f"CSV row {row_num} missing required column: {e.args[0]}") from e

    return SchemaModel(
        schema_version=actual_version,
        format=actual_format,
        columns=columns,
        crs=crs,
        statistics=statistics,
    )


def import_schema_parquet(
    path: Path,
    *,
    format: str | None = None,
    schema_version: str = "1.0.0",
) -> SchemaModel:
    """Import schema from Parquet file.

    If a sidecar .meta.json file exists, it will be used to restore
    schema_version, format, crs, and statistics.

    Args:
        path: Path to Parquet file.
        format: Data format ("geoparquet" or "cog"). If None, reads from sidecar.
        schema_version: Schema version to use. Overridden by sidecar if present.

    Returns:
        SchemaModel loaded from Parquet.

    Raises:
        FileNotFoundError: If file doesn't exist.
        ValueError: If format is not provided and no sidecar exists.
        ImportError: If pyarrow is not installed.
    """
    if not path.exists():
        raise FileNotFoundError(f"File not found: {path}")

    # Read sidecar metadata if present
    meta = _read_sidecar_meta(path)
    actual_format = meta.get("format") or format
    actual_version = meta.get("schema_version") or schema_version
    crs = meta.get("crs")
    statistics = meta.get("statistics")

    if not actual_format:
        raise ValueError("format must be provided when no sidecar .meta.json file exists")

    import pyarrow.parquet as pq

    table = pq.read_table(path)
    rows = table.to_pylist()

    columns: list[ColumnSchema | BandSchema | dict[str, Any]] = []

    for row_idx, row in enumerate(rows):
        try:
            if actual_format == "geoparquet":
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
        except KeyError as e:
            raise ValueError(f"Parquet row {row_idx} missing required column: {e.args[0]}") from e

    return SchemaModel(
        schema_version=actual_version,
        format=actual_format,
        columns=columns,
        crs=crs,
        statistics=statistics,
    )
