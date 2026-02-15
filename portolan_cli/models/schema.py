"""SchemaModel dataclass for column and band metadata.

Schema describes the structure of data in a collection:
- For GeoParquet: column names, types, nullability, geometry info
- For COG: band names, data types, nodata values

Supports user-editable fields (description, unit, semantic_type) for
round-trip editing workflows.
"""

from __future__ import annotations

import re
from dataclasses import dataclass
from typing import Any

# Semantic version pattern
SEMVER_PATTERN = re.compile(r"^\d+\.\d+\.\d+$")


@dataclass
class ColumnSchema:
    """Column metadata for GeoParquet.

    Attributes:
        name: Column name (auto-extracted).
        type: Data type string (auto-extracted).
        nullable: Whether column allows nulls (auto-extracted).
        geometry_type: Geometry type for geometry columns (auto-extracted).
        crs: CRS as EPSG code (auto-extracted).
        description: Human-readable description (user-editable).
        unit: Unit of measurement (user-editable).
        semantic_type: Semantic type from controlled vocabulary (user-editable).
    """

    name: str
    type: str
    nullable: bool
    geometry_type: str | None = None
    crs: str | None = None
    description: str | None = None
    unit: str | None = None
    semantic_type: str | None = None

    def to_dict(self) -> dict[str, Any]:
        """Convert to JSON-serializable dict."""
        result: dict[str, Any] = {
            "name": self.name,
            "type": self.type,
            "nullable": self.nullable,
        }
        if self.geometry_type is not None:
            result["geometry_type"] = self.geometry_type
        if self.crs is not None:
            result["crs"] = self.crs
        if self.description is not None:
            result["description"] = self.description
        if self.unit is not None:
            result["unit"] = self.unit
        if self.semantic_type is not None:
            result["semantic_type"] = self.semantic_type
        return result

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> ColumnSchema:
        """Create ColumnSchema from dict."""
        return cls(
            name=data["name"],
            type=data["type"],
            nullable=data["nullable"],
            geometry_type=data.get("geometry_type"),
            crs=data.get("crs"),
            description=data.get("description"),
            unit=data.get("unit"),
            semantic_type=data.get("semantic_type"),
        )


@dataclass
class BandSchema:
    """Band metadata for COG.

    Attributes:
        name: Band name or index (auto-extracted).
        data_type: Data type string (auto-extracted).
        nodata: Nodata value (auto-extracted).
        description: Human-readable description (user-editable).
        unit: Unit of measurement (user-editable).
    """

    name: str
    data_type: str
    nodata: float | int | None = None
    description: str | None = None
    unit: str | None = None

    def to_dict(self) -> dict[str, Any]:
        """Convert to JSON-serializable dict."""
        result: dict[str, Any] = {
            "name": self.name,
            "data_type": self.data_type,
        }
        if self.nodata is not None:
            result["nodata"] = self.nodata
        if self.description is not None:
            result["description"] = self.description
        if self.unit is not None:
            result["unit"] = self.unit
        return result

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> BandSchema:
        """Create BandSchema from dict."""
        return cls(
            name=data["name"],
            data_type=data["data_type"],
            nodata=data.get("nodata"),
            description=data.get("description"),
            unit=data.get("unit"),
        )


@dataclass
class SchemaModel:
    """Schema metadata for a collection.

    Describes the structure of data files in the collection.

    Attributes:
        schema_version: Schema format version (semver).
        format: Data format ("geoparquet" or "cog").
        columns: Column definitions (list of ColumnSchema, BandSchema, or dicts).
        crs: Coordinate reference system (EPSG code or WKT) for the schema.
        statistics: Optional column statistics.
    """

    schema_version: str
    format: str
    columns: list[ColumnSchema | BandSchema | dict[str, Any]]
    crs: str | None = None
    statistics: dict[str, Any] | None = None

    def __post_init__(self) -> None:
        """Validate fields after initialization."""
        if not SEMVER_PATTERN.match(self.schema_version):
            raise ValueError(
                f"Invalid schema version '{self.schema_version}': must match semver pattern"
            )
        if len(self.columns) == 0:
            raise ValueError("Schema must have at least one column")

    def to_dict(self) -> dict[str, Any]:
        """Convert to JSON-serializable dict."""
        columns_data = []
        for col in self.columns:
            if isinstance(col, (ColumnSchema, BandSchema)):
                columns_data.append(col.to_dict())
            elif isinstance(col, dict):
                columns_data.append(col)
            else:
                # Handle other types with to_dict method
                columns_data.append(col.to_dict() if hasattr(col, "to_dict") else col)

        result: dict[str, Any] = {
            "schema_version": self.schema_version,
            "format": self.format,
            "columns": columns_data,
        }
        if self.crs is not None:
            result["crs"] = self.crs
        if self.statistics is not None:
            result["statistics"] = self.statistics
        return result

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> SchemaModel:
        """Create SchemaModel from dict."""
        columns: list[ColumnSchema | BandSchema | dict[str, Any]] = []
        for col_data in data.get("columns", []):
            # Try to parse as ColumnSchema if it has GeoParquet column fields
            if "name" in col_data and "type" in col_data and "nullable" in col_data:
                columns.append(ColumnSchema.from_dict(col_data))
            # Try to parse as BandSchema if it has COG band fields
            elif "name" in col_data and "data_type" in col_data:
                columns.append(BandSchema.from_dict(col_data))
            else:
                # Keep as dict for unknown formats
                columns.append(col_data)

        return cls(
            schema_version=data["schema_version"],
            format=data["format"],
            columns=columns,
            crs=data.get("crs"),
            statistics=data.get("statistics"),
        )
