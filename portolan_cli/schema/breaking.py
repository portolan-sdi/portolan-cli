"""Breaking change detection for schema evolution.

Detects breaking changes between schema versions per Issue #77 spec:

GeoParquet breaking changes:
- Column removed
- Column type changed
- Column renamed
- Geometry type changed
- CRS changed

COG breaking changes:
- Band removed
- Band data_type changed
- CRS changed
- Resolution changed (not tracked in schema)
- Nodata changed

NOT breaking:
- Adding columns or bands (additive)
- Changing description, unit, semantic_type (metadata-only)
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any

from portolan_cli.models.schema import BandSchema, ColumnSchema, SchemaModel


@dataclass
class BreakingChange:
    """A single breaking change detected between schemas."""

    change_type: str
    """Type of change: column_removed, type_changed, crs_changed, etc."""

    element: str
    """Name of affected element (column or band name)."""

    old_value: str | None
    """Previous value (if applicable)."""

    new_value: str | None
    """New value (if applicable)."""

    def __str__(self) -> str:
        """Human-readable description of the change."""
        if self.change_type == "column_removed":
            return f"Column '{self.element}' removed"
        elif self.change_type == "band_removed":
            return f"Band '{self.element}' removed"
        elif self.change_type == "type_changed":
            return f"Column '{self.element}' type changed: {self.old_value} -> {self.new_value}"
        elif self.change_type == "data_type_changed":
            return f"Band '{self.element}' data_type changed: {self.old_value} -> {self.new_value}"
        elif self.change_type == "geometry_type_changed":
            return f"Column '{self.element}' geometry_type changed: {self.old_value} -> {self.new_value}"
        elif self.change_type == "crs_changed":
            return f"CRS changed: {self.old_value} -> {self.new_value}"
        elif self.change_type == "column_crs_changed":
            return f"Column '{self.element}' CRS changed: {self.old_value} -> {self.new_value}"
        elif self.change_type == "nodata_changed":
            return f"Band '{self.element}' nodata changed: {self.old_value} -> {self.new_value}"
        else:
            return f"{self.change_type}: {self.element} ({self.old_value} -> {self.new_value})"


def detect_breaking_changes(
    old_schema: SchemaModel,
    new_schema: SchemaModel,
) -> list[BreakingChange]:
    """Detect breaking changes between two schema versions.

    Args:
        old_schema: Previous schema version.
        new_schema: New schema version.

    Returns:
        List of breaking changes. Empty list means no breaking changes.
    """
    changes: list[BreakingChange] = []

    # Check format mismatch
    if old_schema.format != new_schema.format:
        changes.append(
            BreakingChange(
                change_type="format_changed",
                element="format",
                old_value=old_schema.format,
                new_value=new_schema.format,
            )
        )
        return changes  # Can't compare further if formats differ

    # Check schema-level CRS
    if old_schema.crs != new_schema.crs:
        changes.append(
            BreakingChange(
                change_type="crs_changed",
                element="schema",
                old_value=old_schema.crs,
                new_value=new_schema.crs,
            )
        )

    if old_schema.format == "geoparquet":
        changes.extend(_detect_geoparquet_changes(old_schema, new_schema))
    elif old_schema.format == "cog":
        changes.extend(_detect_cog_changes(old_schema, new_schema))

    return changes


def _detect_geoparquet_changes(
    old_schema: SchemaModel,
    new_schema: SchemaModel,
) -> list[BreakingChange]:
    """Detect breaking changes in GeoParquet schemas."""
    changes: list[BreakingChange] = []

    # Build lookup by name
    old_cols = _columns_by_name(old_schema)
    new_cols = _columns_by_name(new_schema)

    # Check for removed columns
    for name in old_cols:
        if name not in new_cols:
            changes.append(
                BreakingChange(
                    change_type="column_removed",
                    element=name,
                    old_value=None,
                    new_value=None,
                )
            )
            continue

        old_col = old_cols[name]
        new_col = new_cols[name]

        # Check type changes
        old_type = (
            old_col.get("type") if isinstance(old_col, dict) else getattr(old_col, "type", None)
        )
        new_type = (
            new_col.get("type") if isinstance(new_col, dict) else getattr(new_col, "type", None)
        )
        if old_type != new_type:
            changes.append(
                BreakingChange(
                    change_type="type_changed",
                    element=name,
                    old_value=str(old_type),
                    new_value=str(new_type),
                )
            )

        # Check geometry type changes
        old_geom = (
            old_col.get("geometry_type")
            if isinstance(old_col, dict)
            else getattr(old_col, "geometry_type", None)
        )
        new_geom = (
            new_col.get("geometry_type")
            if isinstance(new_col, dict)
            else getattr(new_col, "geometry_type", None)
        )
        if old_geom != new_geom and old_geom is not None:
            changes.append(
                BreakingChange(
                    change_type="geometry_type_changed",
                    element=name,
                    old_value=str(old_geom),
                    new_value=str(new_geom),
                )
            )

        # Check column-level CRS changes
        old_crs = old_col.get("crs") if isinstance(old_col, dict) else getattr(old_col, "crs", None)
        new_crs = new_col.get("crs") if isinstance(new_col, dict) else getattr(new_col, "crs", None)
        if old_crs != new_crs and old_crs is not None:
            changes.append(
                BreakingChange(
                    change_type="column_crs_changed",
                    element=name,
                    old_value=str(old_crs),
                    new_value=str(new_crs),
                )
            )

    return changes


def _detect_cog_changes(
    old_schema: SchemaModel,
    new_schema: SchemaModel,
) -> list[BreakingChange]:
    """Detect breaking changes in COG schemas."""
    changes: list[BreakingChange] = []

    # Build lookup by name
    old_bands = _columns_by_name(old_schema)
    new_bands = _columns_by_name(new_schema)

    # Check for removed bands
    for name in old_bands:
        if name not in new_bands:
            changes.append(
                BreakingChange(
                    change_type="band_removed",
                    element=name,
                    old_value=None,
                    new_value=None,
                )
            )
            continue

        old_band = old_bands[name]
        new_band = new_bands[name]

        # Check data_type changes
        old_dtype = (
            old_band.get("data_type")
            if isinstance(old_band, dict)
            else getattr(old_band, "data_type", None)
        )
        new_dtype = (
            new_band.get("data_type")
            if isinstance(new_band, dict)
            else getattr(new_band, "data_type", None)
        )
        if old_dtype != new_dtype:
            changes.append(
                BreakingChange(
                    change_type="data_type_changed",
                    element=name,
                    old_value=str(old_dtype),
                    new_value=str(new_dtype),
                )
            )

        # Check nodata changes
        old_nodata = (
            old_band.get("nodata")
            if isinstance(old_band, dict)
            else getattr(old_band, "nodata", None)
        )
        new_nodata = (
            new_band.get("nodata")
            if isinstance(new_band, dict)
            else getattr(new_band, "nodata", None)
        )
        if old_nodata != new_nodata:
            changes.append(
                BreakingChange(
                    change_type="nodata_changed",
                    element=name,
                    old_value=str(old_nodata) if old_nodata is not None else None,
                    new_value=str(new_nodata) if new_nodata is not None else None,
                )
            )

    return changes


def _columns_by_name(
    schema: SchemaModel,
) -> dict[str, ColumnSchema | BandSchema | dict[str, Any]]:
    """Build a lookup dict of columns/bands by name."""
    result: dict[str, ColumnSchema | BandSchema | dict[str, Any]] = {}
    for col in schema.columns:
        if isinstance(col, (ColumnSchema, BandSchema)):
            result[col.name] = col
        elif isinstance(col, dict) and "name" in col:
            result[col["name"]] = col
    return result


def is_breaking(old_schema: SchemaModel, new_schema: SchemaModel) -> bool:
    """Check if schema change is breaking.

    Args:
        old_schema: Previous schema version.
        new_schema: New schema version.

    Returns:
        True if any breaking changes detected.
    """
    return len(detect_breaking_changes(old_schema, new_schema)) > 0
