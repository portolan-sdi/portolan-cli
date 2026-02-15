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

import math
from dataclasses import dataclass
from typing import Any

from portolan_cli.models.schema import BandSchema, ColumnSchema, SchemaModel


def _nodata_equals(a: float | int | None, b: float | int | None) -> bool:
    """Compare nodata values, handling NaN correctly.

    In Python, float('nan') != float('nan') is always True.
    This function treats two NaN values as equal.
    """
    if a is None and b is None:
        return True
    if a is None or b is None:
        return False
    if isinstance(a, float) and isinstance(b, float):
        if math.isnan(a) and math.isnan(b):
            return True
    return a == b


# Message templates for breaking change types
_CHANGE_MESSAGES: dict[str, str] = {
    "format_changed": "Format changed: {old} -> {new}",
    "column_removed": "Column '{element}' removed",
    "band_removed": "Band '{element}' removed",
    "type_changed": "Column '{element}' type changed: {old} -> {new}",
    "data_type_changed": "Band '{element}' data_type changed: {old} -> {new}",
    "geometry_type_changed": "Column '{element}' geometry_type changed: {old} -> {new}",
    "crs_changed": "CRS changed: {old} -> {new}",
    "column_crs_changed": "Column '{element}' CRS changed: {old} -> {new}",
    "nodata_changed": "Band '{element}' nodata changed: {old} -> {new}",
    "nullable_changed": "Column '{element}' nullable changed: {old} -> {new}",
}


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
        template = _CHANGE_MESSAGES.get(self.change_type)
        if template:
            return template.format(
                element=self.element,
                old=self.old_value,
                new=self.new_value,
            )
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


def _get_col_attr(
    col: ColumnSchema | BandSchema | dict[str, Any],
    attr: str,
) -> Any:
    """Get attribute from column, supporting both dict and dataclass."""
    if isinstance(col, dict):
        return col.get(attr)
    return getattr(col, attr, None)


def _check_column_changes(
    name: str,
    old_col: ColumnSchema | BandSchema | dict[str, Any],
    new_col: ColumnSchema | BandSchema | dict[str, Any],
) -> list[BreakingChange]:
    """Check for breaking changes between two columns."""
    changes: list[BreakingChange] = []

    # Check type changes
    old_type = _get_col_attr(old_col, "type")
    new_type = _get_col_attr(new_col, "type")
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
    old_geom = _get_col_attr(old_col, "geometry_type")
    new_geom = _get_col_attr(new_col, "geometry_type")
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
    old_crs = _get_col_attr(old_col, "crs")
    new_crs = _get_col_attr(new_col, "crs")
    if old_crs != new_crs and old_crs is not None:
        changes.append(
            BreakingChange(
                change_type="column_crs_changed",
                element=name,
                old_value=str(old_crs),
                new_value=str(new_crs),
            )
        )

    # Check nullable changes (True -> False is breaking)
    old_nullable = _get_col_attr(old_col, "nullable")
    new_nullable = _get_col_attr(new_col, "nullable")
    if old_nullable is True and new_nullable is False:
        changes.append(
            BreakingChange(
                change_type="nullable_changed",
                element=name,
                old_value="true",
                new_value="false",
            )
        )

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

    # Check for removed columns and changes
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

        changes.extend(_check_column_changes(name, old_cols[name], new_cols[name]))

    return changes


def _check_band_changes(
    name: str,
    old_band: ColumnSchema | BandSchema | dict[str, Any],
    new_band: ColumnSchema | BandSchema | dict[str, Any],
) -> list[BreakingChange]:
    """Check for breaking changes between two bands."""
    changes: list[BreakingChange] = []

    # Check data_type changes
    old_dtype = _get_col_attr(old_band, "data_type")
    new_dtype = _get_col_attr(new_band, "data_type")
    if old_dtype != new_dtype:
        changes.append(
            BreakingChange(
                change_type="data_type_changed",
                element=name,
                old_value=str(old_dtype),
                new_value=str(new_dtype),
            )
        )

    # Check nodata changes (handles NaN correctly)
    old_nodata = _get_col_attr(old_band, "nodata")
    new_nodata = _get_col_attr(new_band, "nodata")
    if not _nodata_equals(old_nodata, new_nodata):
        changes.append(
            BreakingChange(
                change_type="nodata_changed",
                element=name,
                old_value=str(old_nodata) if old_nodata is not None else None,
                new_value=str(new_nodata) if new_nodata is not None else None,
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

    # Check for removed bands and changes
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

        changes.extend(_check_band_changes(name, old_bands[name], new_bands[name]))

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
