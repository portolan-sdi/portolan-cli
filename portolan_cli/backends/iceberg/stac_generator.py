"""STAC metadata generation from Iceberg table state.

Layer 1 (Phase 3): STAC Table Extension fields (table:columns, table:row_count,
table:primary_geometry) extracted from Iceberg table schema and manifests.

Layer 2 (Phase 4): STAC Iceberg Extension fields (iceberg:catalog_type,
iceberg:table_id, iceberg:current_snapshot_id, etc.) from catalog and table state.
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Any

from pyiceberg.types import (
    BinaryType,
    BooleanType,
    DateType,
    DoubleType,
    FloatType,
    GeographyType,
    GeometryType,
    IntegerType,
    LongType,
    StringType,
    TimestampType,
    TimestamptzType,
)

from portolan_cli.constants import ICEBERG_EXTENSION_URI

if TYPE_CHECKING:
    from pyiceberg.table import Table

# Columns added by portolake spatial processing — exclude from table:columns
_DERIVED_PREFIXES = ("geohash_", "bbox_")

# Map Iceberg types to STAC Table Extension type strings
_TYPE_MAP: dict[type, str] = {
    LongType: "int64",
    IntegerType: "int32",
    DoubleType: "float64",
    FloatType: "float32",
    StringType: "string",
    BooleanType: "boolean",
    BinaryType: "binary",
    DateType: "date",
    TimestampType: "datetime",
    TimestamptzType: "datetime",
    # The logical type, with no parameter. The Iceberg schema carries the CRS
    # in its own type string, as geometry(EPSG:4326); the two are different
    # fields and neither belongs in the other.
    GeometryType: "geometry",
    GeographyType: "geography",
}

STAC_TABLE_EXTENSION = "https://stac-extensions.github.io/table/v1.2.0/schema.json"
STAC_ICEBERG_EXTENSION = ICEBERG_EXTENSION_URI

# Map PyIceberg catalog class names to catalog type strings
_CATALOG_TYPE_MAP: dict[str, str] = {
    "SqlCatalog": "sql",
    "RestCatalog": "rest",
    "GlueCatalog": "glue",
    "HiveCatalog": "hive",
    "DynamoDbCatalog": "dynamodb",
}


def _iceberg_type_to_str(iceberg_type: Any) -> str:
    """Convert an Iceberg type to a STAC-friendly type string."""
    return _TYPE_MAP.get(type(iceberg_type), str(iceberg_type))


def _is_derived_column(name: str) -> bool:
    """Check if a column name is a portolake-derived spatial column."""
    return any(name.startswith(prefix) for prefix in _DERIVED_PREFIXES)


def _detect_primary_geometry(field_names: list[str]) -> str | None:
    """Detect the primary geometry column name."""
    for name in ("geometry", "geom"):
        if name in field_names:
            return name
    return None


def _get_row_count(table: Table) -> int:
    """Return the total row count via O(1) snapshot metadata.

    Iceberg records ``total-records`` (net of deletes) in each snapshot's
    summary, so we read that instead of materializing the whole table — the
    data the Iceberg backend targets can be 100K+ rows, and this runs on
    every ``on_post_add``. Returns 0 for an empty table (no current snapshot),
    and falls back to a metadata-only file count for the rare catalog/writer
    that doesn't populate ``total-records``.
    """
    snapshot = table.current_snapshot()
    if snapshot is None:
        return 0
    if snapshot.summary is not None:
        total_records = snapshot.summary.additional_properties.get("total-records")
        if total_records is not None:
            return int(total_records)
    # Falls back to file record_count metadata; only materializes data files
    # that carry unmerged positional deletes (see DataScan.count).
    return int(table.scan().count())


def generate_table_metadata(table: Table) -> dict[str, Any]:
    # portolan-cli also generates table:* from GeoParquet source metadata.
    # This version reflects the Iceberg table state and takes precedence
    # when the Iceberg backend is active (applied via on_post_add hook).
    """Generate STAC Table Extension fields from an Iceberg table.

    Returns a dict with:
        - table:columns: list of {name, type} dicts
        - table:row_count: total rows in current snapshot
        - table:primary_geometry: geometry column name or None
    """
    schema = table.schema()

    columns = []
    field_names = []
    for field in schema.fields:
        field_names.append(field.name)
        if _is_derived_column(field.name):
            continue
        columns.append(
            {
                "name": field.name,
                "type": _iceberg_type_to_str(field.field_type),
            }
        )

    row_count = _get_row_count(table)
    primary_geometry = _detect_primary_geometry(field_names)

    return {
        "table:columns": columns,
        "table:row_count": row_count,
        "table:primary_geometry": primary_geometry,
    }


def _get_catalog_type(table: Table) -> str:
    """Extract the catalog type string from a table's catalog reference.

    The extension enumerates the value, so an unrecognized catalog cannot be
    described. Emitting "unknown" produced a collection that failed the schema
    it declared, and hid the real problem: a catalog class this backend has
    never seen.
    """
    catalog = table.catalog
    class_name = type(catalog).__name__
    if class_name in _CATALOG_TYPE_MAP:
        return _CATALOG_TYPE_MAP[class_name]
    declared = catalog.properties.get("type")
    if declared in _CATALOG_TYPE_MAP.values():
        return str(declared)
    raise ValueError(
        f"Unrecognized Iceberg catalog {class_name!r} (type property: {declared!r}). "
        f"The STAC Iceberg extension allows only {sorted(set(_CATALOG_TYPE_MAP.values()))}."
    )


def _get_catalog_uri(table: Table) -> str | None:
    """Extract the catalog URI from a table's catalog reference."""
    catalog = table.catalog
    uri = catalog.properties.get("uri")
    return str(uri) if uri is not None else None


def _get_table_id(table: Table) -> str:
    """Get the fully qualified table identifier (namespace.name)."""
    name_tuple = table.name()
    return ".".join(name_tuple)


def _get_partition_spec(table: Table) -> list[dict[str, Any]]:
    """Extract the partition spec in the shape the extension defines.

    The entry is keyed ``name``/``transform``/``source-id``/``field-id``. The
    partition field name can differ from the source column name, and the
    earlier ``field`` key carried the source column, so a renamed field or a
    parameterized transform such as ``bucket[16]`` could not be described.
    """
    spec = table.spec()
    result: list[dict[str, Any]] = []
    for field in spec.fields:
        result.append(
            {
                "name": field.name,
                "transform": str(field.transform),
                "source-id": field.source_id,
                "field-id": field.field_id,
            }
        )
    return result


def generate_collection_metadata(table: Table) -> dict[str, Any]:
    """Generate combined STAC metadata from an Iceberg table.

    Combines Layer 1 (table:*) and Layer 2 (iceberg:*) fields.

    Returns a dict suitable for merging into pystac.Collection extra_fields.
    """
    # Layer 1: table:* fields
    metadata = generate_table_metadata(table)

    # Layer 2: iceberg:* fields
    metadata["iceberg:catalog_type"] = _get_catalog_type(table)
    metadata["iceberg:table_id"] = _get_table_id(table)
    metadata["iceberg:format_version"] = table.format_version
    metadata["iceberg:partition_spec"] = _get_partition_spec(table)

    catalog_uri = _get_catalog_uri(table)
    if catalog_uri:
        metadata["iceberg:catalog_uri"] = catalog_uri

    metadata["iceberg:metadata_location"] = table.metadata_location

    # A string: an Iceberg snapshot id is 64-bit, and a JSON parser that stores
    # numbers as doubles rounds it above 2^53. A table with no snapshot has no
    # id to report, and the field is optional, so it is omitted rather than null.
    snap = table.current_snapshot()
    if snap is not None:
        metadata["iceberg:current_snapshot_id"] = str(snap.snapshot_id)

    # Assets and stac_extensions are NOT included here — they must be set
    # via pystac's first-class APIs (collection.assets, collection.stac_extensions)
    # because pystac ignores extra_fields for these during serialization.
    # See on_post_add() in backend.py for how they are applied.

    return metadata
