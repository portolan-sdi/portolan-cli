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
    IntegerType,
    LongType,
    StringType,
    TimestampType,
    TimestamptzType,
)

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
}

STAC_TABLE_EXTENSION = "https://stac-extensions.github.io/table/v1.2.0/schema.json"
STAC_ICEBERG_EXTENSION = "https://portolan-sdi.github.io/stac-iceberg-extension/v1.0.0/schema.json"

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

    row_count = len(table.scan().to_arrow())
    primary_geometry = _detect_primary_geometry(field_names)

    return {
        "table:columns": columns,
        "table:row_count": row_count,
        "table:primary_geometry": primary_geometry,
    }


def _get_catalog_type(table: Table) -> str:
    """Extract the catalog type string from a table's catalog reference."""
    catalog = table.catalog
    class_name = type(catalog).__name__
    if class_name in _CATALOG_TYPE_MAP:
        return _CATALOG_TYPE_MAP[class_name]
    return str(catalog.properties.get("type", "unknown"))


def _get_catalog_uri(table: Table) -> str | None:
    """Extract the catalog URI from a table's catalog reference."""
    catalog = table.catalog
    uri = catalog.properties.get("uri")
    return str(uri) if uri is not None else None


def _get_table_id(table: Table) -> str:
    """Get the fully qualified table identifier (namespace.name)."""
    name_tuple = table.name()
    return ".".join(name_tuple)


def _get_partition_spec(table: Table) -> list[dict[str, str]]:
    """Extract partition spec as a list of field descriptors."""
    spec = table.spec()
    result = []
    for field in spec.fields:
        result.append(
            {
                "field": table.schema().find_field(field.source_id).name,
                "transform": str(field.transform),
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

    snap = table.current_snapshot()
    metadata["iceberg:current_snapshot_id"] = snap.snapshot_id if snap else None

    # Assets and stac_extensions are NOT included here — they must be set
    # via pystac's first-class APIs (collection.assets, collection.stac_extensions)
    # because pystac ignores extra_fields for these during serialization.
    # See on_post_add() in backend.py for how they are applied.

    return metadata
