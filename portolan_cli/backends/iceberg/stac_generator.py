"""STAC metadata generation from Iceberg table state."""

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

from portolan_cli.api import sanitize_published_string

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
    # The logical type only. The Iceberg schema keeps the CRS in its own type string.
    GeometryType: "geometry",
    GeographyType: "geography",
}

STAC_TABLE_EXTENSION = "https://stac-extensions.github.io/table/v1.2.0/schema.json"
# The Iceberg extension that portolan-spec pins under incubating/iceberg.
STAC_ICEBERG_EXTENSION = "https://schemas.portolan-sdi.org/incubating/iceberg/v1.1.0/schema.json"

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
    """Return the total row count via O(1) snapshot metadata."""
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
    """Generate STAC Table Extension fields from an Iceberg table."""
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
    """Extract the catalog type string from a table's catalog reference."""
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
    """Extract the partition spec in the shape the extension defines."""
    spec = table.spec()
    return [
        {
            "name": field.name,
            "transform": str(field.transform),
            "source-id": field.source_id,
            "field-id": field.field_id,
        }
        for field in spec.fields
    ]


def generate_collection_metadata(table: Table) -> dict[str, Any]:
    """Generate combined STAC metadata from an Iceberg table."""
    # Layer 1: table:* fields
    metadata = generate_table_metadata(table)

    # Layer 2: iceberg:* fields
    metadata["iceberg:catalog_type"] = _get_catalog_type(table)
    metadata["iceberg:table_id"] = _get_table_id(table)
    metadata["iceberg:format_version"] = table.format_version
    # The field describes a partition spec. The extension says to omit it for
    # an unpartitioned table, so an empty list says nothing and is left out.
    partition_spec = _get_partition_spec(table)
    if partition_spec:
        metadata["iceberg:partition_spec"] = partition_spec

    # Omit a local file:// or sqlite:// location: it leaks the machine layout. Drop URI credentials
    # too.
    catalog_uri = _get_catalog_uri(table)
    published_uri = sanitize_published_string(catalog_uri) if catalog_uri else None
    if published_uri:
        metadata["iceberg:catalog_uri"] = published_uri

    location = table.metadata_location
    published_location = sanitize_published_string(location) if location else None
    if published_location:
        metadata["iceberg:metadata_location"] = published_location

    # A string, because a 64-bit snapshot id loses precision as a JSON double.
    snap = table.current_snapshot()
    if snap is not None:
        metadata["iceberg:current_snapshot_id"] = str(snap.snapshot_id)

    # pystac ignores extra_fields for assets and stac_extensions. on_post_add sets them.

    return metadata
