"""The STAC fields of a collection, from the ``loadTable`` document.

Layer 1 is the STAC Table extension: ``table:columns``, ``table:row_count``,
``table:primary_geometry``. Layer 2 is the STAC Iceberg extension v1.1.0:
``iceberg:catalog_type``, ``iceberg:catalog_uri``, ``iceberg:rest_prefix``,
``iceberg:authorization_type``, ``iceberg:table_id``,
``iceberg:format_version``, ``iceberg:partition_spec``,
``iceberg:metadata_location``, ``iceberg:current_snapshot_id``.

Everything comes from the table metadata document the REST catalog returns
and from the backend settings. No table object of any library is involved.
The row count is an argument, because the backend queries it: under
DuckDB's merge-on-read writes the snapshot summary's ``total-records``
counts physical rows, including the rows a deletion vector hides, so it is
not the row count a reader sees (duckdb/duckdb-iceberg#1659).
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Any

from portolan_cli.api import sanitize_published_string

if TYPE_CHECKING:
    from portolan_cli.backends.iceberg.rest import LoadTableResult
    from portolan_cli.backends.iceberg.settings import IcebergSettings, StorageSettings

STAC_TABLE_EXTENSION = "https://stac-extensions.github.io/table/v1.2.0/schema.json"
#: The Iceberg extension a collection published as an Iceberg table declares.
#: portolan-spec pins it under incubating/iceberg, next to the partition
#: extension, and serves it from the same host.
STAC_ICEBERG_EXTENSION = "https://schemas.portolan-sdi.org/incubating/iceberg/v1.1.0/schema.json"

# Iceberg primitive type names to STAC Table extension type names. A
# parameterized type (``decimal(10, 2)``, ``geometry(EPSG:25830)``) maps by
# the name before the parenthesis. The CRS is a field of its own in the
# Iceberg schema and does not belong in the type string.
_TYPE_MAP: dict[str, str] = {
    "long": "int64",
    "int": "int32",
    "double": "float64",
    "float": "float32",
    "string": "string",
    "boolean": "boolean",
    "binary": "binary",
    "date": "date",
    "timestamp": "datetime",
    "timestamptz": "datetime",
    "timestamp_ns": "datetime",
    "timestamptz_ns": "datetime",
    "geometry": "geometry",
    "geography": "geography",
    "decimal": "decimal",
    "uuid": "uuid",
}

# The extension enumerates none, oauth2, and sigv4. A static bearer token is
# an OAuth2 access token an identity provider minted, so a reader uses the
# oauth2 mode with it.
_AUTHORIZATION: dict[str, str] = {"none": "none", "oauth2": "oauth2", "bearer": "oauth2"}


def iceberg_type_to_str(iceberg_type: str | dict[str, Any]) -> str:
    """Return the STAC Table extension type name of an Iceberg schema type."""
    if isinstance(iceberg_type, dict):
        return str(iceberg_type.get("type", "struct"))
    name = iceberg_type.split("(", 1)[0].split("[", 1)[0].strip()
    return _TYPE_MAP.get(name, iceberg_type)


def _is_geometry_type(iceberg_type: str | dict[str, Any]) -> bool:
    return iceberg_type_to_str(iceberg_type) in ("geometry", "geography")


def _current_schema_fields(metadata: dict[str, Any]) -> list[dict[str, Any]]:
    schemas = metadata.get("schemas") or []
    current_id = metadata.get("current-schema-id")
    for schema in schemas:
        if schema.get("schema-id") == current_id:
            return list(schema.get("fields", []))
    if schemas:
        return list(schemas[-1].get("fields", []))
    return list((metadata.get("schema") or {}).get("fields", []))


def _default_partition_fields(metadata: dict[str, Any]) -> list[dict[str, Any]]:
    """Return the fields of the default spec in the shape the extension defines."""
    default_id = metadata.get("default-spec-id", 0)
    for spec in metadata.get("partition-specs") or []:
        if spec.get("spec-id") == default_id:
            return [
                {
                    "name": field["name"],
                    "transform": field["transform"],
                    "source-id": field["source-id"],
                    "field-id": field["field-id"],
                }
                for field in spec.get("fields", [])
            ]
    return []


def https_metadata_location(location: str, storage: StorageSettings | None) -> str:
    """Return the HTTPS form of an ``s3://`` location when the store serves one.

    A path-style endpoint over TLS serves ``s3://bucket/key`` at
    ``https://<endpoint>/bucket/key``, and a reader with no S3 client reads
    the metadata file from that URL. Any other location is returned as is.
    """
    if storage is None or storage.endpoint is None or not storage.use_ssl:
        return location
    if not location.startswith("s3://"):
        return location
    return f"https://{storage.endpoint}/{location[len('s3://') :]}"


def generate_table_metadata(loaded: LoadTableResult, *, row_count: int) -> dict[str, Any]:
    """Return the ``table:*`` fields of the table."""
    fields = _current_schema_fields(loaded.metadata)
    primary_geometry = next(
        (str(field["name"]) for field in fields if _is_geometry_type(field["type"])), None
    )
    return {
        "table:columns": [
            {"name": str(field["name"]), "type": iceberg_type_to_str(field["type"])}
            for field in fields
        ],
        "table:row_count": row_count,
        "table:primary_geometry": primary_geometry,
    }


def generate_collection_metadata(
    loaded: LoadTableResult,
    *,
    settings: IcebergSettings,
    collection: str,
    prefix: str,
    row_count: int,
) -> dict[str, Any]:
    """Return the ``table:*`` and ``iceberg:*`` fields of a collection.

    Args:
        loaded: The ``loadTable`` response of the collection's table.
        settings: The backend settings, for the catalog URI, the namespace,
            the authorization mode, and the object store.
        collection: The collection id, which is the table name.
        prefix: The REST prefix ``GET /v1/config`` returned, or ``""``.
        row_count: The row count the backend queried.

    Returns:
        The fields, ready to merge into ``collection.json``. Assets and
        ``stac_extensions`` are not among them: the backend sets those on
        the document itself.
    """
    metadata = generate_table_metadata(loaded, row_count=row_count)
    document = loaded.metadata

    metadata["iceberg:catalog_type"] = "rest"
    # A credential in the URI query is dropped. A local URI has no safe form.
    catalog_uri = sanitize_published_string(settings.uri)
    if catalog_uri:
        metadata["iceberg:catalog_uri"] = catalog_uri
    if prefix:
        metadata["iceberg:rest_prefix"] = prefix
    metadata["iceberg:authorization_type"] = _AUTHORIZATION[settings.auth]
    metadata["iceberg:table_id"] = f"{settings.namespace}.{collection}"
    metadata["iceberg:format_version"] = int(document.get("format-version", 0))

    # The extension says to omit the field for an unpartitioned table.
    partition_spec = _default_partition_fields(document)
    if partition_spec:
        metadata["iceberg:partition_spec"] = partition_spec

    # A file:// location is the absolute path of the machine that ran the
    # command (#939), so it is left out. An s3:// location becomes https when
    # the store serves it that way, which is what a serverless reader needs.
    published_location = sanitize_published_string(
        https_metadata_location(loaded.metadata_location, settings.storage)
    )
    if published_location:
        metadata["iceberg:metadata_location"] = published_location

    # A string: an Iceberg snapshot id is 64-bit, and a JSON parser that
    # stores numbers as doubles rounds it above 2^53. A table with no
    # snapshot has no id, and the field is optional, so it is omitted.
    current = document.get("current-snapshot-id")
    if current is not None and int(current) != -1:
        metadata["iceberg:current_snapshot_id"] = str(int(current))

    return metadata
