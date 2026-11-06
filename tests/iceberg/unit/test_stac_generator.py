"""The STAC fields of a collection come from the ``loadTable`` document.

``generate_collection_metadata`` reads the table metadata a REST catalog
returns, plus the row count the backend queried, and returns the
``table:*`` and ``iceberg:*`` fields of the STAC Iceberg extension v1.1.0.
No pyiceberg object is involved, so the tests build the document by hand.
"""

from __future__ import annotations

from typing import Any

import pytest

from portolan_cli.backends.iceberg.rest import LoadTableResult
from portolan_cli.backends.iceberg.settings import IcebergSettings, StorageSettings
from portolan_cli.backends.iceberg.stac_generator import (
    STAC_ICEBERG_EXTENSION,
    STAC_TABLE_EXTENSION,
    generate_collection_metadata,
    https_metadata_location,
    iceberg_type_to_str,
)

pytestmark = pytest.mark.unit


def _storage(*, endpoint: str | None = "s3.example.org", use_ssl: bool = True) -> StorageSettings:
    return StorageSettings(
        kind="s3",
        endpoint=endpoint,
        use_ssl=use_ssl,
        region="us-east-1",
        url_style="path" if endpoint else "vhost",
        key_id="k",
        secret="s",  # noqa: S106  # a test value
        session_token=None,
    )


def _settings(
    *, uri: str = "https://catalog.example.org", auth: str = "none", namespace: str = "portolake"
) -> IcebergSettings:
    return IcebergSettings(
        uri=uri,
        warehouse="wh",
        namespace=namespace,
        auth=auth,  # type: ignore[arg-type]
        oauth2_server_uri=None,
        client_id=None,
        client_secret=None,
        token=None,
        storage=_storage(),
    )


def _loaded(
    *,
    fields: list[dict[str, Any]] | None = None,
    location: str = "s3://warehouse/portolake/roads/metadata/00001-abc.metadata.json",
    current_snapshot: int | None = 42,
    specs: list[dict[str, Any]] | None = None,
    default_spec: int = 0,
    format_version: int = 3,
) -> LoadTableResult:
    if fields is None:
        fields = [
            {"id": 1, "name": "id", "required": False, "type": "long"},
            {"id": 2, "name": "name", "required": False, "type": "string"},
            {"id": 3, "name": "geometry", "required": False, "type": "geometry(EPSG:25830)"},
        ]
    metadata: dict[str, Any] = {
        "format-version": format_version,
        "table-uuid": "0a7b",
        "current-schema-id": 1,
        "schemas": [
            {"schema-id": 0, "type": "struct", "fields": [fields[0]]},
            {"schema-id": 1, "type": "struct", "fields": fields},
        ],
        "default-spec-id": default_spec,
        "partition-specs": specs if specs is not None else [{"spec-id": 0, "fields": []}],
        "current-snapshot-id": current_snapshot if current_snapshot is not None else -1,
        "refs": {},
        "snapshots": [],
        "properties": {},
    }
    return LoadTableResult(metadata_location=location, metadata=metadata, config={})


def _generate(loaded: LoadTableResult | None = None, **kwargs: Any) -> dict[str, Any]:
    options: dict[str, Any] = {
        "settings": _settings(),
        "collection": "roads",
        "prefix": "",
        "row_count": 7,
    }
    options.update(kwargs)
    return generate_collection_metadata(loaded or _loaded(), **options)


# --- table:* ---


def test_columns_come_from_the_current_schema_with_logical_types() -> None:
    fields = _generate()["table:columns"]

    assert fields == [
        {"name": "id", "type": "int64"},
        {"name": "name", "type": "string"},
        {"name": "geometry", "type": "geometry"},
    ]


def test_the_current_schema_is_the_one_named_by_current_schema_id() -> None:
    """Schema 0 holds one column and schema 1 three. The document names schema 1."""
    columns = _generate()["table:columns"]

    assert len(columns) == 3


@pytest.mark.parametrize(
    ("iceberg_type", "expected"),
    [
        ("long", "int64"),
        ("int", "int32"),
        ("double", "float64"),
        ("float", "float32"),
        ("string", "string"),
        ("boolean", "boolean"),
        ("binary", "binary"),
        ("date", "date"),
        ("timestamp", "datetime"),
        ("timestamptz", "datetime"),
        ("timestamp_ns", "datetime"),
        ("geometry(OGC:CRS84)", "geometry"),
        ("geography(EPSG:4326)", "geography"),
        ("decimal(10, 2)", "decimal"),
        ({"type": "struct", "fields": []}, "struct"),
        ({"type": "list", "element": "string"}, "list"),
        ({"type": "map", "key": "string", "value": "long"}, "map"),
    ],
)
def test_iceberg_type_strings_map_to_table_extension_types(
    iceberg_type: str | dict[str, Any], expected: str
) -> None:
    assert iceberg_type_to_str(iceberg_type) == expected


def test_row_count_is_the_one_the_backend_queried() -> None:
    """The snapshot summary counts physical rows under merge-on-read, so it is not used."""
    assert _generate(row_count=501)["table:row_count"] == 501


def test_primary_geometry_is_the_first_geometry_typed_column() -> None:
    fields = [
        {"id": 1, "name": "id", "required": False, "type": "long"},
        {"id": 2, "name": "shape", "required": False, "type": "geometry(EPSG:4326)"},
        {"id": 3, "name": "geometry", "required": False, "type": "binary"},
    ]

    assert _generate(_loaded(fields=fields))["table:primary_geometry"] == "shape"


def test_primary_geometry_is_none_without_a_geometry_column() -> None:
    fields = [{"id": 1, "name": "id", "required": False, "type": "long"}]

    assert _generate(_loaded(fields=fields))["table:primary_geometry"] is None


# --- iceberg:* ---


def test_the_catalog_fields_describe_a_rest_catalog() -> None:
    metadata = _generate(prefix="ws-one")

    assert metadata["iceberg:catalog_type"] == "rest"
    assert metadata["iceberg:catalog_uri"] == "https://catalog.example.org"
    assert metadata["iceberg:rest_prefix"] == "ws-one"
    assert metadata["iceberg:authorization_type"] == "none"
    assert metadata["iceberg:table_id"] == "portolake.roads"
    assert metadata["iceberg:format_version"] == 3


def test_an_empty_prefix_is_omitted() -> None:
    assert "iceberg:rest_prefix" not in _generate(prefix="")


@pytest.mark.parametrize(
    ("auth", "published"), [("none", "none"), ("oauth2", "oauth2"), ("bearer", "oauth2")]
)
def test_the_authorization_type_is_in_the_extension_enum(auth: str, published: str) -> None:
    """The enum holds none, oauth2, and sigv4. A static bearer token is an OAuth2 token."""
    assert _generate(settings=_settings(auth=auth))["iceberg:authorization_type"] == published


def test_a_credential_in_the_catalog_uri_is_dropped() -> None:
    settings = _settings(uri="https://catalog.example.org?token=SECRET")

    assert _generate(settings=settings)["iceberg:catalog_uri"] == "https://catalog.example.org"


def test_the_table_id_uses_the_configured_namespace() -> None:
    assert _generate(settings=_settings(namespace="lake"))["iceberg:table_id"] == "lake.roads"


def test_the_partition_spec_is_the_default_spec_in_extension_shape() -> None:
    specs = [
        {"spec-id": 0, "fields": []},
        {
            "spec-id": 1,
            "fields": [
                {"name": "region", "transform": "identity", "source-id": 2, "field-id": 1000}
            ],
        },
    ]

    metadata = _generate(_loaded(specs=specs, default_spec=1))

    assert metadata["iceberg:partition_spec"] == [
        {"name": "region", "transform": "identity", "source-id": 2, "field-id": 1000}
    ]


def test_an_unpartitioned_table_omits_the_partition_spec() -> None:
    assert "iceberg:partition_spec" not in _generate()


def test_the_snapshot_id_is_a_string() -> None:
    """A 64-bit id loses precision in a JSON parser that stores numbers as doubles."""
    assert _generate(_loaded(current_snapshot=9007199254740993))["iceberg:current_snapshot_id"] == (
        "9007199254740993"
    )


def test_a_table_with_no_snapshot_omits_the_snapshot_id() -> None:
    assert "iceberg:current_snapshot_id" not in _generate(_loaded(current_snapshot=None))


# --- metadata location ---


def test_an_s3_location_becomes_https_on_a_path_style_endpoint() -> None:
    location = "s3://warehouse/portolake/roads/metadata/00001-abc.metadata.json"

    assert https_metadata_location(location, _storage()) == (
        "https://s3.example.org/warehouse/portolake/roads/metadata/00001-abc.metadata.json"
    )


def test_an_s3_location_stays_s3_without_an_https_endpoint() -> None:
    location = "s3://warehouse/t/metadata/v1.metadata.json"

    assert https_metadata_location(
        location, _storage(endpoint="localhost:9000", use_ssl=False)
    ) == (location)
    assert https_metadata_location(location, _storage(endpoint=None)) == location


def test_the_published_location_is_the_https_form() -> None:
    metadata = _generate()

    assert metadata["iceberg:metadata_location"] == (
        "https://s3.example.org/warehouse/portolake/roads/metadata/00001-abc.metadata.json"
    )


def test_a_local_location_is_omitted() -> None:
    """#939: a file path names the machine that ran the command."""
    loaded = _loaded(location="file:///tmp/c/.portolan/warehouse/t/metadata/v1.metadata.json")

    assert "iceberg:metadata_location" not in _generate(loaded)


# --- extensions ---


def test_the_extension_uris_are_the_canonical_ones() -> None:
    assert STAC_TABLE_EXTENSION == "https://stac-extensions.github.io/table/v1.2.0/schema.json"
    assert STAC_ICEBERG_EXTENSION == (
        "https://schemas.portolan-sdi.org/incubating/iceberg/v1.1.0/schema.json"
    )


def test_the_fields_hold_neither_assets_nor_extensions() -> None:
    """Assets and stac_extensions go through the collection document, not these fields."""
    metadata = _generate()

    assert "assets" not in metadata
    assert "stac_extensions" not in metadata
