"""Read the public proof catalog with no credential, as a reader of a published catalog does.

The maintainer built the catalog ``portolan_cli_tests`` with ``portolan init
--backend iceberg`` and ``portolan add`` (PR #1102), then made it public. It
holds two tables in the namespace ``portolake``:

- ``housing``: 501 rows of a CRS84 GeoParquet file, 25 null geometries,
  versions 1.0.0 and 1.1.0, ``main`` rolled back to 1.0.0.
- ``utm``: 20 rows of an EPSG:25830 GeoParquet file, version 1.0.0.

The hosted catalog runs maintenance on its tables. These tests check that
the version record, the typed geometry, and the published STAC fields stay
readable after it. No test writes, so no secret is needed. A failure of the
STAC test means the published ``collection.json`` names a metadata file that
no longer answers. Run ``portolan version refresh`` and upload it again.
"""

from __future__ import annotations

from typing import Any

import duckdb
import httpx
import pytest

from portolan_cli.backends.iceberg.auth import NoAuth
from portolan_cli.backends.iceberg.rest import LoadTableResult, RestCatalog
from portolan_cli.backends.iceberg.settings import IcebergSettings, StorageSettings
from portolan_cli.backends.iceberg.stac_generator import generate_collection_metadata
from portolan_cli.backends.iceberg.versioning import (
    current_version_from_metadata,
    versions_from_metadata,
)

pytestmark = [pytest.mark.network, pytest.mark.lakehousebox]

PUBLIC_ENDPOINT = "https://public-catalog.lakehousebox.com"
WAREHOUSE = "carto-sdi/portolan_cli_tests"
NAMESPACE = "portolake"
S3_ENDPOINT = "s3.lakehousebox.com"
STAC_ROOT = "https://s3.lakehousebox.com/carto-sdi--portolan-cli-tests--blobs/portolan_cli_tests"


def _settings() -> IcebergSettings:
    """The settings of an anonymous reader of the public endpoint."""
    return IcebergSettings(
        uri=PUBLIC_ENDPOINT,
        warehouse=WAREHOUSE,
        namespace=NAMESPACE,
        auth="none",
        oauth2_server_uri=None,
        client_id=None,
        client_secret=None,
        token=None,
        storage=StorageSettings(
            kind="s3",
            endpoint=S3_ENDPOINT,
            use_ssl=True,
            region="us-east-1",
            url_style="path",
            key_id=None,
            secret=None,
            session_token=None,
        ),
    )


@pytest.fixture(scope="module")
def catalog() -> RestCatalog:
    return RestCatalog(PUBLIC_ENDPOINT, auth=NoAuth(), warehouse=WAREHOUSE)


@pytest.fixture(scope="module")
def tables(catalog: RestCatalog) -> dict[str, LoadTableResult]:
    return {name: catalog.load_table(NAMESPACE, name) for name in ("housing", "utm")}


@pytest.fixture(scope="module")
def reader() -> duckdb.DuckDBPyConnection:
    """A DuckDB connection with the unsigned S3 secret that the Iceberg guide shows."""
    connection = duckdb.connect()
    for extension in ("httpfs", "iceberg", "spatial"):
        connection.execute(f"LOAD {extension}")
    connection.execute(
        f"CREATE SECRET store (TYPE s3, ENDPOINT '{S3_ENDPOINT}', "
        "URL_STYLE 'path', REGION 'us-east-1')"
    )
    return connection


def _geometry_type(loaded: LoadTableResult) -> str:
    metadata = loaded.metadata
    schema = next(s for s in metadata["schemas"] if s["schema-id"] == metadata["current-schema-id"])
    kind: str = next(f["type"] for f in schema["fields"] if f["name"] == "geometry")
    return kind


def test_the_tables_are_format_version_3_with_typed_geometry(
    tables: dict[str, LoadTableResult],
) -> None:
    assert {name: t.metadata["format-version"] for name, t in tables.items()} == {
        "housing": 3,
        "utm": 3,
    }
    assert _geometry_type(tables["housing"]) == "geometry"
    assert _geometry_type(tables["utm"]) == "geometry(EPSG:25830)"


def test_the_version_record_survives_the_maintenance(tables: dict[str, LoadTableResult]) -> None:
    """The tags keep the snapshots of each version, and main stays on the rollback target."""
    housing = tables["housing"].metadata
    current = current_version_from_metadata(housing)

    assert [v.version for v in versions_from_metadata(housing)] == ["1.0.0", "1.1.0"]
    assert current is not None
    assert current.version == "1.0.0"
    assert [v.version for v in versions_from_metadata(tables["utm"].metadata)] == ["1.0.0"]


def test_the_fields_of_an_anonymous_reader(
    catalog: RestCatalog, tables: dict[str, LoadTableResult]
) -> None:
    fields = generate_collection_metadata(
        tables["utm"], settings=_settings(), collection="utm", prefix=catalog.prefix, row_count=20
    )

    assert fields["iceberg:catalog_type"] == "rest"
    assert fields["iceberg:authorization_type"] == "none"
    assert fields["iceberg:rest_prefix"] == "carto-sdi.portolan_cli_tests"
    assert fields["iceberg:format_version"] == 3
    assert fields["iceberg:metadata_location"].startswith(
        "https://s3.lakehousebox.com/carto-sdi--portolan-cli-tests/portolake/utm/metadata/"
    )


def test_duckdb_reads_the_tables_through_the_public_endpoint(
    reader: duckdb.DuckDBPyConnection,
) -> None:
    reader.execute(
        f"ATTACH '{WAREHOUSE}' AS lake "
        f"(TYPE iceberg, ENDPOINT '{PUBLIC_ENDPOINT}', AUTHORIZATION_TYPE 'none')"
    )

    housing = reader.execute(
        "SELECT count(*), count(*) FILTER (WHERE geometry IS NULL) FROM lake.portolake.housing"
    ).fetchone()
    utm = reader.execute(
        "SELECT count(*), typeof(any_value(geometry)) FROM lake.portolake.utm"
    ).fetchone()

    assert housing == (501, 25)
    assert utm == (20, "GEOMETRY('EPSG:25830')")


@pytest.mark.parametrize("collection", ["housing", "utm"])
def test_the_published_metadata_location_answers(
    reader: duckdb.DuckDBPyConnection, collection: str
) -> None:
    """A reader with no catalog opens the table from the published collection."""
    response = httpx.get(f"{STAC_ROOT}/{collection}/collection.json", timeout=60)
    response.raise_for_status()
    document: dict[str, Any] = response.json()
    location = document["iceberg:metadata_location"]

    row = reader.execute(f"SELECT count(*) FROM iceberg_scan('{location}')").fetchone()  # noqa: S608

    assert document["assets"]["iceberg"]["href"] == location
    assert row == (document["table:row_count"],)
