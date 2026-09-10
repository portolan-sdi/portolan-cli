"""GeoServer provider tests."""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any

import pytest
from click.testing import CliRunner

from portolan_cli.cli import cli
from portolan_cli.server.model import (
    CollectionCandidate,
    PlanAction,
    ResourceFormat,
    ResourceType,
    ServerCatalog,
    ServerResourceSpec,
)
from portolan_cli.server.planner import (
    _fetch_json,
    discover_server_resources,
    load_server_catalog_url,
)
from portolan_cli.server.providers.geoserver.client import GeoServerClient, coverage_store_names
from portolan_cli.server.providers.geoserver.planner import (
    GeoServerProvider,
    geoserver_resource_name,
)
from portolan_cli.server.registry import download_registry_catalog, load_registry_entries

pytestmark = pytest.mark.unit


def _write_json(path: Path, data: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(data, indent=2), encoding="utf-8")


def _base_extent() -> dict[str, Any]:
    return {
        "spatial": {"bbox": [[-71.0, -35.0, -70.0, -34.0]]},
        "temporal": {"interval": [[None, None]]},
    }


def _collection(
    collection_id: str,
    asset: dict[str, Any],
    *,
    title: str | None = None,
) -> dict[str, Any]:
    data: dict[str, Any] = {
        "type": "Collection",
        "stac_version": "1.1.0",
        "id": collection_id,
        "description": f"{collection_id} description",
        "license": "CC-BY-4.0",
        "extent": _base_extent(),
        "links": [],
        "assets": {"data": asset},
        "keywords": ["portolan", collection_id],
    }
    if str(asset.get("href", "")).lower().endswith(".parquet"):
        data["table:primary_geometry"] = "geometry"
    if title:
        data["title"] = title
    return data


def _catalog(root: Path) -> None:
    _write_json(
        root / "catalog.json",
        {
            "type": "Catalog",
            "stac_version": "1.1.0",
            "id": "demo-catalog",
            "description": "Demo catalog",
            "links": [
                {"rel": "child", "href": "./roads/collection.json", "type": "application/json"},
                {"rel": "child", "href": "./elevation/collection.json", "type": "application/json"},
                {"rel": "child", "href": "./basemap/collection.json", "type": "application/json"},
                {"rel": "child", "href": "./notes/collection.json", "type": "application/json"},
            ],
        },
    )
    _write_json(root / ".portolan" / "config.yaml", {})
    _write_json(
        root / "roads" / "collection.json",
        _collection(
            "roads",
            {
                "href": "https://example.test/roads.parquet",
                "type": "application/vnd.apache.parquet",
                "roles": ["data"],
                "file:checksum": "sha256:roads",
            },
            title="Roads",
        ),
    )
    _write_json(
        root / "elevation" / "collection.json",
        _collection(
            "elevation",
            {
                "href": "https://example.test/elevation.tif",
                "type": "image/tiff; application=geotiff; profile=cloud-optimized",
                "roles": ["data"],
            },
        ),
    )
    _write_json(
        root / "basemap" / "collection.json",
        _collection(
            "basemap",
            {
                "href": "https://example.test/basemap.pmtiles",
                "type": "application/vnd.pmtiles",
                "roles": ["data"],
            },
        ),
    )
    _write_json(
        root / "notes" / "collection.json",
        _collection(
            "notes",
            {
                "href": "https://example.test/notes.txt",
                "type": "text/plain",
                "roles": ["data"],
            },
        ),
    )


class RecordingGeoServerClient:
    """Small fake for provider tests."""

    def __init__(self, existing: dict[str, dict[str, Any]] | None = None) -> None:
        self.existing = existing or {}
        self.calls: list[tuple[str, tuple[Any, ...], dict[str, Any]]] = []

    def list_portolan_resources(self, workspace: str) -> dict[str, dict[str, Any]]:
        self.calls.append(("list_portolan_resources", (workspace,), {}))
        return self.existing

    def ensure_workspace(self, workspace: str) -> None:
        self.calls.append(("ensure_workspace", (workspace,), {}))

    def publish_geoparquet(
        self,
        workspace: str,
        name: str,
        href: str,
        metadata: dict[str, object],
        primary_key: str | None = None,
        native_name: str | None = None,
    ) -> None:
        self.calls.append(
            ("publish_geoparquet", (workspace, name, href, metadata, primary_key, native_name), {})
        )

    def publish_cog(
        self, workspace: str, name: str, href: str, metadata: dict[str, object]
    ) -> None:
        self.calls.append(("publish_cog", (workspace, name, href, metadata), {}))


class RaisingRestClient:
    def get(self, path: str) -> Any:
        raise RuntimeError(path)


class RestEndpoints:
    def featuretype(self, workspace: str, datastore: str, name: str) -> str:
        return f"/{workspace}/{datastore}/{name}"


class RestService:
    rest_client = RaisingRestClient()
    rest_endpoints = RestEndpoints()


class BrokenGeoServerApi:
    rest_service = RestService()

    def get_datastores(self, workspace: str) -> tuple[list[dict[str, str]], int]:
        return ([{"name": "broken"}], 200)


def test_geoserver_client_ignores_unreadable_existing_feature_types() -> None:
    client = GeoServerClient.__new__(GeoServerClient)
    client._client = BrokenGeoServerApi()
    client._coverage_stores = lambda workspace: ([], 404)  # type: ignore[method-assign]

    assert client.list_portolan_resources("workspace") == {}


def test_discovers_publishable_collection_assets(tmp_path: Path) -> None:
    _catalog(tmp_path)

    resources = discover_server_resources(tmp_path)

    assert [(r.id, r.resource_type, r.format) for r in resources] == [
        ("roads", ResourceType.VECTOR, ResourceFormat.GEOPARQUET),
        ("elevation", ResourceType.RASTER, ResourceFormat.COG),
        ("basemap", ResourceType.TILES, ResourceFormat.PMTILES),
    ]
    assert resources[0].href == "https://example.test/roads.parquet"
    assert resources[0].metadata["file:checksum"] == "sha256:roads"


def test_skips_non_spatial_parquet_assets(tmp_path: Path) -> None:
    _catalog(tmp_path)
    collection_path = tmp_path / "roads" / "collection.json"
    collection = json.loads(collection_path.read_text(encoding="utf-8"))
    collection.pop("table:primary_geometry")
    collection["table:columns"] = [
        {"name": "_id", "type": "int64"},
        {"name": "name", "type": "string"},
    ]
    _write_json(collection_path, collection)

    provider = GeoServerProvider(client=RecordingGeoServerClient(), workspace="portolan")

    plan = provider.plan(tmp_path)

    assert plan.entries[0].collection == "roads"
    assert plan.entries[0].action == PlanAction.SKIP
    assert plan.entries[0].reason == "non-spatial Parquet asset"


def test_discovers_primary_key_hint_from_table_columns(tmp_path: Path) -> None:
    _catalog(tmp_path)
    collection_path = tmp_path / "roads" / "collection.json"
    collection = json.loads(collection_path.read_text(encoding="utf-8"))
    collection["table:columns"] = [
        {"name": "name", "type": "string"},
        {"name": "objectid", "type": "int64"},
        {"name": "geometry", "type": "binary"},
    ]
    _write_json(collection_path, collection)

    resources = discover_server_resources(tmp_path)

    assert resources[0].primary_key == "objectid"


def test_discovers_uppercase_primary_key_hint_from_table_columns(tmp_path: Path) -> None:
    _catalog(tmp_path)
    collection_path = tmp_path / "roads" / "collection.json"
    collection = json.loads(collection_path.read_text(encoding="utf-8"))
    collection["table:columns"] = [
        {"name": "OGC_FID", "type": "int64"},
        {"name": "geom", "type": "binary"},
    ]
    _write_json(collection_path, collection)

    resources = discover_server_resources(tmp_path)

    assert resources[0].primary_key == "OGC_FID"


def test_geoparquet_native_name_comes_from_asset_href_stem(tmp_path: Path) -> None:
    _catalog(tmp_path)
    collection_path = tmp_path / "roads" / "collection.json"
    collection = json.loads(collection_path.read_text(encoding="utf-8"))
    collection["assets"] = {
        "data": {
            "href": "https://example.test/brazil_car_area_imovel.parquet",
            "type": "application/vnd.apache.parquet",
            "roles": ["data"],
        }
    }
    _write_json(collection_path, collection)

    resources = discover_server_resources(tmp_path)

    assert resources[0].source_asset == "data"
    assert resources[0].native_name == "brazil_car_area_imovel"


def test_geoserver_plan_marks_existing_and_skipped_resources(tmp_path: Path) -> None:
    _catalog(tmp_path)
    client = RecordingGeoServerClient(
        existing={
            "roads": {
                "portolan.asset": "https://example.test/roads.parquet",
                "portolan.checksum": "sha256:roads",
                "portolan.primary_key": "id",
            }
        }
    )
    provider = GeoServerProvider(client=client, workspace="portolan")

    plan = provider.plan(tmp_path)

    assert [(entry.collection, entry.format, entry.action) for entry in plan.entries] == [
        ("roads", ResourceFormat.GEOPARQUET, PlanAction.EXISTS),
        ("elevation", ResourceFormat.COG, PlanAction.CREATE),
        ("basemap", ResourceFormat.PMTILES, PlanAction.SKIP),
        ("notes", None, PlanAction.SKIP),
    ]


def test_geoserver_plan_marks_managed_resource_with_different_asset_as_update(
    tmp_path: Path,
) -> None:
    _catalog(tmp_path)
    client = RecordingGeoServerClient(existing={"roads": {"portolan.asset": "old.parquet"}})
    provider = GeoServerProvider(client=client, workspace="portolan")

    plan = provider.plan(tmp_path)

    assert plan.entries[0].action == PlanAction.UPDATE


def test_geoserver_publish_sends_resource_provenance(tmp_path: Path) -> None:
    _catalog(tmp_path)
    client = RecordingGeoServerClient()
    provider = GeoServerProvider(client=client, workspace="portolan")

    result = provider.publish(tmp_path)

    assert result.published == 2
    assert client.calls[0] == ("ensure_workspace", ("portolan",), {})
    publish_calls = [call for call in client.calls if call[0].startswith("publish_")]
    assert [call[0] for call in publish_calls] == ["publish_geoparquet", "publish_cog"]
    metadata = publish_calls[0][1][3]
    primary_key = publish_calls[0][1][4]
    native_name = publish_calls[0][1][5]
    assert metadata["portolan.managed"] is True
    assert metadata["portolan.catalog"] == str(tmp_path / "catalog.json")
    assert metadata["portolan.collection"] == "roads"
    assert metadata["portolan.asset"] == "https://example.test/roads.parquet"
    assert metadata["portolan.checksum"] == "sha256:roads"
    assert metadata["portolan.primary_key"] is None
    assert metadata["portolan.native_name"] == "roads"
    assert primary_key is None
    assert native_name == "roads"


def test_geoserver_publish_uses_safe_name_for_nested_collection_id() -> None:
    resource = ServerResourceSpec(
        id="medio-ambiente/meteorologicos-diarios",
        resource_type=ResourceType.VECTOR,
        format=ResourceFormat.GEOPARQUET,
        href="https://example.test/meteorologicos-diarios.parquet",
        title="Meteorologicos Diarios",
        description=None,
        extent=None,
        crs=None,
        primary_key=None,
        native_name="meteorologicos-diarios",
        metadata={},
        source_catalog=None,
        source_collection="medio-ambiente/meteorologicos-diarios",
        source_asset="data",
    )
    catalog = ServerCatalog(
        root=None,
        catalog_id="madrid-datos-abiertos",
        catalog_href="https://example.test/catalog.json",
        candidates=[
            CollectionCandidate(
                collection="medio-ambiente/meteorologicos-diarios",
                resource=resource,
            )
        ],
    )
    client = RecordingGeoServerClient()
    provider = GeoServerProvider(client=client)

    result = provider.publish_catalog(catalog)

    assert result.published == 1
    publish_call = [call for call in client.calls if call[0] == "publish_geoparquet"][0]
    assert publish_call[1][1] == "medio-ambiente__meteorologicos-diarios"
    assert publish_call[1][5] == "meteorologicos-diarios"
    assert publish_call[1][3]["portolan.collection"] == "medio-ambiente/meteorologicos-diarios"
    assert publish_call[1][3]["portolan.geoserver_name"] == (
        "medio-ambiente__meteorologicos-diarios"
    )


def test_geoserver_resource_name_replaces_url_path_separators() -> None:
    assert (
        geoserver_resource_name("medio-ambiente/meteorologicos-diarios")
        == "medio-ambiente__meteorologicos-diarios"
    )


def test_cli_geoserver_plan_uses_env_credentials(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    _catalog(tmp_path)
    monkeypatch.chdir(tmp_path)
    monkeypatch.setenv("PORTOLAN_GEOSERVER_URL", "http://geoserver.test/geoserver/cloud")
    monkeypatch.setenv("PORTOLAN_GEOSERVER_USER", "admin")
    monkeypatch.setenv("PORTOLAN_GEOSERVER_PASSWORD", "secret")
    monkeypatch.setattr(
        "portolan_cli.server.providers.geoserver.client.GeoServerClient",
        lambda url, user, password: RecordingGeoServerClient(),
    )

    result = CliRunner().invoke(cli, ["server", "geoserver", "plan"])

    assert result.exit_code == 0, result.output
    assert "Workspace: demo-catalog" in result.output
    assert "roads" in result.output
    assert "GeoParquet" in result.output
    assert "elevation" in result.output
    assert "COG" in result.output
    assert "secret" not in result.output


def test_cli_geoserver_plan_accepts_catalog_root_argument(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    _catalog(tmp_path)
    monkeypatch.setenv("PORTOLAN_GEOSERVER_URL", "http://geoserver.test/geoserver/cloud")
    monkeypatch.setenv("PORTOLAN_GEOSERVER_USER", "admin")
    monkeypatch.setenv("PORTOLAN_GEOSERVER_PASSWORD", "secret")
    monkeypatch.setattr(
        "portolan_cli.server.providers.geoserver.client.GeoServerClient",
        lambda url, user, password: RecordingGeoServerClient(),
    )

    result = CliRunner().invoke(cli, ["server", "geoserver", "plan", str(tmp_path)])

    assert result.exit_code == 0, result.output
    assert "Workspace: demo-catalog" in result.output
    assert "roads" in result.output


def test_registry_entries_use_valid_child_links() -> None:
    registry = {
        "links": [
            {
                "rel": "child",
                "href": "https://example.test/a/catalog.json",
                "title": "A",
                "portolan_registry:id": "catalog-a",
                "portolan_registry:status": "valid",
            },
            {
                "rel": "child",
                "href": "https://example.test/b/catalog.json",
                "portolan_registry:id": "catalog-b",
                "portolan_registry:status": "stale",
            },
        ]
    }

    entries = load_registry_entries(
        "https://registry.test/catalogs.json", fetch_json=lambda url: registry
    )

    assert [(entry.id, entry.url, entry.title, entry.status) for entry in entries] == [
        ("catalog-a", "https://example.test/a/catalog.json", "A", "valid")
    ]


def test_download_registry_catalog_writes_local_snapshot_with_absolute_asset_hrefs(
    tmp_path: Path,
) -> None:
    responses = {
        "https://example.test/demo/catalog.json": {
            "type": "Catalog",
            "id": "demo",
            "links": [
                {"rel": "child", "href": "./roads/collection.json", "type": "application/json"}
            ],
        },
        "https://example.test/demo/roads/collection.json": _collection(
            "roads",
            {
                "href": "./roads.parquet",
                "type": "application/vnd.apache.parquet",
                "roles": ["data"],
            },
        ),
    }

    catalog_root = download_registry_catalog(
        "https://example.test/demo/catalog.json",
        tmp_path,
        fetch_json=lambda url: responses[url],
    )

    assert catalog_root == tmp_path / "demo"
    catalog = json.loads((catalog_root / "catalog.json").read_text(encoding="utf-8"))
    collection = json.loads(
        (catalog_root / "roads" / "collection.json").read_text(encoding="utf-8")
    )
    assert catalog["links"][0]["href"] == "./roads/collection.json"
    assert collection["assets"]["data"]["href"] == "https://example.test/demo/roads/roads.parquet"


def test_cli_registry_list_outputs_online_catalogs(monkeypatch: pytest.MonkeyPatch) -> None:
    from portolan_cli.server.model import RegistryCatalogEntry

    monkeypatch.setattr(
        "portolan_cli.server.registry.load_registry_entries",
        lambda *args, **kwargs: [
            RegistryCatalogEntry(
                id="catalog-a",
                url="https://example.test/a/catalog.json",
                title="Catalog A",
                status="valid",
            )
        ],
    )

    result = CliRunner().invoke(cli, ["registry", "list"])

    assert result.exit_code == 0, result.output
    assert "catalog-a" in result.output
    assert "https://example.test/a/catalog.json" in result.output


def test_cli_registry_fetch_outputs_catalog_path(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    from portolan_cli.server.model import RegistryCatalogEntry

    monkeypatch.setattr(
        "portolan_cli.server.registry.load_registry_entries",
        lambda *args, **kwargs: [
            RegistryCatalogEntry(
                id="catalog-a",
                url="https://example.test/a/catalog.json",
                title="Catalog A",
                status="valid",
            )
        ],
    )
    monkeypatch.setattr(
        "portolan_cli.server.registry.download_registry_catalog",
        lambda catalog_url, output_dir: tmp_path / "catalog-a",
    )

    result = CliRunner().invoke(
        cli,
        ["registry", "fetch", "catalog-a", "--output", str(tmp_path), "--path-only"],
    )

    assert result.exit_code == 0, result.output
    assert result.output.strip() == str(tmp_path / "catalog-a")


def test_cli_registry_fetch_all_outputs_all_catalog_paths(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    from portolan_cli.server.model import RegistryCatalogEntry

    entries = [
        RegistryCatalogEntry(
            id="catalog-a",
            url="https://example.test/a/catalog.json",
            title="Catalog A",
            status="valid",
        ),
        RegistryCatalogEntry(
            id="catalog-b",
            url="https://example.test/b/catalog.json",
            title="Catalog B",
            status="valid",
        ),
    ]
    monkeypatch.setattr(
        "portolan_cli.server.registry.load_registry_entries",
        lambda *args, **kwargs: entries,
    )
    monkeypatch.setattr(
        "portolan_cli.server.registry.download_registry_catalog",
        lambda catalog_url, output_dir: output_dir / catalog_url.split("/")[-2],
    )

    result = CliRunner().invoke(
        cli,
        ["registry", "fetch", "--all", "--output", str(tmp_path), "--path-only"],
    )

    assert result.exit_code == 0, result.output
    assert result.output.splitlines() == [
        str(tmp_path / "a"),
        str(tmp_path / "b"),
    ]


def test_load_server_catalog_url_resolves_remote_collection_assets() -> None:
    responses = {
        "https://example.test/demo/catalog.json": {
            "type": "Catalog",
            "id": "demo",
            "links": [
                {"rel": "child", "href": "./roads/collection.json", "type": "application/json"}
            ],
        },
        "https://example.test/demo/roads/collection.json": _collection(
            "roads",
            {
                "href": "./roads.parquet",
                "type": "application/vnd.apache.parquet",
                "roles": ["data"],
            },
        ),
    }

    catalog = load_server_catalog_url(
        "https://example.test/demo/catalog.json",
        fetch_json=lambda url: responses[url],
    )

    assert catalog.catalog_id == "demo"
    assert catalog.catalog_href == "https://example.test/demo/catalog.json"
    assert catalog.candidates[0].resource is not None
    assert catalog.candidates[0].resource.href == "https://example.test/demo/roads/roads.parquet"


def test_fetch_json_rejects_non_http_url() -> None:
    with pytest.raises(ValueError, match="Unsupported catalog URL scheme: file"):
        _fetch_json("file:///tmp/catalog.json")


def test_geoserver_provider_can_plan_loaded_registry_catalog() -> None:
    responses = {
        "https://example.test/demo/catalog.json": {
            "type": "Catalog",
            "id": "demo",
            "links": [
                {"rel": "child", "href": "./roads/collection.json", "type": "application/json"}
            ],
        },
        "https://example.test/demo/roads/collection.json": _collection(
            "roads",
            {
                "href": "https://example.test/demo/roads/roads.parquet",
                "type": "application/vnd.apache.parquet",
                "roles": ["data"],
            },
        ),
    }
    catalog = load_server_catalog_url(
        "https://example.test/demo/catalog.json",
        fetch_json=lambda url: responses[url],
    )
    provider = GeoServerProvider(client=RecordingGeoServerClient(), workspace="demo-workspace")

    plan = provider.plan_catalog(catalog)

    assert plan.workspace == "demo-workspace"
    assert [(entry.collection, entry.action) for entry in plan.entries] == [
        ("roads", PlanAction.CREATE)
    ]


def test_empty_geoserver_coverage_store_payload_returns_no_names() -> None:
    payload = {"coverageStores": ""}

    assert coverage_store_names(payload) == []
