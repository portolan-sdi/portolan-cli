"""Portolan registry command tests."""

from __future__ import annotations

import json
from typing import TYPE_CHECKING, Any

import httpx
import pytest
from click.testing import CliRunner

from portolan_cli.cli import cli
from portolan_cli.registry import (
    RegistryCatalogEntry,
    _fetch_json,
    _same_origin_request_validator,
    download_registry_catalog,
    load_registry_entries,
)

if TYPE_CHECKING:
    from pathlib import Path

pytestmark = pytest.mark.unit


def _collection(collection_id: str, asset: dict[str, Any]) -> dict[str, Any]:
    return {
        "type": "Collection",
        "stac_version": "1.1.0",
        "id": collection_id,
        "description": f"{collection_id} description",
        "license": "CC-BY-4.0",
        "extent": {
            "spatial": {"bbox": [[-71.0, -35.0, -70.0, -34.0]]},
            "temporal": {"interval": [[None, None]]},
        },
        "links": [],
        "assets": {"data": asset},
    }


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


def test_registry_entries_resolve_relative_child_urls() -> None:
    registry = {
        "links": [
            {
                "rel": "child",
                "href": "../demo/catalog.json",
                "portolan_registry:id": "demo",
                "portolan_registry:status": "valid",
            }
        ]
    }

    entries = load_registry_entries(
        "https://registry.test/exports/catalogs.json",
        fetch_json=lambda url: registry,
    )

    assert entries[0].url == "https://registry.test/demo/catalog.json"


def test_registry_entries_ignore_malformed_links() -> None:
    registry = {
        "links": [
            "not-an-object",
            {"rel": "self", "href": "catalogs.json"},
            {"rel": "child", "href": 42, "portolan_registry:id": "bad-href"},
            {"rel": "child", "href": "catalog.json"},
            {
                "rel": "child",
                "href": "javascript:alert(1)",
                "portolan_registry:id": "unsafe-scheme",
                "portolan_registry:status": "valid",
            },
        ]
    }

    entries = load_registry_entries(fetch_json=lambda url: registry)

    assert entries == []


@pytest.mark.parametrize(
    ("payload", "expected"),
    [
        ({"links": []}, {"links": []}),
        (["not", "an", "object"], TypeError),
    ],
)
def test_fetch_json_validates_response_shape(
    payload: object,
    expected: dict[str, Any] | type[Exception],
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    original_client = httpx.Client

    def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(200, json=payload, request=request)

    def client_factory(**kwargs: object) -> httpx.Client:
        return original_client(transport=httpx.MockTransport(handler), **kwargs)

    monkeypatch.setattr(httpx, "Client", client_factory)

    if isinstance(expected, type) and issubclass(expected, Exception):
        with pytest.raises(expected, match="Expected JSON object"):
            _fetch_json("https://registry.test/catalogs.json")
    else:
        assert _fetch_json("https://registry.test/catalogs.json") == expected


def test_registry_entries_filter_ids_include_stale_and_apply_limit() -> None:
    registry = {
        "links": [
            {
                "rel": "child",
                "href": f"https://example.test/{catalog_id}/catalog.json",
                "portolan_registry:id": catalog_id,
                "portolan_registry:status": status,
            }
            for catalog_id, status in [
                ("catalog-a", "valid"),
                ("catalog-b", "stale"),
                ("catalog-c", "valid"),
            ]
        ]
    }

    entries = load_registry_entries(
        fetch_json=lambda url: registry,
        catalog_ids={"catalog-b", "catalog-c"},
        include_stale=True,
        limit=1,
    )

    assert entries == [
        RegistryCatalogEntry(
            id="catalog-b",
            url="https://example.test/catalog-b/catalog.json",
            status="stale",
        )
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
    assert catalog["links"][0]["href"] == "roads/collection.json"
    assert collection["assets"]["data"]["href"] == "https://example.test/demo/roads/roads.parquet"


def test_download_registry_catalog_rewrites_downloaded_children_to_local_hrefs(
    tmp_path: Path,
) -> None:
    root_url = "https://example.test/demo/catalog.json"
    child_url = "https://example.test/demo/roads/collection.json"
    item_url = "https://example.test/demo/roads/item.json"
    responses = {
        root_url: {
            "type": "Catalog",
            "id": "demo",
            "links": [
                {"rel": "self", "href": root_url},
                {"rel": "child", "href": child_url},
                {"rel": "child", "href": item_url},
            ],
        },
        child_url: {
            **_collection("roads", {}),
            "links": [{"rel": "child", "href": root_url}],
        },
        item_url: {"type": "Feature", "id": "road-1"},
    }

    catalog_root = download_registry_catalog(
        root_url,
        tmp_path,
        fetch_json=lambda url: responses[url],
    )

    catalog = json.loads((catalog_root / "catalog.json").read_text(encoding="utf-8"))
    collection = json.loads(
        (catalog_root / "roads" / "collection.json").read_text(encoding="utf-8")
    )
    assert catalog["links"] == [
        {"rel": "self", "href": root_url},
        {"rel": "child", "href": "roads/collection.json"},
        {"rel": "child", "href": item_url},
    ]
    assert collection["links"] == [{"rel": "child", "href": "../catalog.json"}]


def test_download_registry_catalog_makes_non_downloaded_relative_links_absolute(
    tmp_path: Path,
) -> None:
    root_url = "https://example.test/demo/catalog.json"
    collection_url = "https://example.test/demo/roads/collection.json"
    feature_url = "https://example.test/demo/roads/features/road-1.json"
    responses = {
        root_url: {
            "type": "Catalog",
            "id": "demo",
            "links": [{"rel": "child", "href": "./roads/collection.json"}],
        },
        collection_url: {
            **_collection("roads", {}),
            "links": [
                {"rel": "item", "href": "./items/road-1.json"},
                {"rel": "child", "href": "./features/road-1.json"},
            ],
        },
        feature_url: {"type": "Feature", "id": "road-1"},
    }

    catalog_root = download_registry_catalog(
        root_url,
        tmp_path,
        fetch_json=lambda url: responses[url],
    )

    collection = json.loads(
        (catalog_root / "roads" / "collection.json").read_text(encoding="utf-8")
    )
    assert collection["links"] == [
        {"rel": "item", "href": "https://example.test/demo/roads/items/road-1.json"},
        {"rel": "child", "href": feature_url},
    ]


def test_download_registry_catalog_keeps_downloaded_root_and_parent_links_local(
    tmp_path: Path,
) -> None:
    root_url = "https://example.test/demo/catalog.json"
    collection_url = "https://example.test/demo/roads/collection.json"
    responses = {
        root_url: {
            "type": "Catalog",
            "id": "demo",
            "links": [{"rel": "child", "href": "./roads/collection.json"}],
        },
        collection_url: {
            **_collection("roads", {}),
            "links": [
                {"rel": "root", "href": root_url},
                {"rel": "parent", "href": root_url},
            ],
        },
    }

    catalog_root = download_registry_catalog(
        root_url,
        tmp_path,
        fetch_json=lambda url: responses[url],
    )

    collection = json.loads(
        (catalog_root / "roads" / "collection.json").read_text(encoding="utf-8")
    )
    assert collection["links"] == [
        {"rel": "root", "href": "../catalog.json"},
        {"rel": "parent", "href": "../catalog.json"},
    ]


def test_download_registry_catalog_rejects_id_outside_output_directory(tmp_path: Path) -> None:
    catalog = {"type": "Catalog", "id": "../../outside", "links": []}

    with pytest.raises(ValueError, match="safe directory name"):
        download_registry_catalog(
            "https://example.test/demo/catalog.json",
            tmp_path,
            fetch_json=lambda url: catalog,
        )


def test_download_registry_catalog_rejects_non_http_url(tmp_path: Path) -> None:
    with pytest.raises(ValueError, match="must use HTTP or HTTPS"):
        download_registry_catalog("file:///tmp/catalog.json", tmp_path)


def test_download_registry_catalog_skips_child_cycles(tmp_path: Path) -> None:
    root_url = "https://example.test/demo/catalog.json"
    child_url = "https://example.test/demo/roads/collection.json"
    responses = {
        root_url: {
            "type": "Catalog",
            "id": "demo",
            "links": [{"rel": "child", "href": "./roads/collection.json"}],
        },
        child_url: {
            **_collection("roads", {}),
            "links": [{"rel": "child", "href": "../catalog.json"}],
        },
    }
    fetched_urls: list[str] = []

    def fetch_json(url: str) -> dict[str, Any]:
        fetched_urls.append(url)
        if fetched_urls.count(url) > 1:
            raise AssertionError(f"Fetched document twice: {url}")
        return responses[url]

    download_registry_catalog(root_url, tmp_path, fetch_json=fetch_json)

    assert fetched_urls == [root_url, child_url]


@pytest.mark.parametrize(
    ("child_url", "message"),
    [
        ("https://other.test/demo/collection.json", "different origin"),
        ("https://example.test/demo/../../outside.json", "outside catalog root"),
    ],
)
def test_download_registry_catalog_rejects_unsafe_child_urls_before_fetch(
    child_url: str,
    message: str,
    tmp_path: Path,
) -> None:
    root_url = "https://example.test/demo/catalog.json"
    catalog = {
        "type": "Catalog",
        "id": "demo",
        "links": [{"rel": "child", "href": child_url}],
    }
    fetched_urls: list[str] = []

    def fetch_json(url: str) -> dict[str, Any]:
        fetched_urls.append(url)
        if url != root_url:
            raise AssertionError(f"Fetched unsafe child URL: {url}")
        return catalog

    with pytest.raises(ValueError, match=message):
        download_registry_catalog(root_url, tmp_path, fetch_json=fetch_json)

    assert fetched_urls == [root_url]


def test_download_registry_catalog_rejects_symlink_escape(tmp_path: Path) -> None:
    root_url = "https://example.test/demo/catalog.json"
    child_url = "https://example.test/demo/linked/collection.json"
    output_dir = tmp_path / "output"
    outside_dir = tmp_path / "outside"
    catalog_root = output_dir / "demo"
    outside_dir.mkdir()
    catalog_root.mkdir(parents=True)
    (catalog_root / "linked").symlink_to(outside_dir, target_is_directory=True)
    responses = {
        root_url: {
            "type": "Catalog",
            "id": "demo",
            "links": [{"rel": "child", "href": child_url}],
        },
        child_url: _collection("linked", {}),
    }

    download_registry_catalog(
        root_url,
        output_dir,
        fetch_json=lambda url: responses[url],
    )

    assert not (outside_dir / "collection.json").exists()
    assert (catalog_root / "linked" / "collection.json").exists()


def test_download_registry_catalog_rejects_catalog_root_symlink(tmp_path: Path) -> None:
    output_dir = tmp_path / "output"
    outside_dir = tmp_path / "outside"
    output_dir.mkdir()
    outside_dir.mkdir()
    (output_dir / "demo").symlink_to(outside_dir, target_is_directory=True)

    with pytest.raises(ValueError, match="Catalog directory must not be a symlink"):
        download_registry_catalog(
            "https://example.test/demo/catalog.json",
            output_dir,
            fetch_json=lambda url: {"type": "Catalog", "id": "demo", "links": []},
        )

    assert list(outside_dir.iterdir()) == []


def test_download_registry_catalog_rejects_registry_id_mismatch(tmp_path: Path) -> None:
    with pytest.raises(ValueError, match="does not match registry id"):
        download_registry_catalog(
            "https://example.test/demo/catalog.json",
            tmp_path,
            expected_catalog_id="selected-catalog",
            fetch_json=lambda url: {"type": "Catalog", "id": "other-catalog", "links": []},
        )

    assert not (tmp_path / "selected-catalog").exists()


def test_download_registry_catalog_preserves_snapshot_when_child_fetch_fails(
    tmp_path: Path,
) -> None:
    catalog_root = tmp_path / "demo"
    catalog_root.mkdir()
    previous = catalog_root / "catalog.json"
    previous.write_text('{"id": "previous"}\n', encoding="utf-8")
    root_url = "https://example.test/demo/catalog.json"

    def fetch_json(url: str) -> dict[str, Any]:
        if url == root_url:
            return {
                "type": "Catalog",
                "id": "demo",
                "links": [{"rel": "child", "href": "./missing.json"}],
            }
        raise httpx.HTTPStatusError(
            "Unavailable",
            request=httpx.Request("GET", url),
            response=httpx.Response(503),
        )

    with pytest.raises(httpx.HTTPStatusError):
        download_registry_catalog(root_url, tmp_path, fetch_json=fetch_json)

    assert previous.read_text(encoding="utf-8") == '{"id": "previous"}\n'
    assert not list(tmp_path.glob(".demo.staging-*"))


def test_download_registry_catalog_rejects_concurrent_download(tmp_path: Path) -> None:
    (tmp_path / ".demo.lock").touch()

    with pytest.raises(RuntimeError, match="already in progress"):
        download_registry_catalog(
            "https://example.test/demo/catalog.json",
            tmp_path,
            fetch_json=lambda url: {"type": "Catalog", "id": "demo", "links": []},
        )


def test_download_registry_catalog_ignores_malformed_children_and_assets(tmp_path: Path) -> None:
    root_url = "https://example.test/demo/catalog.json"
    child_url = "https://example.test/demo/roads/collection.json"
    responses = {
        root_url: {
            "type": "Catalog",
            "id": "demo",
            "links": [
                "not-an-object",
                {"rel": "item", "href": "item.json"},
                {"rel": "child", "href": 42},
                {"rel": "child", "href": "roads/collection.json"},
            ],
        },
        child_url: {
            **_collection("roads", {}),
            "assets": {"metadata": "not-an-object"},
        },
    }

    catalog_root = download_registry_catalog(
        root_url,
        tmp_path,
        fetch_json=lambda url: responses[url],
    )

    collection = json.loads(
        (catalog_root / "roads" / "collection.json").read_text(encoding="utf-8")
    )
    assert collection["assets"] == {"metadata": "not-an-object"}


def test_redirect_request_validator_rejects_cross_origin_redirect() -> None:
    validate = _same_origin_request_validator("https://example.test/demo/catalog.json")

    with pytest.raises(ValueError, match="Redirect changed origin"):
        validate(httpx.Request("GET", "http://127.0.0.1/private"))


def test_download_registry_catalog_rejects_local_path_collisions(tmp_path: Path) -> None:
    root_url = "https://example.test/demo/catalog.json"
    first_url = "https://example.test/demo/collection.json?version=1"
    second_url = "https://example.test/demo/collection.json?version=2"
    responses = {
        root_url: {
            "type": "Catalog",
            "id": "demo",
            "links": [
                {"rel": "child", "href": first_url},
                {"rel": "child", "href": second_url},
            ],
        },
        first_url: _collection("first", {}),
        second_url: _collection("second", {}),
    }
    fetched_urls: list[str] = []

    def fetch_json(url: str) -> dict[str, Any]:
        fetched_urls.append(url)
        return responses[url]

    with pytest.raises(ValueError, match="same local path"):
        download_registry_catalog(root_url, tmp_path, fetch_json=fetch_json)

    assert fetched_urls == [root_url, first_url]


def test_cli_registry_list_outputs_online_catalogs(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(
        "portolan_cli.registry.load_registry_entries",
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


def test_cli_registry_list_outputs_json(monkeypatch: pytest.MonkeyPatch) -> None:
    entry = RegistryCatalogEntry(
        id="catalog-a",
        url="https://example.test/a/catalog.json",
        title="Catalog A",
        status="valid",
    )
    monkeypatch.setattr(
        "portolan_cli.registry.load_registry_entries",
        lambda *args, **kwargs: [entry],
    )

    result = CliRunner().invoke(cli, ["registry", "list", "--json"])

    assert result.exit_code == 0, result.output
    assert json.loads(result.output)["data"]["catalogs"] == [
        {
            "id": "catalog-a",
            "url": "https://example.test/a/catalog.json",
            "title": "Catalog A",
            "status": "valid",
        }
    ]


@pytest.mark.parametrize(
    "failure",
    [
        httpx.ConnectError("connection refused"),
        ValueError("Registry URL must use HTTP or HTTPS"),
        TypeError("Expected JSON object"),
    ],
)
def test_cli_registry_list_reports_registry_failures(
    failure: Exception, monkeypatch: pytest.MonkeyPatch
) -> None:
    def fail(*args: object, **kwargs: object) -> list[RegistryCatalogEntry]:
        raise failure

    monkeypatch.setattr("portolan_cli.registry.load_registry_entries", fail)

    result = CliRunner().invoke(cli, ["registry", "list"])

    assert result.exit_code == 1
    assert f"Could not load Portolan registry: {failure}" in result.output


def test_cli_registry_list_reports_registry_failure_as_json(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    def fail(*args: object, **kwargs: object) -> list[RegistryCatalogEntry]:
        raise ValueError("invalid registry URL")

    monkeypatch.setattr("portolan_cli.registry.load_registry_entries", fail)

    result = CliRunner().invoke(cli, ["registry", "list", "--json"])

    assert result.exit_code == 1
    assert json.loads(result.output) == {
        "success": False,
        "command": "registry list",
        "data": {},
        "errors": [
            {
                "type": "ValueError",
                "message": "Could not load Portolan registry: invalid registry URL",
            }
        ],
    }


def test_cli_registry_fetch_outputs_catalog_path(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setattr(
        "portolan_cli.registry.load_registry_entries",
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
        "portolan_cli.registry.download_registry_catalog",
        lambda catalog_url, output_dir, **kwargs: tmp_path / "catalog-a",
    )

    result = CliRunner().invoke(
        cli,
        ["registry", "fetch", "catalog-a", "--output", str(tmp_path), "--path-only"],
    )

    assert result.exit_code == 0, result.output
    assert result.output.strip() == str(tmp_path / "catalog-a")


def test_cli_registry_fetch_reports_download_failure_as_json(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setattr(
        "portolan_cli.registry.load_registry_entries",
        lambda *args, **kwargs: [
            RegistryCatalogEntry(
                id="catalog-a",
                url="https://example.test/a/catalog.json",
                status="valid",
            )
        ],
    )

    def fail(catalog_url: str, output_dir: Path, **kwargs: object) -> Path:
        raise httpx.HTTPStatusError(
            "404 Not Found",
            request=httpx.Request("GET", catalog_url),
            response=httpx.Response(404),
        )

    monkeypatch.setattr("portolan_cli.registry.download_registry_catalog", fail)

    result = CliRunner().invoke(
        cli,
        [
            "registry",
            "fetch",
            "catalog-a",
            "--output",
            str(tmp_path),
            "--json",
        ],
    )

    assert result.exit_code == 1
    assert json.loads(result.output) == {
        "success": False,
        "command": "registry fetch",
        "data": {},
        "errors": [
            {
                "type": "HTTPStatusError",
                "message": "Could not download catalog 'catalog-a': 404 Not Found",
            }
        ],
    }


def test_cli_registry_fetch_reports_registry_failure_as_json(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    def fail(*args: object, **kwargs: object) -> list[RegistryCatalogEntry]:
        raise ValueError("invalid registry response")

    monkeypatch.setattr("portolan_cli.registry.load_registry_entries", fail)

    result = CliRunner().invoke(cli, ["registry", "fetch", "catalog-a", "--json"])

    assert result.exit_code == 1
    error = json.loads(result.output)["errors"][0]
    assert error == {
        "type": "ValueError",
        "message": "Could not load Portolan registry: invalid registry response",
    }


@pytest.mark.parametrize(
    ("arguments", "message"),
    [
        (["registry", "fetch", "missing"], "Catalog not found in registry: missing"),
        (["registry", "fetch", "--all"], "No catalogs found in registry."),
    ],
)
def test_cli_registry_fetch_reports_empty_registry(
    arguments: list[str],
    message: str,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(
        "portolan_cli.registry.load_registry_entries",
        lambda *args, **kwargs: [],
    )

    result = CliRunner().invoke(cli, arguments)

    assert result.exit_code == 1
    assert message in result.output


@pytest.mark.parametrize(
    ("arguments", "message"),
    [
        (["registry", "fetch", "missing", "--json"], "Catalog not found in registry: missing"),
        (["registry", "fetch", "--all", "--json"], "No catalogs found in registry."),
    ],
)
def test_cli_registry_fetch_reports_empty_registry_as_json(
    arguments: list[str],
    message: str,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(
        "portolan_cli.registry.load_registry_entries",
        lambda *args, **kwargs: [],
    )

    result = CliRunner().invoke(cli, arguments)

    assert result.exit_code == 1
    assert json.loads(result.output) == {
        "success": False,
        "command": "registry fetch",
        "data": {},
        "errors": [{"type": "ClickException", "message": message}],
    }


def test_cli_registry_fetch_outputs_json(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    entry = RegistryCatalogEntry(
        id="catalog-a",
        url="https://example.test/a/catalog.json",
        status="valid",
    )
    catalog_root = tmp_path / "catalog-a"
    monkeypatch.setattr(
        "portolan_cli.registry.load_registry_entries",
        lambda *args, **kwargs: [entry],
    )
    monkeypatch.setattr(
        "portolan_cli.registry.download_registry_catalog",
        lambda *args, **kwargs: catalog_root,
    )

    result = CliRunner().invoke(
        cli,
        ["registry", "fetch", "catalog-a", "--output", str(tmp_path), "--json"],
    )

    assert result.exit_code == 0, result.output
    assert json.loads(result.output)["data"]["catalogs"] == [
        {
            "id": "catalog-a",
            "url": "https://example.test/a/catalog.json",
            "path": str(catalog_root),
        }
    ]


def test_cli_registry_fetch_all_outputs_all_catalog_paths(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
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
        "portolan_cli.registry.load_registry_entries",
        lambda *args, **kwargs: entries,
    )
    monkeypatch.setattr(
        "portolan_cli.registry.download_registry_catalog",
        lambda catalog_url, output_dir, **kwargs: output_dir / catalog_url.split("/")[-2],
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


@pytest.mark.parametrize(
    ("arguments", "message"),
    [
        (["registry", "fetch"], "Provide CATALOG_ID or use --all."),
        (
            ["registry", "fetch", "catalog-a", "--all"],
            "Use either CATALOG_ID or --all, not both.",
        ),
    ],
)
def test_cli_registry_fetch_rejects_invalid_selection(arguments: list[str], message: str) -> None:
    result = CliRunner().invoke(cli, arguments)

    assert result.exit_code == 1
    assert message in result.output


@pytest.mark.parametrize(
    ("arguments", "message"),
    [
        (["registry", "fetch", "--json"], "Provide CATALOG_ID or use --all."),
        (
            ["registry", "fetch", "catalog-a", "--all", "--json"],
            "Use either CATALOG_ID or --all, not both.",
        ),
    ],
)
def test_cli_registry_fetch_rejects_invalid_selection_as_json(
    arguments: list[str], message: str
) -> None:
    result = CliRunner().invoke(cli, arguments)

    assert result.exit_code == 1
    assert json.loads(result.output) == {
        "success": False,
        "command": "registry fetch",
        "data": {},
        "errors": [{"type": "ClickException", "message": message}],
    }
