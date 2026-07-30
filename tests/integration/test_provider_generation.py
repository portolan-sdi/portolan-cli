"""Generated collections declare providers and their derived provenance (issue #684).

``init`` + ``add`` used to emit collections with no ``providers`` array, so every
generated catalog failed PTL-PRV-001 against its own ``portolan check``. These
tests build real catalogs both ways the spec recognises — official, where the
producer also hosts, and mirror, where they differ — and assert that rashid finds
nothing to report in either the provider or the provenance rule family.
"""

from __future__ import annotations

import json
import shutil
from datetime import datetime
from pathlib import Path
from typing import Any

import pyarrow as pa
import pyarrow.parquet as pq
import pytest
import yaml
from click.testing import CliRunner
from rashid import Severity, validate

from portolan_cli.cli import cli

pytestmark = pytest.mark.integration

FIXTURE = Path(__file__).resolve().parents[1] / "fixtures" / "simple.parquet"

OFFICIAL_PROVIDERS = [
    {
        "name": "Example City GIS",
        "roles": ["producer", "licensor", "host"],
        "url": "https://gis.example.org",
    }
]

MIRROR_PROVIDERS = [
    {"name": "INDEC", "roles": ["producer", "licensor"], "url": "https://indec.gob.ar"},
    {"name": "Example City GIS", "roles": ["host"], "url": "https://gis.example.org"},
]


def _build_catalog(root: Path, metadata: dict[str, Any], *, tabular: bool = False) -> None:
    collection_dir = root / "roads"
    collection_dir.mkdir(parents=True)
    shutil.copy(FIXTURE, collection_dir / "roads.parquet")

    paths = [str(collection_dir / "roads.parquet")]
    if tabular:
        tabular_dir = root / "demographics"
        tabular_dir.mkdir(parents=True)
        pq.write_table(
            pa.table({"tract_id": ["001", "002"], "population": [5000, 7500]}),
            tabular_dir / "census.parquet",
        )
        paths.append(str(tabular_dir / "census.parquet"))

    portolan_dir = root / ".portolan"
    portolan_dir.mkdir()
    (portolan_dir / "metadata.yaml").write_text(yaml.dump(metadata), encoding="utf-8")

    runner = CliRunner()
    result = runner.invoke(
        cli,
        [
            "init",
            str(root),
            "--auto",
            "--title",
            "Provider Catalog",
            "--description",
            "Roads published to exercise the provider model.",
        ],
        catch_exceptions=False,
    )
    assert result.exit_code == 0, result.output

    if tabular:
        config = portolan_dir / "config.yaml"
        config.write_text(config.read_text() + "tabular:\n  enabled: true\n", encoding="utf-8")

    result = runner.invoke(
        cli,
        ["add", "--portolan-dir", str(root), *paths],
        catch_exceptions=False,
    )
    assert result.exit_code == 0, result.output


def _read(path: Path) -> dict[str, Any]:
    loaded: dict[str, Any] = json.loads(path.read_text(encoding="utf-8"))
    return loaded


def _rfc3339(value: object) -> datetime:
    """Parse an offset-bearing RFC 3339 date-time, the shape PTL-PRO-003 accepts."""
    assert isinstance(value, str), f"expected an RFC 3339 string, got {value!r}"
    return datetime.fromisoformat(value.replace("Z", "+00:00"))


def _rel(collection: dict[str, Any], rel: str) -> list[dict[str, Any]]:
    return [link for link in collection.get("links", []) if link.get("rel") == rel]


def _provider_findings(root: Path) -> list[str]:
    report = validate(root, structural=True, data=True)
    return sorted(
        f.rule_id
        for f in report.findings
        if f.severity is Severity.ERROR
        and (f.rule_id.startswith("PTL-PRV") or f.rule_id.startswith("PTL-PRO"))
    )


@pytest.fixture(scope="module")
def official_catalog(tmp_path_factory: pytest.TempPathFactory) -> Path:
    root = tmp_path_factory.mktemp("official") / "catalog"
    _build_catalog(
        root,
        {
            "license": "CC-BY-4.0",
            "contact": {"name": "Example City GIS", "email": "gis@example.org"},
            "providers": OFFICIAL_PROVIDERS,
        },
        tabular=True,
    )
    return root


@pytest.fixture(scope="module")
def mirror_catalog(tmp_path_factory: pytest.TempPathFactory) -> Path:
    root = tmp_path_factory.mktemp("mirror") / "catalog"
    _build_catalog(
        root,
        {
            "license": "CC-BY-4.0",
            "contact": {"name": "Example City GIS", "email": "gis@example.org"},
            "providers": MIRROR_PROVIDERS,
            "source_url": "https://indec.gob.ar/descargas/roads",
        },
    )
    return root


class TestOfficialCatalog:
    def test_the_collection_declares_the_providers_with_the_host_last(
        self, official_catalog: Path
    ) -> None:
        collection = _read(official_catalog / "roads" / "collection.json")

        assert collection["providers"] == OFFICIAL_PROVIDERS

    def test_a_tabular_collection_declares_them_too(self, official_catalog: Path) -> None:
        """The tabular path never reaches finalize_items, so it applies them itself."""
        collection = _read(official_catalog / "demographics" / "collection.json")

        assert collection["providers"] == OFFICIAL_PROVIDERS

    def test_it_carries_no_upstream_links(self, official_catalog: Path) -> None:
        """PTL-PRO-004: an official collection is the source, not a mirror."""
        collection = _read(official_catalog / "roads" / "collection.json")

        assert _rel(collection, "via") == []
        assert _rel(collection, "canonical") == []

    def test_it_carries_no_updated_stamp(self, official_catalog: Path) -> None:
        collection = _read(official_catalog / "roads" / "collection.json")

        assert "updated" not in collection

    def test_rashid_reports_no_provider_or_provenance_error(self, official_catalog: Path) -> None:
        assert _provider_findings(official_catalog) == []


class TestMirrorCatalog:
    def test_the_collection_declares_the_providers_with_the_host_last(
        self, mirror_catalog: Path
    ) -> None:
        collection = _read(mirror_catalog / "roads" / "collection.json")

        assert [p["name"] for p in collection["providers"]] == ["INDEC", "Example City GIS"]
        assert collection["providers"][-1]["roles"] == ["host"]

    def test_it_links_back_to_the_source_as_text_html(self, mirror_catalog: Path) -> None:
        """PTL-PRO-001 wants the via link, and wants it typed text/html."""
        collection = _read(mirror_catalog / "roads" / "collection.json")

        via = _rel(collection, "via")
        assert len(via) == 1
        assert via[0]["href"] == "https://indec.gob.ar/descargas/roads"
        assert via[0]["type"] == "text/html"

    def test_it_records_the_sync_time(self, mirror_catalog: Path) -> None:
        """PTL-PRO-003 wants a top-level RFC 3339 updated field on the mirror."""
        collection = _read(mirror_catalog / "roads" / "collection.json")

        assert _rfc3339(collection.get("updated")).tzinfo is not None

    def test_the_root_catalog_records_it_too(self, mirror_catalog: Path) -> None:
        """PTL-PRO-003 reaches the root when every collection in the tree is a mirror."""
        catalog = _read(mirror_catalog / "catalog.json")

        assert _rfc3339(catalog.get("updated")).tzinfo is not None

    def test_rashid_reports_no_provider_or_provenance_error(self, mirror_catalog: Path) -> None:
        assert _provider_findings(mirror_catalog) == []


class TestContactSeedsTheHost:
    def test_a_producer_alone_still_yields_a_conformant_host(
        self, tmp_path_factory: pytest.TempPathFactory
    ) -> None:
        """PTL-PRV-002 and -003 are satisfied from contact, which is already required."""
        root = tmp_path_factory.mktemp("seeded") / "catalog"
        _build_catalog(
            root,
            {
                "license": "CC-BY-4.0",
                "contact": {"name": "Example City GIS", "email": "gis@example.org"},
                "providers": [{"name": "INDEC", "roles": ["producer"]}],
                "source_url": "https://indec.gob.ar/descargas/roads",
            },
        )

        collection = _read(root / "roads" / "collection.json")

        assert collection["providers"][-1] == {
            "name": "Example City GIS",
            "roles": ["host"],
            "email": "gis@example.org",
        }
        assert _provider_findings(root) == []
