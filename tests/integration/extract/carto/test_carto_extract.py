"""Integration tests for Carto extraction auto-init.

Exercises the real catalog auto-init pipeline (init_catalog → add_files →
via-links → metadata seeding) using a real GeoParquet fixture, with only the
network extraction mocked out.
"""

from __future__ import annotations

import json
import shutil
from pathlib import Path

import pytest

from portolan_cli.extract.carto.discovery import CartoDiscoveryResult, CartoTableInfo
from portolan_cli.extract.carto.orchestrator import CartoLayerSource
from portolan_cli.extract.common.report import ExtractionReport, MetadataExtracted
from portolan_cli.extract.common.source import LayerRef, build_catalog
from portolan_cli.licensing import ResolvedLicense
from tests.support.extract import make_layer_result, make_report

pytestmark = [pytest.mark.integration]

FIXTURES_DIR = Path(__file__).parent.parent.parent.parent / "fixtures"
SQL_API_URL = "https://phl.carto.com/api/v2/sql"


# Building the catalog directly skips the license resolution the driver does
# before its download, so the license is passed in here (issue #686).
TEST_LICENSE = ResolvedLicense(license_id="CC-BY-4.0", license_url=None)


def _auto_init_catalog(
    output_dir: Path,
    report: ExtractionReport,
    found: CartoDiscoveryResult,
    resolved_license: ResolvedLicense,
) -> None:
    """Build the catalog for ``report`` through the shared driver seam."""
    layers = [LayerRef(id=t.id, name=t.name, data=t) for t in found.tables]
    discovery = CartoLayerSource.discovery_from(found, layers)
    build_catalog(
        CartoLayerSource(), discovery, output_dir, report, resolved_license=resolved_license
    )


def _report(output_path: str, name: str) -> ExtractionReport:
    layer = make_layer_result(name=name, output_path=output_path, features=3, size_bytes=1202)
    metadata = MetadataExtracted(
        source_url=SQL_API_URL,
        description=None,
        attribution="Carto account: phl",
        keywords=None,
        producer=None,
        processing_notes=None,
        known_issues=None,
        license_info_raw=None,
    )
    return make_report([layer], SQL_API_URL, metadata=metadata)


def _discovery() -> CartoDiscoveryResult:
    return CartoDiscoveryResult(
        service_url=SQL_API_URL,
        tables=[CartoTableInfo("vacant_land", id=0, has_geometry=True)],
        account_name="phl",
    )


def test_auto_init_builds_catalog_with_via_link_and_metadata(tmp_path: Path) -> None:
    collection_dir = tmp_path / "vacant_land"
    collection_dir.mkdir()
    shutil.copy(FIXTURES_DIR / "simple.parquet", collection_dir / "vacant_land.parquet")

    report = _report("vacant_land/vacant_land.parquet", "vacant_land")
    _auto_init_catalog(tmp_path, report, _discovery(), TEST_LICENSE)

    # Catalog + collection STAC created
    assert (tmp_path / "catalog.json").exists()
    collection_json = tmp_path / "vacant_land" / "collection.json"
    assert collection_json.exists()

    # Provenance via-link points at the Carto SQL query for the table
    links = json.loads(collection_json.read_text()).get("links", [])
    via = [link for link in links if link.get("rel") == "via"]
    assert via, "expected a via provenance link"
    assert "phl.carto.com/api/v2/sql" in via[0]["href"]
    assert "vacant_land" in via[0]["href"]

    # metadata.yaml seeded at catalog and collection level
    assert (tmp_path / ".portolan" / "metadata.yaml").exists()
    assert (collection_dir / ".portolan" / "metadata.yaml").exists()


def test_auto_init_tracks_non_geo_table_as_tabular_collection(tmp_path: Path) -> None:
    """A non-geo extracted Parquet becomes a tabular collection.

    Auto-init must enable ``tabular.enabled``, otherwise add_files rejects a
    geometry-less file. The collection carries no marker field: tabular status
    is read off the asset itself. The docstring used to promise a
    ``portolan:geospatial: false`` flag under a validator rule id that no
    longer exists, which contradicted the assertion below (issue #654).
    """
    from portolan_cli.config import get_setting

    collection_dir = tmp_path / "lookup"
    collection_dir.mkdir()
    shutil.copy(
        FIXTURES_DIR / "scan" / "geoparquet_with_companions" / "lookup.parquet",
        collection_dir / "lookup.parquet",
    )

    report = _report("lookup/lookup.parquet", "lookup")
    discovery = CartoDiscoveryResult(
        service_url=SQL_API_URL,
        tables=[CartoTableInfo("lookup", id=0, has_geometry=False)],
        account_name="phl",
    )
    _auto_init_catalog(tmp_path, report, discovery, TEST_LICENSE)

    # Tabular support auto-enabled because a non-geo output was present.
    assert get_setting("tabular.enabled", catalog_path=tmp_path) is True

    collection_json = tmp_path / "lookup" / "collection.json"
    assert collection_json.exists()
    coll = json.loads(collection_json.read_text())
    # Tabular status is derived from asset content, not flagged (issue #654).
    assert "portolan:geospatial" not in coll

    # Provenance via-link still wired for the tabular collection.
    via = [link for link in coll.get("links", []) if link.get("rel") == "via"]
    assert via, "expected a via provenance link on the tabular collection"
    assert "lookup" in via[0]["href"]


def test_auto_init_skipped_when_no_successful_tables(tmp_path: Path) -> None:
    report = _report("vacant_land/vacant_land.parquet", "vacant_land")
    report.layers[0].status = "failed"
    report.layers[0].output_path = None

    _auto_init_catalog(tmp_path, report, _discovery(), TEST_LICENSE)
    assert not (tmp_path / "catalog.json").exists()


def test_a_thumbnail_exception_keeps_the_versions_of_the_add(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """The thumbnails run after the add. An exception there still commits the add."""
    import portolan_cli.collection_thumbnail as collection_thumbnail
    from portolan_cli.versions import read_versions

    def broken_thumbnails(*_args: object, **_kwargs: object) -> None:
        raise RuntimeError("thumbnail broke")

    monkeypatch.setattr(collection_thumbnail, "ensure_collection_thumbnails", broken_thumbnails)
    collection_dir = tmp_path / "vacant_land"
    collection_dir.mkdir()
    shutil.copy(FIXTURES_DIR / "simple.parquet", collection_dir / "vacant_land.parquet")
    report = _report("vacant_land/vacant_land.parquet", "vacant_land")

    with pytest.raises(RuntimeError, match="thumbnail broke"):
        _auto_init_catalog(tmp_path, report, _discovery(), TEST_LICENSE)

    versions = read_versions(collection_dir / "versions.json")
    assert versions.current_version == "1.0.0"
    assert sorted(versions.versions[-1].assets) == ["vacant_land.parquet"]
    index = json.loads((tmp_path / "versions.json").read_text(encoding="utf-8"))
    assert index["collections"]["vacant_land"]["current_version"] == "1.0.0"
