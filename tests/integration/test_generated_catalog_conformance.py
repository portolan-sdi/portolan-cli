"""The conformance gate: a freshly generated catalog passes rashid (issues #654, #746).

Builds a catalog the way a user does — ``portolan init`` then ``portolan add``,
with the metadata.yaml enrichment expects — and runs every offline pass rashid
has over the result. Any generation change that breaks spec conformance fails
here, so the gate is the executable form of "Portolan emits conformant catalogs".

Two things widened it after #654 (issue #746).

The catalog was vector-only. Raster items, the item mirror, style assets,
PMTiles, and the catalog logo never reached rashid, so the drift #654 fixed —
raster declared at v1.1.0, a ``rel:"items"`` link the spec does not ask for,
seven undefined ``portolan:`` fields — could not have been caught here. The
catalog now carries all of them.

The gate also read only errors. rashid does own an extension-version rule,
PTL-CNF-004, and it fires on exactly the stale raster URI #654 removed, but it
reports a WARNING and the assertion threw warnings away. Severity is a judgment
about how bad a finding is, not about whether generated output changed, so the
gate now pins warnings and infos too. Every finding a generated catalog produces
is enumerated below; anything else fails.
"""

from __future__ import annotations

import json
import shutil
from collections import Counter
from pathlib import Path
from typing import Any

import pyarrow as pa
import pyarrow.parquet as pq
import pytest
import yaml
from click.testing import CliRunner
from rashid import RulesConfig, Severity, validate
from rashid.data.reader import LocalOnlyReader

from portolan_cli.cli import cli

pytestmark = pytest.mark.integration

FIXTURES = Path(__file__).resolve().parents[1] / "fixtures"
FIXTURE = FIXTURES / "simple.parquet"
RGB_COG = FIXTURES / "raster" / "valid" / "rgb.tif"
SINGLEBAND_COG = FIXTURES / "raster" / "valid" / "singleband.tif"
POINTS = FIXTURES / "vector" / "valid" / "points.geojson"
RASTER_SCHEMA = FIXTURES / "schemas" / "raster-v2.0.0.schema.json"

# A real 1x1 transparent PNG. rashid reads logo bytes, so magic bytes alone
# would not do.
LOGO_PNG = bytes.fromhex(
    "89504e470d0a1a0a0000000d4948445200000001000000010806000000"
    "1f15c4890000000a49444154789c630001000005000100"
    "0d0a2db40000000049454e44ae426082"
)

# Rules a generated catalog cannot yet satisfy without machinery that does not
# exist in the CLI. Each is a named gap with a diagnosed cause, not an accepted
# violation: ``test_known_gaps_are_still_real`` fails the moment one stops firing,
# so a fix cannot land without deleting its entry here.
KNOWN_GAPS = frozenset(
    {
        # PTL-DAT-007: every row group needs spatial statistics. geoparquet-io
        # writes a single row group for a file this small and emits neither a
        # bbox covering column nor native GeospatialStatistics for it, so the
        # rule cannot see per-row-group extents. Surfaced when rashid 0.1.1
        # promoted the data pass to default-on.
        "PTL-DAT-007",
        # PTL-DAT-009 (issue #748): COGs ship without embedded band statistics.
        # Portolan does compute them and does write them into the item, but GDAL
        # puts them in a `.aux.xml` PAM sidecar. rashid reads with
        # GDAL_PAM_ENABLED=NO, as PORTO-FMT-026..030 require, and sees none.
        # Found by adding raster to this gate.
        "PTL-DAT-009",
    }
)

# Findings a conformant generated catalog produces on purpose. Pinned by exact
# count, so a second collection growing the same warning is a change the gate
# reports rather than absorbs.
EXPECTED_ADVISORIES: dict[str, int] = {
    # PTL-DAT-010, twice, one per raster item: the valid-percent half of the
    # PTL-DAT-009 sidecar problem above. Same root cause, same issue #748.
    "PTL-DAT-010": 2,
    # PTL-DAT-015 (issue #749): the tabular collection documents no
    # `table:columns`. `_apply_table_extension` in finalization.py gates on
    # FormatType.VECTOR, so the tabular writer never reaches it.
    "PTL-DAT-015": 1,
    # PTL-VIZ-001, on that same tabular collection, and downstream of the line
    # above rather than separate from it. `table:columns` is how rashid decides
    # whether a collection is geospatial. Without it the question is undecidable,
    # so rashid softens the thumbnail MUST to a warning instead of skipping the
    # check. Documenting the columns answers it and retires both entries, which
    # is why #749 covers this line too.
    "PTL-VIZ-001": 1,
    # PTL-PRO-002, once per collection: every collection is a mirror, because
    # metadata.yaml names a producer distinct from the host. Advisory — a
    # rel:'canonical' link is only meaningful when the upstream publishes STAC.
    "PTL-PRO-002": 4,
}


def _write_metadata(root: Path) -> Path:
    """Write the .portolan/metadata.yaml every catalog in this module shares."""
    portolan_dir = root / ".portolan"
    portolan_dir.mkdir(parents=True, exist_ok=True)
    (portolan_dir / "metadata.yaml").write_text(
        yaml.dump(
            {
                "license": "CC-BY-4.0",
                "contact": "data@example.org",
                "source": "Example municipal open-data portal",
                # A mirror, the more demanding of the two provenance shapes: the
                # producer differs from the host, so generation owes a rel:'via'
                # link and an 'updated' stamp on both collections and, because
                # every collection here is a mirror, on the root catalog too
                # (PTL-PRV-001/002/003, PTL-PRO-001/003).
                "providers": [
                    {
                        "name": "Example Statistics Agency",
                        "roles": ["producer", "licensor"],
                        "url": "https://stats.example.org",
                    },
                    {
                        "name": "Example Municipal Open Data",
                        "roles": ["host"],
                        "url": "https://data.example.org/contact",
                    },
                ],
                "source_url": "https://stats.example.org/downloads/roads",
            }
        ),
        encoding="utf-8",
    )
    return portolan_dir


def _build_catalog(root: Path) -> None:
    """Run init + add over a geo and a tabular collection, with metadata.yaml enrichment."""
    collection_dir = root / "roads"
    collection_dir.mkdir(parents=True)
    shutil.copy(FIXTURE, collection_dir / "roads.parquet")

    # A three-level id, so generation has to write two intermediate catalogs and
    # the deeper one's parent differs from its root. Flat ids alone let #711
    # ship: root and parent coincide at the top level and the conflation between
    # them stays invisible.
    nested_dir = root / "env" / "air" / "quality"
    nested_dir.mkdir(parents=True)
    shutil.copy(FIXTURE, nested_dir / "quality.parquet")

    # A geometry-less Parquet exercises the tabular path, which writes its
    # collection.json outside finalize_items.
    tabular_dir = root / "demographics"
    tabular_dir.mkdir(parents=True)
    pq.write_table(
        pa.table({"tract_id": ["001", "002"], "population": [5000, 7500]}),
        tabular_dir / "census.parquet",
    )

    # Two raster scenes, because raster is the path #654's drift rode in on and
    # the gate never built it. Two rather than one for a reason: a lone scene
    # trips PTL-COL-001, which asks a single-file collection to carry its data at
    # collection level instead of wrapping it in an item. Scenes also give the
    # collection items, and items are what the mirror needs — every other
    # collection here is a single collection-level asset with no items at all,
    # which is why items.parquet was never generated under the old gate.
    # The collection/item/file layout is mandatory: `add` skips a raster placed
    # one level shallower, and still exits 0 while doing it.
    scene_a = root / "imagery" / "scene-a"
    scene_b = root / "imagery" / "scene-b"
    scene_a.mkdir(parents=True)
    scene_b.mkdir(parents=True)
    shutil.copy(RGB_COG, scene_a / "rgb.tif")
    shutil.copy(SINGLEBAND_COG, scene_b / "singleband.tif")

    portolan_dir = _write_metadata(root)
    logo_source = root.parent / "brand.png"
    logo_source.write_bytes(LOGO_PNG)

    runner = CliRunner()
    result = runner.invoke(
        cli,
        [
            "init",
            str(root),
            "--auto",
            "--title",
            "Demo Catalog",
            "--description",
            "Roads published for the conformance gate.",
            # init keeps the metadata.yaml written above, so this only satisfies the
            # flag's requirement; the license the catalog uses is the one in that file.
            "--license",
            "CC-BY-4.0",
            # PORTO-CORE-074..077. A MAY, so plain init writes no icon link —
            # which is why the gate has to ask for one to cover it.
            "--logo",
            str(logo_source),
            "--logo-title",
            "Example Municipal Open Data",
        ],
        catch_exceptions=False,
    )
    assert result.exit_code == 0, result.output

    config = portolan_dir / "config.yaml"
    config.write_text(config.read_text() + "tabular:\n  enabled: true\n", encoding="utf-8")

    result = runner.invoke(
        cli,
        [
            "add",
            "--portolan-dir",
            str(root),
            str(collection_dir / "roads.parquet"),
            str(nested_dir / "quality.parquet"),
            str(tabular_dir / "census.parquet"),
            str(scene_a / "rgb.tif"),
            str(scene_b / "singleband.tif"),
        ],
        catch_exceptions=False,
    )
    assert result.exit_code == 0, result.output
    # `add` reports a skipped raster on stdout and still exits 0, so the exit
    # code alone would not prove the scenes landed.
    assert (root / "imagery" / "collection.json").exists(), result.output


def _build_visual_catalog(root: Path) -> None:
    """Build a collection carrying three styles and PMTiles.

    Separate from the main catalog because PMTiles needs tippecanoe, which CI
    installs on Linux and macOS but not Windows. Keeping it apart lets the core
    gate run everywhere.

    `add` never registers style assets on its own — `register_style_assets` has
    exactly two callers, the end of PMTiles generation and the extract
    orchestrator — so `--pmtiles` is the only offline route to a styled
    collection. Two hand-written styles plus the generated `styles/default`
    make three, and three is the point: PTL-VIZ-006 short-circuits below two
    styles, so a smaller collection would never test the default-role MUST.
    """
    collection_dir = root / "places"
    (collection_dir / "styles").mkdir(parents=True)
    # points.geojson, not simple.parquet: tippecanoe needs two distinct feature
    # locations to guess a maxzoom, and simple.parquet holds one point. It is
    # also EPSG:4326, which matters because the thumbnail renderer returns None
    # on a projected CRS and the collection would then fail PTL-VIZ-001.
    shutil.copy(POINTS, collection_dir / "places.geojson")
    for name, title in (("aqua", "Aqua"), ("zebra", "Zebra")):
        (collection_dir / "styles" / f"{name}.json").write_text(
            json.dumps({"version": 8, "name": title, "sources": {}, "layers": []}),
            encoding="utf-8",
        )

    _write_metadata(root)

    runner = CliRunner()
    result = runner.invoke(
        cli,
        [
            "init",
            str(root),
            "--auto",
            "--title",
            "Visual Catalog",
            "--description",
            "Styled points published for the conformance gate.",
            "--license",
            "CC-BY-4.0",
        ],
        catch_exceptions=False,
    )
    assert result.exit_code == 0, result.output

    result = runner.invoke(
        cli,
        [
            "add",
            "--portolan-dir",
            str(root),
            "--pmtiles",
            str(collection_dir / "places.geojson"),
        ],
        catch_exceptions=False,
    )
    assert result.exit_code == 0, result.output


@pytest.fixture(scope="module")
def generated_catalog(tmp_path_factory: pytest.TempPathFactory) -> Path:
    root = tmp_path_factory.mktemp("conformance") / "demo-catalog"
    _build_catalog(root)
    return root


@pytest.fixture(scope="module")
def visual_catalog(tmp_path_factory: pytest.TempPathFactory) -> Path:
    root = tmp_path_factory.mktemp("conformance-visual") / "visual-catalog"
    _build_visual_catalog(root)
    return root


def _validate(root: Path, **kwargs: Any) -> Any:
    """Run every offline pass rashid has: metadata, STAC structural, profile, data.

    ``schema=True`` adds PTL-SCH-001, which applies the Portolan profile schema
    the catalog declares. rashid defaults it off because it overlaps the metadata
    rules, but it is bundled in the wheel and costs no network, so leaving it off
    only forfeits coverage.

    ``LocalOnlyReader`` is what keeps the gate hermetic. The default data reader
    resolves an absolute https href over the wire, so a catalog that ever grows a
    remote asset would silently start making requests. This one resolves inside
    the catalog tree and nowhere else. ``live`` stays off; it is the only pass
    that probes hosts by design.
    """
    return validate(
        root,
        structural=True,
        schema=True,
        data=True,
        data_reader_factory=LocalOnlyReader,
        **kwargs,
    )


def _rule_counts(root: Path, severity: Severity) -> Counter[str]:
    """Count findings by rule id at one severity, with the known gaps disabled."""
    report = _validate(root, config=RulesConfig(disabled=KNOWN_GAPS))
    return Counter(f.rule_id for f in report.findings if f.severity is severity)


def _findings(root: Path, severity: Severity) -> list[dict[str, Any]]:
    report = _validate(root, config=RulesConfig(disabled=KNOWN_GAPS))
    return [f.to_dict() for f in report.findings if f.severity is severity]


def _load(path: Path) -> dict[str, Any]:
    loaded: dict[str, Any] = json.loads(path.read_text(encoding="utf-8"))
    return loaded


class TestGeneratedCatalogConformance:
    def test_no_errors(self, generated_catalog: Path) -> None:
        """rashid reports zero errors on a generated catalog."""
        assert _findings(generated_catalog, Severity.ERROR) == []

    def test_known_gaps_are_still_real(self, generated_catalog: Path) -> None:
        """The disabled rules still fire, so the gap list cannot rot silently."""
        report = _validate(generated_catalog)
        fired = {f.rule_id for f in report.findings if f.severity is Severity.ERROR}

        assert fired == KNOWN_GAPS

    def test_no_unexpected_advisories(self, generated_catalog: Path) -> None:
        """Warnings and infos are pinned by exact count, not discarded.

        This is the assertion #654 needed and did not have. rashid's PTL-CNF-004
        reports a stale extension version as a WARNING, so an error-only gate
        watched raster sit at v1.1.0 and stayed green. Pinning counts means a new
        advisory fails here, and a fixed one fails too until its entry goes.
        """
        observed = _rule_counts(generated_catalog, Severity.WARNING) + _rule_counts(
            generated_catalog, Severity.INFO
        )

        assert dict(observed) == EXPECTED_ADVISORIES

    def test_every_pass_actually_ran(self, generated_catalog: Path) -> None:
        """No pass silently skipped itself.

        rashid degrades a pass it cannot run to a single ``-000`` warning rather
        than failing the run. A gate that counts only rule findings cannot tell a
        clean data pass from an absent one, so assert the absence directly.
        """
        report = _validate(generated_catalog)
        skipped = sorted(f.rule_id for f in report.findings if f.rule_id.endswith("-000"))

        assert skipped == []


class TestUnlicensedCatalogIsRefused:
    """The gate above only ever saw the happy path, which is what hid issue #686.

    A catalog whose metadata.yaml omits the license used to generate collections
    carrying ``license: "other"`` with no ``rel="license"`` link, a PTL-LIC-002
    ERROR. Portolan now refuses to write such a collection at all, so the honest
    assertion is that nothing gets written rather than that the output conforms.
    """

    def _catalog_without_a_license(self, root: Path) -> Path:
        """Build a managed catalog whose metadata.yaml declares no license."""
        collection_dir = root / "roads"
        collection_dir.mkdir(parents=True)
        shutil.copy(FIXTURE, collection_dir / "roads.parquet")

        portolan_dir = root / ".portolan"
        portolan_dir.mkdir()
        (portolan_dir / "config.yaml").write_text("# Portolan configuration\n", encoding="utf-8")
        (portolan_dir / "metadata.yaml").write_text(
            yaml.dump({"contact": "data@example.org"}), encoding="utf-8"
        )
        (root / "catalog.json").write_text(
            '{"type": "Catalog", "stac_version": "1.1.0", "id": "demo",'
            ' "description": "No license here", "links": []}',
            encoding="utf-8",
        )
        return collection_dir / "roads.parquet"

    def test_add_refuses_and_writes_no_collection(self, tmp_path: Path) -> None:
        root = tmp_path / "unlicensed"
        source = self._catalog_without_a_license(root)

        result = CliRunner().invoke(cli, ["add", "--portolan-dir", str(root), str(source)])

        assert result.exit_code == 1, result.output
        assert "PRTLN-VAL004" in result.output
        assert list(root.rglob("collection.json")) == []

    def test_the_violation_it_prevents_is_real(self, tmp_path: Path) -> None:
        """Prove PTL-LIC-002 is what the gate averts, not a rule we assume exists.

        Takes a conformant generated catalog and puts back exactly what the old
        code emitted: ``license: "other"`` and no ``rel="license"`` link. Without
        this, the test above would keep passing even if the rule it protects
        against were renamed or dropped upstream.
        """
        root = tmp_path / "damaged-catalog"
        _build_catalog(root)
        collection_path = root / "roads" / "collection.json"

        collection = json.loads(collection_path.read_text(encoding="utf-8"))
        collection["license"] = "other"
        collection["links"] = [link for link in collection["links"] if link.get("rel") != "license"]
        collection_path.write_text(json.dumps(collection, indent=2), encoding="utf-8")

        fired = {
            f.rule_id
            for f in _validate(root, config=RulesConfig(disabled=KNOWN_GAPS)).findings
            if f.severity is Severity.ERROR
        }

        assert fired == {"PTL-LIC-002"}


class TestAnIdentifierOutsideThePopularShortlistSurvivesGeneration:
    """Issue #727: the two commands disagreed on real SPDX identifiers.

    ``metadata validate`` judged the license against a hand-written 26-entry
    subset while ``check`` used rashid's full list, so ``EUPL-1.2`` was rejected
    by one and accepted by the other. Proving agreement on a dict is not enough;
    this runs the identifier through generation and validates the output.
    """

    def test_eupl_reaches_the_collection_and_conforms(self, tmp_path: Path) -> None:
        root = tmp_path / "eupl-catalog"
        collection_dir = root / "roads"
        collection_dir.mkdir(parents=True)
        shutil.copy(FIXTURE, collection_dir / "roads.parquet")

        portolan_dir = root / ".portolan"
        portolan_dir.mkdir()
        (portolan_dir / "config.yaml").write_text("# Portolan configuration\n", encoding="utf-8")
        (portolan_dir / "metadata.yaml").write_text(
            yaml.dump({"contact": "data@example.org", "license": "EUPL-1.2"}), encoding="utf-8"
        )
        (root / "catalog.json").write_text(
            '{"type": "Catalog", "stac_version": "1.1.0", "id": "demo",'
            ' "description": "Licensed under EUPL-1.2", "links": []}',
            encoding="utf-8",
        )

        result = CliRunner().invoke(
            cli, ["add", "--portolan-dir", str(root), str(collection_dir / "roads.parquet")]
        )

        assert result.exit_code == 0, result.output
        collection = json.loads((collection_dir / "collection.json").read_text(encoding="utf-8"))
        assert collection["license"] == "EUPL-1.2"

        license_findings = [
            f.rule_id
            for f in _validate(root, config=RulesConfig(disabled=KNOWN_GAPS)).findings
            if f.rule_id.startswith("PTL-LIC")
        ]
        assert license_findings == []


COG_MEDIA_TYPE = "image/tiff; application=geotiff; profile=cloud-optimized"


class TestRasterItemsMatchThePublishedExtensionSchema:
    """Raster is the path #654's drift rode in on, and the gate never built it.

    A green rashid run is not enough on its own here. PTL-CNF-004 compares a
    declared URI against a pinned registry, which catches a stale *version* but
    says nothing about whether the document at that URI would accept the item.
    These assertions read the schema itself.
    """

    def test_the_item_declares_raster_v2(self, generated_catalog: Path) -> None:
        item = _load(generated_catalog / "imagery" / "scene-a" / "scene-a.json")

        from portolan_cli.stac import EXTENSION_URLS

        assert EXTENSION_URLS["raster"] in item["stac_extensions"]

    def test_the_item_validates_against_that_schema(self, generated_catalog: Path) -> None:
        """The assertion string-comparing URIs cannot make.

        raster v2.0.0 requires at least one ``raster:``-prefixed field somewhere
        in an item that declares it. Portolan writes ``raster:spatial_resolution``
        and declares the extension only when it did, which is exactly what makes
        this pass. Both halves are load-bearing, so validate the real output.
        """
        import jsonschema

        for scene in ("scene-a", "scene-b"):
            item = _load(generated_catalog / "imagery" / scene / f"{scene}.json")
            jsonschema.validate(item, _load(RASTER_SCHEMA))

    def test_a_raster_field_is_actually_present(self, generated_catalog: Path) -> None:
        """Guard the schema test above against passing for the wrong reason.

        The v2.0.0 ``anyOf`` only binds on items that declare the extension. If a
        later change stopped declaring it, validation would still pass while the
        item lost its raster metadata.
        """
        item = _load(generated_catalog / "imagery" / "scene-a" / "scene-a.json")
        raster_fields = [key for key in item["properties"] if key.startswith("raster:")]

        assert raster_fields == ["raster:spatial_resolution"]

    def test_the_data_asset_carries_the_cog_media_type(self, generated_catalog: Path) -> None:
        """PTL-AST-006, which rashid gained in 0.1.5.

        In 0.1.4 a wrong COG media type also masked PTL-COL-004 and PTL-MIR-001,
        so this is the assertion the pin bump bought.
        """
        item = _load(generated_catalog / "imagery" / "scene-a" / "scene-a.json")

        assert item["assets"]["data"]["type"] == COG_MEDIA_TYPE

    def test_the_collection_uses_proj_code_not_proj_epsg(self, generated_catalog: Path) -> None:
        """projection v2.0.0 deleted ``proj:epsg``; #744 moved Portolan to ``proj:code``."""
        collection = _load(generated_catalog / "imagery" / "collection.json")

        assert "proj:epsg" not in collection
        assert collection["summaries"]["proj:code"] == ["EPSG:4326"]


class TestTheItemMirrorHasItsCanonicalShape:
    """#744 made the mirror an asset alone. Nothing built one under the gate.

    The mirror needs items, and every other collection here is a single
    collection-level asset with none, so the raster scenes are what put
    items.parquet on disk at all.
    """

    def test_the_mirror_is_generated_by_default(self, generated_catalog: Path) -> None:
        """No flag, no config, no item-count threshold — #744 removed all three."""
        assert (generated_catalog / "imagery" / "items.parquet").is_file()

    def test_the_mirror_asset_is_the_whole_registration(self, generated_catalog: Path) -> None:
        collection = _load(generated_catalog / "imagery" / "collection.json")
        mirror = collection["assets"]["geoparquet-items"]

        assert mirror["href"] == "./items.parquet"
        assert mirror["type"] == "application/vnd.apache.parquet"
        assert mirror["roles"] == ["collection-mirror"]

    def test_no_items_link_and_no_stac_items_role(self, generated_catalog: Path) -> None:
        """formats.md: registering the asset is the whole requirement.

        Portolan wrote a ``rel:"items"`` link and a ``stac-items`` role for
        months. Neither is in the spec, and rashid has no rule against either, so
        only an assertion on the emitted bytes keeps them gone.
        """
        collection = _load(generated_catalog / "imagery" / "collection.json")

        assert [link for link in collection["links"] if link.get("rel") == "items"] == []
        for key, asset in collection["assets"].items():
            assert "stac-items" not in asset.get("roles", []), key

    def test_the_mirror_is_not_re_ingested_as_data(self, generated_catalog: Path) -> None:
        """A generated asset must not become an input on the next add.

        items.parquet sits inside the collection directory and is a GeoParquet
        file, so a directory scan finds it and a naive add treats it as data.
        """
        collection = _load(generated_catalog / "imagery" / "collection.json")
        data_assets = {
            key for key, asset in collection["assets"].items() if "data" in asset.get("roles", [])
        }

        assert "geoparquet-items" not in data_assets


class TestTheCatalogLogoConforms:
    """PORTO-CORE-074..077, enforced by PTL-LNK-007/008/009 as of rashid 0.1.5.

    007 is an error and 008/009 are warnings, so the count assertion in
    ``test_no_unexpected_advisories`` is what covers the latter two — a logo that
    lost its title or went absolute would show up there as a new advisory.
    """

    def test_the_icon_link_has_its_canonical_shape(self, generated_catalog: Path) -> None:
        catalog = _load(generated_catalog / "catalog.json")
        icons = [link for link in catalog["links"] if link.get("rel") == "icon"]

        assert icons == [
            {
                "rel": "icon",
                "href": "./_assets/brand.png",
                "type": "image/png",
                "title": "Example Municipal Open Data",
            }
        ]

    def test_the_image_is_published_under_assets(self, generated_catalog: Path) -> None:
        assert (generated_catalog / "_assets" / "brand.png").read_bytes() == LOGO_PNG

    def test_only_the_root_catalog_carries_one(self, generated_catalog: Path) -> None:
        """core.md scopes the logo to the root catalog.

        rashid's icon rules run on every node, so they would accept one on a
        collection. Portolan should not write one there.
        """
        for collection_json in generated_catalog.rglob("collection.json"):
            links = _load(collection_json)["links"]
            assert [link for link in links if link.get("rel") == "icon"] == [], collection_json


class TestThumbnailsSurviveARealPass:
    """Thumbnails reached rashid already, generated on every add since #743.

    What was missing is any assertion on their shape in a validated catalog, and
    on the file-extension declaration #744 added for the case where a thumbnail
    is the only asset carrying ``file:`` fields.
    """

    def test_every_geospatial_collection_has_one(self, generated_catalog: Path) -> None:
        for name in ("roads", "env/air/quality", "imagery"):
            collection = _load(generated_catalog / name / "collection.json")
            thumbnail = collection["assets"]["thumbnail"]

            assert thumbnail["roles"] == ["thumbnail"]
            assert thumbnail["type"] in {"image/png", "image/jpeg", "image/webp"}
            assert thumbnail["file:size"] > 0
            # Multihash, not a bare digest: 0x12 sha2-256, 0x20 32 bytes.
            assert thumbnail["file:checksum"].startswith("1220")

    def test_the_file_extension_is_declared_where_file_fields_appear(
        self, generated_catalog: Path
    ) -> None:
        file_ext = "https://stac-extensions.github.io/file/v2.1.0/schema.json"
        for collection_json in generated_catalog.rglob("collection.json"):
            collection = _load(collection_json)
            has_file_fields = any(
                key.startswith("file:") for asset in collection["assets"].values() for key in asset
            )
            if has_file_fields:
                assert file_ext in collection["stac_extensions"], collection_json

    def test_the_tabular_collection_has_none(self, generated_catalog: Path) -> None:
        """A geometry-less collection is not geospatial, so a thumbnail would lie."""
        collection = _load(generated_catalog / "demographics" / "collection.json")

        assert "thumbnail" not in collection["assets"]


class TestNoUndefinedPortolanFieldsAreEmitted:
    """#744 removed seven ``portolan:`` fields the spec never defined.

    rashid cannot help here. Its profile schema is open, so every one of those
    fields validates clean, and no metadata rule inspects unknown keys. That is
    why the drift lasted: nothing in the toolchain objected. The only durable
    guard is reading the emitted bytes, which is what this does.
    """

    # The prefix is legitimate in exactly one place: the versioned profile URI
    # each catalog and collection declares.
    SCHEMA_URI_PREFIX = "https://schemas.portolan-sdi.org/portolan/"

    def _portolan_keys(self, node: Any) -> list[str]:
        found: list[str] = []
        if isinstance(node, dict):
            for key, value in node.items():
                if isinstance(key, str) and key.startswith("portolan:"):
                    found.append(key)
                found.extend(self._portolan_keys(value))
        elif isinstance(node, list):
            for entry in node:
                found.extend(self._portolan_keys(entry))
        return found

    def test_no_object_in_the_catalog_carries_one(self, generated_catalog: Path) -> None:
        offenders: dict[str, list[str]] = {}
        for path in generated_catalog.rglob("*.json"):
            if path.name == "versions.json" or ".portolan" in path.parts:
                continue
            keys = self._portolan_keys(_load(path))
            if keys:
                offenders[str(path.relative_to(generated_catalog))] = keys

        assert offenders == {}

    def test_the_schema_uri_is_the_only_portolan_string(self, generated_catalog: Path) -> None:
        """Catch the fields a key-walk would miss, such as one nested in a value.

        Also proves the test above is not passing because nothing writes the
        prefix at all: the profile URI must still be there.
        """
        catalog_text = (generated_catalog / "catalog.json").read_text(encoding="utf-8")

        assert self.SCHEMA_URI_PREFIX in catalog_text
        assert "portolan:" not in catalog_text.replace(self.SCHEMA_URI_PREFIX, "")


@pytest.mark.skipif(
    shutil.which("tippecanoe") is None,
    reason="tippecanoe not installed (required for PMTiles generation)",
)
class TestStylesAndPMTilesConform:
    """The two paths `add` alone never reaches.

    PMTiles is off by default and styles register only at the end of PMTiles
    generation, so neither had ever been validated end to end.
    """

    def test_the_catalog_has_no_errors(self, visual_catalog: Path) -> None:
        assert _findings(visual_catalog, Severity.ERROR) == []

    def test_exactly_one_style_is_the_default(self, visual_catalog: Path) -> None:
        """PORTO-CORE-070, a MUST once a collection registers more than one style.

        Before #744 an unmarked tie left no default at all, so `add` emitted a
        collection that violated the MUST. PTL-VIZ-006 short-circuits below two
        styles, which is why the fixture carries three.
        """
        collection = _load(visual_catalog / "places" / "collection.json")
        styles = {
            key: asset
            for key, asset in collection["assets"].items()
            if "style" in asset.get("roles", [])
        }
        defaults = sorted(key for key, asset in styles.items() if "default" in asset["roles"])

        assert sorted(styles) == ["styles/aqua", "styles/default", "styles/zebra"]
        assert defaults == ["styles/default"]

    def test_style_assets_are_typed_and_stamped(self, visual_catalog: Path) -> None:
        collection = _load(visual_catalog / "places" / "collection.json")
        for key, asset in collection["assets"].items():
            if "style" not in asset.get("roles", []):
                continue
            assert asset["type"] == "application/vnd.mapbox.style+json", key
            assert asset["file:size"] > 0, key
            assert asset["file:checksum"].startswith("1220"), key

    def test_the_pmtiles_asset_is_registered_and_stamped(self, visual_catalog: Path) -> None:
        """The stamping half is new.

        Style and thumbnail writers stamped ``file:`` fields; the PMTiles writer
        did not, and the resulting PTL-AST-003 warnings went unseen because the
        gate read only errors.
        """
        collection = _load(visual_catalog / "places" / "collection.json")
        tiles = collection["assets"]["places-tiles"]

        assert tiles["type"] == "application/vnd.pmtiles"
        assert tiles["roles"] == ["visual"]
        assert tiles["file:size"] > 0
        assert tiles["file:checksum"].startswith("1220")

    def test_pmtiles_is_registered_as_a_web_map_link(self, visual_catalog: Path) -> None:
        collection = _load(visual_catalog / "places" / "collection.json")
        links = [link for link in collection["links"] if link.get("rel") == "pmtiles"]

        assert len(links) == 1
        assert links[0]["type"] == "application/vnd.pmtiles"
        assert (
            "https://stac-extensions.github.io/web-map-links/v1.3.0/schema.json"
            in collection["stac_extensions"]
        )


class TestTheGateCatchesTheDriftItWasBuiltFor:
    """Falsification. A gate nobody has seen fail proves nothing.

    Each test puts back something #744 removed and asserts the gate goes red.
    Without these, the assertions above could be vacuous and look identical.
    """

    @pytest.fixture
    def damaged(self, generated_catalog: Path, tmp_path: Path) -> Path:
        root = tmp_path / "damaged-catalog"
        shutil.copytree(generated_catalog, root)
        return root

    def test_a_stale_extension_version_fails(self, damaged: Path) -> None:
        """The #654 drift itself: raster pinned a major version behind.

        This is the case an error-only gate could not see. PTL-CNF-004 reports it
        as a warning, deliberately — a catalog published before the registry moved
        is behind, not broken — so the count assertion is what catches it.
        """
        item_path = damaged / "imagery" / "scene-a" / "scene-a.json"
        item = _load(item_path)
        item["stac_extensions"] = [
            "https://stac-extensions.github.io/raster/v1.1.0/schema.json"
            if uri.startswith("https://stac-extensions.github.io/raster/")
            else uri
            for uri in item["stac_extensions"]
        ]
        item_path.write_text(json.dumps(item, indent=2), encoding="utf-8")

        advisories = _rule_counts(damaged, Severity.WARNING) + _rule_counts(damaged, Severity.INFO)

        assert advisories["PTL-CNF-004"] == 1
        assert dict(advisories) != EXPECTED_ADVISORIES

    def test_a_stale_extension_version_also_fails_the_schema(self, damaged: Path) -> None:
        """And the schema assertion catches it a second way.

        Two independent detectors for one defect, because PTL-CNF-004 only reads
        the URI string while the schema check reads the document behind it.
        """
        import jsonschema

        item_path = damaged / "imagery" / "scene-a" / "scene-a.json"
        item = _load(item_path)
        del item["properties"]["raster:spatial_resolution"]

        with pytest.raises(jsonschema.ValidationError):
            jsonschema.validate(item, _load(RASTER_SCHEMA))

    def test_the_profile_schema_pass_is_doing_work(self, damaged: Path) -> None:
        """``schema=True`` has to earn the flag.

        rashid leaves the profile pass off by default because it overlaps the
        metadata rules. It does not overlap them entirely: a malformed ``extent``
        is a shape the metadata rules never inspect, and PTL-SCH-001 rejects it.
        """
        collection_path = damaged / "roads" / "collection.json"
        collection = _load(collection_path)
        collection["extent"] = "not an object"
        collection_path.write_text(json.dumps(collection, indent=2), encoding="utf-8")

        fired = {f.rule_id for f in _validate(damaged).findings if f.severity is Severity.ERROR}

        assert "PTL-SCH-001" in fired

    def test_a_reinstated_items_link_is_visible(self, damaged: Path) -> None:
        """No rashid rule forbids the link, so the gate's own assertion is the guard.

        Proves ``test_no_items_link_and_no_stac_items_role`` is not vacuous: the
        thing it asserts against is expressible and would otherwise pass silently.
        """
        collection_path = damaged / "imagery" / "collection.json"
        collection = _load(collection_path)
        collection["links"].append(
            {"rel": "items", "href": "./items.parquet", "type": "application/vnd.apache.parquet"}
        )
        collection_path.write_text(json.dumps(collection, indent=2), encoding="utf-8")

        links = [link for link in _load(collection_path)["links"] if link.get("rel") == "items"]

        assert links != []
        assert _findings(damaged, Severity.ERROR) == [], (
            "rashid tolerates the legacy link, which is why the gate asserts on it directly"
        )
