"""The conformance gate: every catalog the CLI writes passes rashid (#654, #746, #1033).

Each row of ``SCENARIOS`` builds a catalog the way a user does, through the
real CLI, and runs every offline pass rashid has over the result. The rows
cover the operations that cause drift: a fresh ``add``, the same ``add`` again,
``add --force`` after the data changes, ``readme``, ``--item-id``, a
multi-file collection, partitioning, the antimeridian, a sibling PMTiles, the
Iceberg backend, each realdata fixture, ``extract arcgis``, and ``push``.

A row pins every rashid result it produces. ``known_errors`` maps each rule
that fires because of an open bug to its issue, and
``test_known_gaps_are_still_real`` fails when one stops firing, so a fix
cannot land without deleting its entry. ``expected_advisories`` pins the
warnings and infos by exact count. Any other result fails the gate.

Every build runs through the ``run_cli`` harness, so the catalog invariants
in ``tests/catalog_invariants.py`` check each catalog too.

The gate pins warnings and infos as well as errors (#746). rashid reports a
stale extension version (PTL-CNF-004) as a WARNING, so a gate that read only
errors watched the raster extension sit at v1.1.0 and stayed green.
"""

from __future__ import annotations

import json
import shutil
import sys
import urllib.request
from collections import Counter
from collections.abc import Callable, Iterator, Mapping
from contextlib import contextmanager
from dataclasses import dataclass, field
from pathlib import Path
from typing import TYPE_CHECKING, Any

import pyarrow as pa
import pyarrow.parquet as pq
import pytest
import yaml
from rashid import RulesConfig, Severity, validate
from rashid.data.reader import LocalOnlyReader

from tests.catalog_invariants import check_catalog_invariants
from tests.known_upstream import GPIO_WFS_OOM
from tests.properties.test_rebuild_idempotence import shrink_geoparquet

if TYPE_CHECKING:
    from tests.catalog_invariants import RunCli
    from tests.conftest import CliHarness

pytestmark = pytest.mark.integration

FIXTURES = Path(__file__).resolve().parents[1] / "fixtures"
FIXTURE = FIXTURES / "simple.parquet"
RGB_COG = FIXTURES / "raster" / "valid" / "rgb.tif"
SINGLEBAND_COG = FIXTURES / "raster" / "valid" / "singleband.tif"
# Ten points in CRS84, GeoParquet 1.1. An earlier draft fed the GeoJSON these
# were converted from and let `add` convert it, which tiled on Linux and
# silently produced nothing on macOS. Handing the converted file over directly
# drops that hop and the platform split with it.
POINTS = FIXTURES / "vector" / "valid" / "points.parquet"
ANTIMERIDIAN_GEOJSON = FIXTURES / "edge" / "antimeridian.geojson"
SAMPLE_PMTILES = FIXTURES / "cloud_native" / "sample.pmtiles"
REALDATA = FIXTURES / "realdata"
PHILADELPHIA = REALDATA / "philadelphia-housing"
RASTER_SCHEMA = FIXTURES / "schemas" / "raster-v2.0.0.schema.json"

# A real 1x1 transparent PNG. rashid reads logo bytes, so magic bytes alone
# would not do.
LOGO_PNG = bytes.fromhex(
    "89504e470d0a1a0a0000000d4948445200000001000000010806000000"
    "1f15c4890000000a49444154789c630001000005000100"
    "0d0a2db40000000049454e44ae426082"
)

Builder = Callable[[Path, "RunCli"], None]
Builds = Callable[[str], tuple[Path, list[Path]]]


@dataclass(frozen=True)
class Scenario:
    """One row of the gate: how to build a catalog and what rashid reports on it."""

    build: Builder
    # Rules that fire because of an open bug, each mapped to its issue.
    known_errors: Mapping[str, str] = field(default_factory=dict)
    # Warnings and infos the catalog produces on purpose, by exact count.
    expected_advisories: Mapping[str, int] = field(default_factory=dict)
    # Catalog invariant codes that fire because of an open bug, each mapped to
    # its issue (tests/catalog_invariants.py).
    known_invariants: Mapping[str, str] = field(default_factory=dict)
    marks: tuple[pytest.MarkDecorator, ...] = ()


# ---------------------------------------------------------------------------
# Builders
# ---------------------------------------------------------------------------


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


def _init(root: Path, run: RunCli, *extra: str) -> None:
    """Write metadata.yaml, then run ``init --auto`` into ``root``."""
    _write_metadata(root)
    run(
        "init",
        str(root),
        "--auto",
        "--title",
        "Demo Catalog",
        "--description",
        "A catalog published for the conformance gate.",
        # init keeps the metadata.yaml written above, so this only satisfies the
        # flag's requirement; the license the catalog uses is the one in that file.
        "--license",
        "CC-BY-4.0",
        *extra,
    )


def _add(root: Path, run: RunCli, *paths: Path | str, expect: int | None = 0) -> None:
    run("add", "--portolan-dir", str(root), *(str(p) for p in paths), expect=expect)


def _enable_tabular(root: Path) -> None:
    config = root / ".portolan" / "config.yaml"
    config.write_text(config.read_text() + "tabular:\n  enabled: true\n", encoding="utf-8")


def _write_census(path: Path) -> Path:
    """Write a geometry-less Parquet with a timestamp column.

    The timestamp column is load-bearing (issue #749): PTL-DAT-015 asks a
    tabular collection for `table:columns` and, only when the data has a time
    dimension, for `extent.temporal`.
    """
    path.parent.mkdir(parents=True, exist_ok=True)
    pq.write_table(
        pa.table(
            {
                "tract_id": ["001", "002"],
                "population": [5000, 7500],
                "surveyed_at": pa.array(["2020-04-01", "2020-09-30"], pa.string()).cast(
                    pa.timestamp("us")
                ),
            }
        ),
        path,
    )
    return path


def _copy(source: Path, target: Path) -> Path:
    target.parent.mkdir(parents=True, exist_ok=True)
    shutil.copy(source, target)
    return target


def build_fresh(root: Path, run: RunCli) -> None:
    """Run init + add over a geo, a nested, a tabular, and a raster collection."""
    roads = _copy(FIXTURE, root / "roads" / "roads.parquet")
    # A three-level id, so generation has to write two intermediate catalogs and
    # the deeper one's parent differs from its root. Flat ids alone let #711
    # ship: root and parent coincide at the top level and the conflation between
    # them stays invisible.
    quality = _copy(FIXTURE, root / "env" / "air" / "quality" / "quality.parquet")
    # A geometry-less Parquet exercises the tabular path, which writes its
    # collection.json outside finalize_items.
    census = _write_census(root / "demographics" / "census.parquet")
    # Two raster scenes, because raster is the path #654's drift rode in on.
    # Two rather than one for a reason: a lone scene trips PTL-COL-001, which
    # asks a single-file collection to carry its data at collection level
    # instead of wrapping it in an item (see the realdata-rapidai4eo row).
    # Scenes also give the collection items, and items are what the mirror
    # needs. The collection/item/file layout is mandatory: `add` skips a raster
    # placed one level shallower, and still exits 0 while doing it.
    scene_a = _copy(RGB_COG, root / "imagery" / "scene-a" / "rgb.tif")
    scene_b = _copy(SINGLEBAND_COG, root / "imagery" / "scene-b" / "singleband.tif")

    logo_source = root.parent / "brand.png"
    logo_source.write_bytes(LOGO_PNG)
    # PORTO-CORE-074..077. A MAY, so plain init writes no icon link, which is
    # why the gate has to ask for one to cover it.
    _init(
        root,
        run,
        "--logo",
        str(logo_source),
        "--logo-title",
        "Example Municipal Open Data",
    )
    _enable_tabular(root)
    _add(root, run, roads, quality, census, scene_a, scene_b)
    # `add` reports a skipped raster on stdout and still exits 0, so the exit
    # code alone would not prove the scenes landed.
    assert (root / "imagery" / "collection.json").exists()


def _versions_bytes(root: Path) -> dict[str, bytes]:
    return {
        str(path.relative_to(root)): path.read_bytes()
        for path in sorted(root.rglob("versions.json"))
    }


def build_re_add(root: Path, run: RunCli) -> None:
    """Run the same ``add`` twice. The second run changes no version state."""
    build_fresh(root, run)
    before = _versions_bytes(root)
    _add(
        root,
        run,
        root / "roads" / "roads.parquet",
        root / "env" / "air" / "quality" / "quality.parquet",
        root / "demographics" / "census.parquet",
        root / "imagery" / "scene-a" / "rgb.tif",
        root / "imagery" / "scene-b" / "singleband.tif",
    )
    assert _versions_bytes(root) == before


def build_force_after_mutation(root: Path, run: RunCli) -> None:
    """``add``, then change the data and metadata.yaml, then ``add --force``.

    The geo file loses its first column and its north-east rows, so the
    extent must shrink (#801) and ``table:columns`` must lose the column
    (#931). The first ``add`` runs before metadata.yaml names any provider,
    so both collections get the placeholder provider. The providers arrive
    before ``add --force``, which must apply them to the tabular collection
    too (#813).
    """
    roads = _copy(POINTS, root / "roads" / "roads.parquet")
    census = _write_census(root / "demographics" / "census.parquet")
    _init(root, run)
    _enable_tabular(root)
    metadata = root / ".portolan" / "metadata.yaml"
    full = metadata.read_text(encoding="utf-8")
    without_providers = yaml.safe_load(full)
    del without_providers["providers"]
    metadata.write_text(yaml.dump(without_providers), encoding="utf-8")
    _add(root, run, roads, census)
    metadata.write_text(full, encoding="utf-8")
    shrink_geoparquet(roads, drop="id")
    pq.write_table(pq.read_table(census).drop_columns(["tract_id"]), census)
    run("add", "--force", "--portolan-dir", str(root), str(roads), str(census))


def build_readme(root: Path, run: RunCli) -> None:
    """Register the README as an asset, change metadata.yaml, then ``readme`` (#930).

    The first ``add`` writes ``roads/README.md``. ``add --force`` then finds
    it in the collection directory and registers it as the ``documentation``
    asset with its ``file:size`` and ``file:checksum``. ``readme`` rewrites
    the file after a description change.
    """
    roads = _copy(POINTS, root / "roads" / "roads.parquet")
    _init(root, run)
    _add(root, run, roads)
    run("add", "--force", "--portolan-dir", str(root), str(roads))
    metadata = root / ".portolan" / "metadata.yaml"
    loaded = yaml.safe_load(metadata.read_text(encoding="utf-8"))
    loaded["description"] = "Roads, described again after the first add."
    metadata.write_text(yaml.dump(loaded), encoding="utf-8")
    run("readme", cwd=root)


def build_item_id(root: Path, run: RunCli) -> None:
    """Add one raster scene under an item id that differs from its directory (#840)."""
    scene_a = _copy(RGB_COG, root / "imagery" / "scene-a" / "rgb.tif")
    scene_b = _copy(SINGLEBAND_COG, root / "imagery" / "scene-b" / "singleband.tif")
    _init(root, run)
    _add(root, run, scene_b)
    run("add", "--portolan-dir", str(root), "--item-id", "custom-scene", str(scene_a))


def build_multi_file(root: Path, run: RunCli) -> None:
    """Put both Philadelphia GeoParquet files in one collection (#932)."""
    housing = _copy(
        PHILADELPHIA / "affordable_housing.parquet", root / "housing" / "affordable_housing.parquet"
    )
    districts = _copy(
        PHILADELPHIA / "council_districts_2024.parquet",
        root / "housing" / "council_districts_2024.parquet",
    )
    _init(root, run)
    _add(root, run, housing, districts)


def build_partitioned(root: Path, run: RunCli) -> None:
    """Partition ``open-buildings.parquet`` with a threshold it exceeds."""
    buildings = _copy(REALDATA / "open-buildings.parquet", root / "buildings" / "buildings.parquet")
    _init(root, run)
    for key, value in (
        ("partitioning.enabled", "true"),
        ("partitioning.prompt", "false"),
        ("partitioning.threshold_gb", "0.0001"),
        ("partitioning.target_rows", "200"),
    ):
        run("config", "set", key, value, cwd=root)
    _add(root, run, buildings)


def build_antimeridian(root: Path, run: RunCli) -> None:
    """Add two collections whose data crosses the antimeridian (#852)."""
    edge = _copy(ANTIMERIDIAN_GEOJSON, root / "edge" / "antimeridian.geojson")
    fieldmaps = _copy(
        REALDATA / "fieldmaps-boundaries.parquet", root / "boundaries" / "boundaries.parquet"
    )
    _init(root, run)
    _add(root, run, edge, fieldmaps)


def build_sibling_pmtiles(root: Path, run: RunCli) -> None:
    """Add a GeoParquet that has a PMTiles file with the same stem beside it (#929)."""
    points = _copy(POINTS, root / "points" / "points.parquet")
    _copy(SAMPLE_PMTILES, root / "points" / "points.pmtiles")
    _init(root, run)
    _add(root, run, points)


def build_iceberg(root: Path, run: RunCli) -> None:
    """Add a GeoParquet on the Iceberg backend, with the default SQLite catalog (#939)."""
    pytest.importorskip("pyiceberg")
    if sys.platform == "win32":
        pytest.skip("PyIceberg SQL catalog warehouse paths break on Windows")
    roads = _copy(POINTS, root / "roads" / "roads.parquet")
    # The run_cli harness hides a developer's PyIceberg config.
    _init(root, run)
    run("config", "set", "backend", "iceberg", cwd=root)
    _add(root, run, roads)


def _realdata(source: Path, *, expect: int = 0) -> Builder:
    """Return a builder that adds one realdata file as it ships."""

    def build(root: Path, run: RunCli) -> None:
        if source.suffix == ".tif":
            # A raster needs the collection/item/file layout.
            target = root / "imagery" / "scene" / source.name
        else:
            target = root / source.stem / source.name
        if source.is_dir():
            shutil.copytree(source, target)
        else:
            _copy(source, target)
        _init(root, run)
        _add(root, run, target, expect=expect)

    return build


def _extract_recorded(
    root: Path, run: RunCli, scenario: str, kind: str, path: str, *args: str
) -> None:
    """Run ``extract`` against a recorded scenario of ``tests/replay_server.py``."""
    from tests.replay_server import ReplayServer

    server = ReplayServer(FIXTURES / "network" / scenario)
    with server as base_url:
        run("extract", kind, base_url + path, str(root), "--auto", "--license", "CC-BY-4.0", *args)
    assert server.unmatched == []


def build_extract_arcgis(root: Path, run: RunCli) -> None:
    """Extract both services of the recorded Philadelphia ArcGIS server."""
    _extract_recorded(root, run, "arcgis/philadelphia", "arcgis", "/ArcGIS/rest/services")


def build_extract_wfs(root: Path, run: RunCli) -> None:
    """Extract the two prefixed layers of the recorded GeoServer WFS."""
    _extract_recorded(root, run, "wfs/workspace_layer", "wfs", "/geoserver/wfs")


def build_extract_carto(root: Path, run: RunCli) -> None:
    """Extract three rows of the recorded Philadelphia CARTO table."""
    _extract_recorded(
        root,
        run,
        "carto/phl_zoning",
        "carto",
        "/api/v2/sql",
        "--tables",
        "zoning_basedistricts",
        "--limit",
        "3",
    )


def _roads_catalog(local: Path, run: RunCli) -> None:
    roads = _copy(POINTS, local / "roads" / "roads.parquet")
    _init(local, run)
    _add(local, run, roads)


@contextmanager
def _pushed(root: Path, run: RunCli, build: Builder = _roads_catalog) -> Iterator[Any]:
    """Build a catalog beside ``root``, push it to moto S3, and yield the S3 client.

    The AWS variables point at the moto server for the whole block, so a
    ``clone`` inside it reads the same bucket.
    """
    boto3 = pytest.importorskip("boto3")
    pytest.importorskip("moto")
    from moto.server import ThreadedMotoServer

    local = root.parent / "local"
    build(local, run)

    server = ThreadedMotoServer(ip_address="127.0.0.1", port=0, verbose=False)
    server.start()
    try:
        host, port = server.get_host_and_port()
        endpoint = f"http://{host}:{port}"
        client = boto3.client(
            "s3",
            endpoint_url=endpoint,
            aws_access_key_id="testing",
            aws_secret_access_key="testing",
            region_name="us-east-1",
        )
        # moto keeps its backends across servers in one process. A serial run,
        # without xdist otherwise clones the bucket of an earlier test.
        urllib.request.urlopen(
            urllib.request.Request(f"{endpoint}/moto-api/reset", method="POST"), timeout=30
        ).close()
        client.create_bucket(Bucket="gate")
        with pytest.MonkeyPatch.context() as env:
            env.setenv("AWS_ENDPOINT_URL", endpoint)
            env.setenv("AWS_ACCESS_KEY_ID", "testing")
            env.setenv("AWS_SECRET_ACCESS_KEY", "testing")
            env.setenv("AWS_DEFAULT_REGION", "us-east-1")
            env.setenv("AWS_ALLOW_HTTP", "true")
            run("push", "s3://gate/catalog", "--catalog", str(local))
            yield client
    finally:
        server.stop()


def build_push(root: Path, run: RunCli) -> None:
    """Push a catalog to moto S3, then download every object into ``root``.

    rashid checks the downloaded copy, so the gate sees what a reader of the
    bucket sees (#761, #807).
    """
    with _pushed(root, run) as client:
        listing = client.list_objects_v2(Bucket="gate", Prefix="catalog/")
        for entry in listing.get("Contents", []):
            target = root / entry["Key"].removeprefix("catalog/")
            target.parent.mkdir(parents=True, exist_ok=True)
            client.download_file("gate", entry["Key"], str(target))
    assert (root / "catalog.json").is_file()


def build_clone(root: Path, run: RunCli) -> None:
    """Push a catalog to moto S3, then ``clone`` it into ``root``."""
    with _pushed(root, run):
        run("clone", "s3://gate/catalog", str(root))
    assert (root / "catalog.json").is_file()


# ---------------------------------------------------------------------------
# The scenario table
# ---------------------------------------------------------------------------

# A design reason, not an issue: tests/docs/test_guardrail_exemptions.py
# lists it in DESIGN_REASONS.
SINGLE_COG_LAYOUT = "single COG layout: spec and CLI disagree (PTL-COL-001), maintainer decision"
# extract names the producer from the source attribution, and no host. Only the
# person who runs extract knows the organization that serves the catalog, and a
# seeded placeholder is never published as one (#1039). PTL-SCH-001 fires
# because the profile schema requires a host provider.
HOST_UNSET = {"PTL-PRV-002": "host unset by design", "PTL-SCH-001": "host unset by design"}

# Every collection is a mirror, because metadata.yaml names a producer
# distinct from the host. PTL-PRO-002 is an advisory: a rel:'canonical' link is
# only meaningful when the upstream publishes STAC.
ONE_MIRROR = {"PTL-PRO-002": 1}


def _realdata_row(source: Path) -> Scenario:
    """A realdata file that ``add`` takes as it ships.

    The four older Parquet files declare GeoParquet 1.0.0, which PTL-DAT-012
    rejects. ``add`` rewrites each to GeoParquet 1.1 with a bbox covering
    column, so the published file passes (TestRealdataGeoParquetIsRewritten).
    """
    return Scenario(
        _realdata(source), expected_advisories=ONE_MIRROR, marks=(pytest.mark.realdata,)
    )


SCENARIOS: dict[str, Scenario] = {
    "fresh": Scenario(build_fresh, expected_advisories={"PTL-PRO-002": 4}),
    "re-add": Scenario(build_re_add, expected_advisories={"PTL-PRO-002": 4}),
    "force-after-mutation": Scenario(
        build_force_after_mutation,
        # metadata.yaml now reaches both collections (#813), and it names a
        # producer distinct from the host, so both are mirrors.
        expected_advisories={"PTL-PRO-002": 2},
    ),
    "readme": Scenario(build_readme, expected_advisories=ONE_MIRROR),
    "item-id": Scenario(build_item_id, expected_advisories={"PTL-PRO-002": 1}),
    "multi-file": Scenario(
        build_multi_file,
        # rashid compares each collection-level asset with extent.spatial.bbox[0]
        # and never reads an asset-level bbox, so no CLI change clears this.
        known_errors={"PTL-DAT-005": "#932"},
        expected_advisories=ONE_MIRROR,
        marks=(pytest.mark.realdata,),
    ),
    "partitioned": Scenario(
        build_partitioned,
        known_errors={
            # `add` leaves partition:glob for `push` to write (add.py:353,
            # push.py:296), so the local catalog has none. Whether a local
            # catalog carries the glob at all is the open spec question #914.
            "PTL-EXT-001": "#914",
            "PTL-PRT-001": "#914",
            "PTL-SCH-001": "#914",
        },
        expected_advisories={
            **ONE_MIRROR,
            # The partition asset names its files with a glob, and a glob has no
            # single size or checksum. #914 holds the spec question.
            "PTL-AST-003": 2,
            # One per partition item: rashid asks each item for table:columns.
            "PTL-DAT-017": 4,
        },
        marks=(pytest.mark.realdata,),
    ),
    "antimeridian": Scenario(
        build_antimeridian,
        expected_advisories={"PTL-PRO-002": 2},
        marks=(pytest.mark.realdata,),
    ),
    "sibling-pmtiles": Scenario(
        build_sibling_pmtiles,
        expected_advisories={
            **ONE_MIRROR,
            # sample.pmtiles holds three points in San Francisco, New York, and
            # Chicago, so its bounds differ from points.parquet on purpose.
            "PTL-DAT-005": 1,
        },
    ),
    "iceberg": Scenario(
        build_iceberg,
        expected_advisories={
            **ONE_MIRROR,
            # The incubating Iceberg extension is not in rashid's registry.
            "PTL-EXT-002": 1,
        },
    ),
    "realdata-fieldmaps-boundaries": _realdata_row(REALDATA / "fieldmaps-boundaries.parquet"),
    "realdata-nwi-wetlands": _realdata_row(REALDATA / "nwi-wetlands.parquet"),
    "realdata-open-buildings": _realdata_row(REALDATA / "open-buildings.parquet"),
    "realdata-road-detections": _realdata_row(REALDATA / "road-detections.parquet"),
    "realdata-rapidai4eo-sample": Scenario(
        _realdata(REALDATA / "rapidai4eo-sample.tif"),
        known_errors={
            # `add` refuses a raster outside an item directory, and PTL-COL-001
            # asks a single-file collection to carry its data at collection
            # level. No layout of one COG passes both. The maintainer decides.
            "PTL-COL-001": SINGLE_COG_LAYOUT,
        },
        expected_advisories={"PTL-PRO-002": 1},
        marks=(pytest.mark.realdata,),
    ),
    "realdata-affordable-housing": _realdata_row(PHILADELPHIA / "affordable_housing.parquet"),
    "realdata-council-districts": _realdata_row(PHILADELPHIA / "council_districts_2024.parquet"),
    # The GDAL autotest file declares no CRS on its one spatial layer, so `add`
    # refuses it. That refusal is correct, and the catalog stays empty.
    "realdata-field-alias-gdb": Scenario(
        _realdata(REALDATA / "filegdb" / "field_alias.gdb", expect=1),
        marks=(pytest.mark.realdata,),
    ),
    "extract-arcgis": Scenario(
        build_extract_arcgis,
        known_errors=HOST_UNSET,
        marks=(pytest.mark.realdata,),
    ),
    "extract-wfs": Scenario(
        build_extract_wfs,
        known_errors=HOST_UNSET,
        marks=(GPIO_WFS_OOM,),
    ),
    "extract-carto": Scenario(
        build_extract_carto,
        known_errors=HOST_UNSET,
    ),
    "push": Scenario(build_push, expected_advisories=ONE_MIRROR),
    "clone": Scenario(build_clone, expected_advisories=ONE_MIRROR),
}


# ---------------------------------------------------------------------------
# Harness
# ---------------------------------------------------------------------------


@pytest.fixture(scope="module")
def build_scenario(
    tmp_path_factory: pytest.TempPathFactory, cli_harness: type[CliHarness]
) -> Builds:
    """Build each scenario once per module, in the test that first asks for it.

    The build runs in the test body, not in fixture setup, so a strict xfail
    on a row sees a failed build as the expected failure. A failed build is
    cached and raised again for the next test of the row. The result is the
    root that rashid reads and every catalog root that the build wrote.
    """
    built: dict[str, tuple[Path, list[Path]] | BaseException] = {}

    def _build(name: str) -> tuple[Path, list[Path]]:
        if name not in built:
            base = tmp_path_factory.mktemp(f"conformance-{name}")
            harness = cli_harness(base)
            root = base / "catalog"
            try:
                SCENARIOS[name].build(root, harness)
                built[name] = (root, sorted(harness.roots | {root.resolve()}))
            except BaseException as exc:  # noqa: BLE001 - cached and raised again
                built[name] = exc
        result = built[name]
        if isinstance(result, BaseException):
            raise result
        return result

    return _build


@pytest.fixture(scope="module")
def generated_catalog(build_scenario: Builds) -> Path:
    """The ``fresh`` scenario, which the shape tests below read."""
    return build_scenario("fresh")[0]


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
    that probes hosts by design. The extension pass still fetches a registered
    extension at a version rashid does not pin. A test that declares one stubs
    ``rashid.extensions._fetch_schema``.
    """
    return validate(
        root,
        structural=True,
        schema=True,
        data=True,
        data_reader_factory=LocalOnlyReader,
        **kwargs,
    )


def _findings(root: Path, known: Mapping[str, str] | None = None) -> list[Any]:
    """Return rashid findings with the known rules disabled."""
    report = _validate(root, config=RulesConfig(disabled=frozenset(known or {})))
    return list(report.findings)


def _rule_counts(
    root: Path, severity: Severity, known: Mapping[str, str] | None = None
) -> Counter[str]:
    """Count findings by rule id at one severity, with the known rules disabled."""
    return Counter(f.rule_id for f in _findings(root, known) if f.severity is severity)


def _errors(root: Path, known: Mapping[str, str] | None = None) -> list[dict[str, Any]]:
    return [f.to_dict() for f in _findings(root, known) if f.severity is Severity.ERROR]


def _load(path: Path) -> dict[str, Any]:
    loaded: dict[str, Any] = json.loads(path.read_text(encoding="utf-8"))
    return loaded


def _scenario_params() -> list[Any]:
    return [pytest.param(name, marks=row.marks, id=name) for name, row in SCENARIOS.items()]


def _build_visual_catalog(root: Path, run: RunCli) -> None:
    """Build a collection carrying three styles and PMTiles.

    Separate from the main catalog because PMTiles needs tippecanoe, which CI
    installs on Linux and macOS but not Windows. Keeping it apart lets the core
    gate run everywhere.

    `add` never registers style assets on its own. `register_style_assets` has
    exactly two callers, the end of PMTiles generation and the extract
    orchestrator, so `--pmtiles` is the only offline route to a styled
    collection. Two hand-written styles plus the generated `styles/default`
    make three, and three is the point: PTL-VIZ-006 short-circuits below two
    styles, so a smaller collection would never test the default-role MUST.
    """
    collection_dir = root / "places"
    (collection_dir / "styles").mkdir(parents=True)
    # Not simple.parquet: tippecanoe needs two distinct feature locations to
    # guess a maxzoom, and simple.parquet holds one point. Not the UTM buildings
    # fixture either, because the thumbnail renderer returns None on a projected
    # CRS and the collection would then fail PTL-VIZ-001.
    shutil.copy(POINTS, collection_dir / "places.parquet")
    for name, title in (("aqua", "Aqua"), ("zebra", "Zebra")):
        (collection_dir / "styles" / f"{name}.json").write_text(
            json.dumps({"version": 8, "name": title, "sources": {}, "layers": []}),
            encoding="utf-8",
        )
    _init(root, run)
    result = run(
        "add", "--portolan-dir", str(root), "--pmtiles", str(collection_dir / "places.parquet")
    )

    # Fail here, loudly, rather than three assertion errors down the file.
    # `register_style_assets` runs at the end of PMTiles generation, outside the
    # per-asset try block, so a collection can come back with its hand-written
    # styles registered and no tiles at all. That shape reads as "the default
    # style is missing" and "there is no tiles asset" instead of "tiling did not
    # run", which is what actually happened.
    assert (collection_dir / "places.pmtiles").is_file(), (
        f"tippecanoe produced no tiles, so the styles and PMTiles assertions "
        f"below would report the wrong defect. add output:\n{result.output}"
    )


@pytest.fixture(scope="module")
def visual_catalog(tmp_path_factory: pytest.TempPathFactory, cli_harness: type[CliHarness]) -> Path:
    base = tmp_path_factory.mktemp("conformance-visual")
    harness = cli_harness(base)
    root = base / "visual-catalog"
    _build_visual_catalog(root, harness)
    harness.verify()
    return root


@pytest.mark.parametrize("name", _scenario_params())
class TestScenarioConformance:
    """Each scenario passes rashid except for the rules its open bugs explain."""

    def test_no_errors(self, name: str, build_scenario: Builds) -> None:
        """rashid reports no error outside the scenario's known errors."""
        root, _ = build_scenario(name)

        assert _errors(root, SCENARIOS[name].known_errors) == []

    def test_known_gaps_are_still_real(self, name: str, build_scenario: Builds) -> None:
        """Each known error still fires, so the list cannot rot silently."""
        root, _ = build_scenario(name)
        known = SCENARIOS[name].known_errors
        fired = {f.rule_id for f in _validate(root).findings}

        assert fired & set(known) == set(known)

    def test_no_unexpected_advisories(self, name: str, build_scenario: Builds) -> None:
        """Warnings and infos are pinned by exact count, not discarded.

        A new advisory fails here, and a fixed one fails too until its entry goes.
        """
        root, _ = build_scenario(name)
        row = SCENARIOS[name]
        observed = _rule_counts(root, Severity.WARNING, row.known_errors) + _rule_counts(
            root, Severity.INFO, row.known_errors
        )

        assert dict(observed) == dict(row.expected_advisories)

    def test_catalog_invariants_hold(self, name: str, build_scenario: Builds) -> None:
        """The invariants that fire are exactly the row's known ones.

        The row pins the codes itself instead of the global
        ``KNOWN_INVARIANT_GAPS``, so a gap in one row cannot hide the same code
        in another.
        """
        _, roots = build_scenario(name)
        fired = {v.code for root in roots for v in check_catalog_invariants(root)}

        assert fired == set(SCENARIOS[name].known_invariants)

    def test_every_pass_actually_ran(self, name: str, build_scenario: Builds) -> None:
        """No pass silently skipped itself.

        rashid degrades a pass it cannot run to a single ``-000`` warning rather
        than failing the run. A gate that counts only rule findings cannot tell a
        clean data pass from an absent one, so assert the absence directly.
        """
        root, _ = build_scenario(name)
        skipped = sorted(f.rule_id for f in _validate(root).findings if f.rule_id.endswith("-000"))

        assert skipped == []


@pytest.mark.realdata
class TestPushedPartitionGlob:
    """``push`` writes the bulk-access glob of a partitioned collection (#761)."""

    def test_the_remote_glob_names_the_scheme_and_bucket(
        self, tmp_path: Path, run_cli: RunCli
    ) -> None:
        with _pushed(tmp_path / "remote", run_cli, build_partitioned) as client:
            body = client.get_object(Bucket="gate", Key="catalog/buildings/collection.json")["Body"]
            collection = json.loads(body.read())

        # push.py writes the glob onto each asset whose href is a glob.
        globs = [
            asset.get("partition:glob")
            for asset in collection["assets"].values()
            if "*" in asset["href"]
        ]

        assert globs == ["s3://gate/catalog/buildings/kdtree_cell=*/*.parquet"]


@pytest.mark.realdata
class TestRealdataGeoParquetIsRewritten:
    """What ``add`` does with a realdata file that declares GeoParquet 1.0.0 (#1033).

    PTL-DAT-012 rejects 1.0.0. The gate does not skip these files: it pins
    that ``add`` publishes them as GeoParquet 1.1 with a bbox covering column.
    """

    @pytest.mark.parametrize(
        ("name", "stem"),
        [
            ("realdata-fieldmaps-boundaries", "fieldmaps-boundaries"),
            ("realdata-open-buildings", "open-buildings"),
            ("realdata-road-detections", "road-detections"),
        ],
    )
    def test_the_published_file_is_geoparquet_1_1(
        self, name: str, stem: str, build_scenario: Builds
    ) -> None:
        source = json.loads(pq.read_schema(REALDATA / f"{stem}.parquet").metadata[b"geo"])
        root, _ = build_scenario(name)
        published = pq.read_schema(root / stem / f"{stem}.parquet")
        geo = json.loads(published.metadata[b"geo"])

        assert source["version"] == "1.0.0"
        assert geo["version"] == "1.1.0"
        assert geo["columns"]["geometry"]["covering"]["bbox"]["xmin"] == ["bbox", "xmin"]

    def test_add_refuses_the_gdb_with_no_crs(self, build_scenario: Builds) -> None:
        """The refusal leaves the catalog without a collection."""
        root, _ = build_scenario("realdata-field-alias-gdb")

        assert list(root.rglob("collection.json")) == []


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

    def test_add_refuses_and_writes_no_collection(self, tmp_path: Path, run_cli: RunCli) -> None:
        root = tmp_path / "unlicensed"
        source = self._catalog_without_a_license(root)

        result = run_cli("add", "--portolan-dir", str(root), str(source), expect=1)

        assert "PRTLN-VAL004" in result.output
        assert list(root.rglob("collection.json")) == []

    def test_the_violation_it_prevents_is_real(self, tmp_path: Path, run_cli: RunCli) -> None:
        """Prove PTL-LIC-002 is what the gate averts, not a rule we assume exists.

        Takes a conformant generated catalog and puts back exactly what the old
        code emitted: ``license: "other"`` and no ``rel="license"`` link. Without
        this, the test above would keep passing even if the rule it protects
        against were renamed or dropped upstream.
        """
        # The license is outside the catalog invariants, so run_cli checks
        # this catalog at teardown and finds nothing.
        root = tmp_path / "damaged-catalog"
        build_fresh(root, run_cli)
        collection_path = root / "roads" / "collection.json"

        collection = json.loads(collection_path.read_text(encoding="utf-8"))
        collection["license"] = "other"
        collection["links"] = [link for link in collection["links"] if link.get("rel") != "license"]
        collection_path.write_text(json.dumps(collection, indent=2), encoding="utf-8")

        fired = {f.rule_id for f in _findings(root) if f.severity is Severity.ERROR}

        assert fired == {"PTL-LIC-002"}


class TestAnIdentifierOutsideThePopularShortlistSurvivesGeneration:
    """Issue #727: the two commands disagreed on real SPDX identifiers.

    ``metadata validate`` judged the license against a hand-written 26-entry
    subset while ``check`` used rashid's full list, so ``EUPL-1.2`` was rejected
    by one and accepted by the other. Proving agreement on a dict is not enough;
    this runs the identifier through generation and validates the output.
    """

    def test_eupl_reaches_the_collection_and_conforms(
        self, tmp_path: Path, run_cli: RunCli
    ) -> None:
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

        run_cli("add", "--portolan-dir", str(root), str(collection_dir / "roads.parquet"))

        collection = json.loads((collection_dir / "collection.json").read_text(encoding="utf-8"))
        assert collection["license"] == "EUPL-1.2"

        license_findings = [f.rule_id for f in _findings(root) if f.rule_id.startswith("PTL-LIC")]
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


class TestTheTabularCollectionDocumentsItsTable:
    """#749: a tabular collection answers both Tabular Data SHOULDs.

    The advisory counts above prove PTL-DAT-015 stopped firing. These read the
    emitted bytes instead, because a rule can go quiet for the wrong reason —
    an unreadable Parquet makes rashid skip the check entirely. Asserting the
    column names and the derived interval pins the values, not just the silence.
    """

    def test_columns_land_on_the_collection(self, generated_catalog: Path) -> None:
        """The collection is the table, so its schema belongs at collection level.

        ``readme`` and ``metadata`` read ``table:columns`` from here, not from
        the asset, so a schema parked on the asset would be invisible to both.
        """
        collection = _load(generated_catalog / "demographics" / "collection.json")

        assert [column["name"] for column in collection["table:columns"]] == [
            "tract_id",
            "population",
            "surveyed_at",
        ]
        assert [column["type"] for column in collection["table:columns"]] == [
            "string",
            "int64",
            "timestamp[us]",
        ]
        assert collection["table:row_count"] == 2
        assert "table:primary_geometry" not in collection

    def test_the_table_extension_is_declared(self, generated_catalog: Path) -> None:
        collection = _load(generated_catalog / "demographics" / "collection.json")

        assert (
            "https://stac-extensions.github.io/table/v1.2.0/schema.json"
            in collection["stac_extensions"]
        )

    def test_the_temporal_extent_comes_from_the_timestamp_column(
        self, generated_catalog: Path
    ) -> None:
        """Read from Parquet column statistics, not from the clock.

        The bounds are the fixture's own values, so a fallback to "now" or to
        the sentinel ``[[null, null]]`` fails here rather than passing quietly.
        """
        collection = _load(generated_catalog / "demographics" / "collection.json")

        assert collection["extent"]["temporal"]["interval"] == [
            ["2020-04-01T00:00:00Z", "2020-09-30T00:00:00Z"]
        ]

    def test_the_geo_collections_keep_their_own_schema(self, generated_catalog: Path) -> None:
        """The tabular writer must not reach a collection it does not own.

        Both writers stamp ``table:columns`` at collection level, so a geo
        collection is where a mistargeted tabular stamp would surface.
        """
        collection = _load(generated_catalog / "roads" / "collection.json")

        assert "geometry" in {column["name"] for column in collection["table:columns"]}
        assert "tract_id" not in {column["name"] for column in collection["table:columns"]}


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
        assert _errors(visual_catalog) == []

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
        # A copy, not a CLI build, so the run_cli check never sees it.
        root = tmp_path / "damaged-catalog"
        shutil.copytree(generated_catalog, root)
        return root

    def test_a_stale_extension_version_fails(
        self, damaged: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """The #654 drift itself: raster pinned a major version behind.

        This is the case an error-only gate could not see. PTL-CNF-004 reports it
        as a warning, deliberately — a catalog published before the registry moved
        is behind, not broken — so the count assertion is what catches it.

        rashid fetches the schema of a registered extension at a version it does
        not pin. The stub makes that fetch fail, so the extension pass reports
        PTL-EXT-000 on every machine and the test stays offline.
        """
        import rashid.extensions

        def offline(uri: str) -> dict[str, Any]:
            raise OSError(f"offline test: {uri}")

        monkeypatch.setattr(rashid.extensions, "_fetch_schema", offline)
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
        assert advisories["PTL-EXT-000"] == 1
        assert dict(advisories) != dict(SCENARIOS["fresh"].expected_advisories)

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
        assert _errors(damaged) == [], (
            "rashid tolerates the legacy link, which is why the gate asserts on it directly"
        )
