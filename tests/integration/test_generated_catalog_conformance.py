"""Every catalog the CLI writes passes rashid, built through the real CLI.

Each SCENARIOS row pins every rashid result: known_errors by issue, advisories by count.
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
from rashid_core import validate
from rashid_core.api import LocalOnlyReader, RulesConfig, Severity, ValidationOptions

from tests.catalog_invariants import check_catalog_invariants
from tests.known_upstream import GPIO_WFS_OOM
from tests.property.test_rebuild_idempotence import shrink_geoparquet

if TYPE_CHECKING:
    from tests.catalog_invariants import RunCli
    from tests.conftest import CliHarness

pytestmark = pytest.mark.integration

FIXTURES = Path(__file__).resolve().parents[1] / "fixtures"
FIXTURE = FIXTURES / "simple.parquet"
RGB_COG = FIXTURES / "raster" / "valid" / "rgb.tif"
SINGLEBAND_COG = FIXTURES / "raster" / "valid" / "singleband.tif"
# Ten CRS84 points, GeoParquet 1.1. Converting the source GeoJSON in `add`
# tiles on Linux but writes nothing on macOS, so the gate skips that step.
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


# Builders


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
                # A mirror: producer differs from host, so collections and root owe a
                # rel:'via' link and an 'updated' stamp (PTL-PRV-001/002/003, PTL-PRO-001/003).
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
        # init keeps the metadata.yaml above; this flag only satisfies the CLI.
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

    PTL-DAT-015 asks for `extent.temporal` only when the data has a time dimension.
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
    # A three-level id, so the deeper catalog's parent differs from its root.
    # Flat ids hide a parent/root mix-up.
    quality = _copy(FIXTURE, root / "env" / "air" / "quality" / "quality.parquet")
    # A geometry-less Parquet exercises the tabular path, which writes its
    # collection.json outside finalize_items.
    census = _write_census(root / "demographics" / "census.parquet")
    # Two scenes: a lone scene trips PTL-COL-001. `add` skips a raster outside
    # the collection/item/file layout and still exits 0.
    scene_a = _copy(RGB_COG, root / "imagery" / "scene-a" / "rgb.tif")
    scene_b = _copy(SINGLEBAND_COG, root / "imagery" / "scene-b" / "singleband.tif")

    logo_source = root.parent / "brand.png"
    logo_source.write_bytes(LOGO_PNG)
    # PORTO-CORE-074..077 is a MAY, so plain init writes no icon link.
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
    """``add``, change the data and the providers, then ``add --force``.

    The extent must shrink, the dropped column must leave, and new providers reach both collections.
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
    """Register the README as a ``documentation`` asset, then rewrite it with ``readme``."""
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
    """Add one raster scene under an item id that differs from its directory."""
    scene_a = _copy(RGB_COG, root / "imagery" / "scene-a" / "rgb.tif")
    scene_b = _copy(SINGLEBAND_COG, root / "imagery" / "scene-b" / "singleband.tif")
    _init(root, run)
    _add(root, run, scene_b)
    run("add", "--portolan-dir", str(root), "--item-id", "custom-scene", str(scene_a))


def build_multi_file(root: Path, run: RunCli) -> None:
    """Put both Philadelphia GeoParquet files in one collection."""
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
    """Add two collections whose data crosses the antimeridian."""
    edge = _copy(ANTIMERIDIAN_GEOJSON, root / "edge" / "antimeridian.geojson")
    fieldmaps = _copy(
        REALDATA / "fieldmaps-boundaries.parquet", root / "boundaries" / "boundaries.parquet"
    )
    _init(root, run)
    _add(root, run, edge, fieldmaps)


def build_sibling_pmtiles(root: Path, run: RunCli) -> None:
    """Add a GeoParquet that has a PMTiles file with the same stem beside it."""
    points = _copy(POINTS, root / "points" / "points.parquet")
    _copy(SAMPLE_PMTILES, root / "points" / "points.pmtiles")
    _init(root, run)
    _add(root, run, points)


def build_iceberg(root: Path, run: RunCli) -> None:
    """Add a GeoParquet on the Iceberg backend, with the default SQLite catalog."""
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

    The AWS variables point at moto for the whole block, so a ``clone`` reads the same bucket.
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

    rashid checks the downloaded copy, so the gate sees what a bucket reader sees.
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


# The scenario table

# A design reason, not an issue: tests/docs/test_guardrail_exemptions.py
# lists it in DESIGN_REASONS.
SINGLE_COG_LAYOUT = "single COG layout: spec and CLI disagree (PTL-COL-001), maintainer decision"
# extract names no host: only its user knows who serves the catalog. PTL-SCH-001
# fires because the profile schema requires a host provider.
HOST_UNSET = {"PTL-PRV-002": "host unset by design", "PTL-SCH-001": "host unset by design"}

# Every collection is a mirror. PTL-PRO-002 is an advisory: a rel:'canonical'
# link matters only when the upstream publishes STAC.

ONE_MIRROR = {"PTL-PRO-002": 1}


def _undescribed(assets: int, columns: int = 0, too_few_rows: int = 0) -> dict[str, int]:
    """Count the undescribed-asset, undescribed-column and too-few-rows advisories.

    ``add`` writes no asset or column descriptions; small files hold too few rows.
    """
    counts = {"PTL-AST-010": assets, "PTL-DAT-020": columns, "PTL-DAT-006": too_few_rows}
    return {rule: count for rule, count in counts.items() if count}


# One vector `add` into a mirror collection.
ONE_VECTOR = {**ONE_MIRROR, **_undescribed(4, columns=1, too_few_rows=1)}

# The sample scene declares no nodata value, alpha band, or mask, so rashid asks
# for one (PTL-DAT-019, PORTO-FMT-073, a SHOULD). That is the data, not the CLI.
NO_NODATA = {"PTL-DAT-019": 1}


def _realdata_row(source: Path, *, too_few_rows: int = 0) -> Scenario:
    """A realdata file that ``add`` takes as it ships.

    ``add`` rewrites GeoParquet 1.0.0 files to 1.1, so PTL-DAT-012 passes.
    """
    return Scenario(
        _realdata(source),
        expected_advisories={
            **ONE_MIRROR,
            **_undescribed(4, columns=1, too_few_rows=too_few_rows),
        },
        marks=(pytest.mark.realdata,),
    )


SCENARIOS: dict[str, Scenario] = {
    "fresh": Scenario(
        build_fresh,
        expected_advisories={
            "PTL-PRO-002": 4,
            **_undescribed(21, columns=3, too_few_rows=3),
            **NO_NODATA,
        },
    ),
    "re-add": Scenario(
        build_re_add,
        expected_advisories={
            "PTL-PRO-002": 4,
            **_undescribed(21, columns=3, too_few_rows=3),
            **NO_NODATA,
        },
    ),
    "force-after-mutation": Scenario(
        build_force_after_mutation,
        # metadata.yaml reaches both collections, and both are mirrors.
        expected_advisories={"PTL-PRO-002": 2, **_undescribed(6, columns=2, too_few_rows=1)},
    ),
    "readme": Scenario(build_readme, expected_advisories=ONE_VECTOR),
    "item-id": Scenario(
        build_item_id,
        expected_advisories={**ONE_MIRROR, **_undescribed(11, too_few_rows=1), **NO_NODATA},
    ),
    "multi-file": Scenario(
        build_multi_file,
        expected_advisories={**ONE_MIRROR, **_undescribed(6, columns=2, too_few_rows=1)},
        marks=(pytest.mark.realdata,),
    ),
    "partitioned": Scenario(
        build_partitioned,
        known_errors={
            # `add` leaves partition:glob for `push`, so the local catalog has
            # none. Whether a local catalog needs it is an open spec question.
            "PTL-EXT-001": "#914",
            "PTL-PRT-001": "#914",
            "PTL-SCH-001": "#914",
            # `add` writes the partition glob as a `data` asset href, and
            # PORTO-FMT-065 says a data asset href MUST NOT contain `*`.
            "PTL-AST-012": "#1143",
        },
        expected_advisories={
            **ONE_MIRROR,
            **_undescribed(11, too_few_rows=1),
            # The partition asset names its files with a glob, and a glob has no
            # single size or checksum.
            "PTL-AST-003": 2,
            # One per partition item: rashid asks each item for table:columns.
            "PTL-DAT-017": 4,
        },
        marks=(pytest.mark.realdata,),
    ),
    "antimeridian": Scenario(
        build_antimeridian,
        expected_advisories={"PTL-PRO-002": 2, **_undescribed(10, columns=2, too_few_rows=2)},
        marks=(pytest.mark.realdata,),
    ),
    "sibling-pmtiles": Scenario(
        build_sibling_pmtiles,
        expected_advisories={
            **ONE_MIRROR,
            **_undescribed(7, columns=1, too_few_rows=1),
            # sample.pmtiles holds three points in San Francisco, New York, and
            # Chicago, so its bounds differ from points.parquet on purpose.
            "PTL-DAT-005": 1,
        },
    ),
    "iceberg": Scenario(build_iceberg, expected_advisories=ONE_VECTOR),
    "realdata-fieldmaps-boundaries": _realdata_row(
        REALDATA / "fieldmaps-boundaries.parquet", too_few_rows=1
    ),
    "realdata-nwi-wetlands": _realdata_row(REALDATA / "nwi-wetlands.parquet"),
    "realdata-open-buildings": _realdata_row(REALDATA / "open-buildings.parquet"),
    "realdata-road-detections": _realdata_row(REALDATA / "road-detections.parquet"),
    "realdata-rapidai4eo-sample": Scenario(
        _realdata(REALDATA / "rapidai4eo-sample.tif"),
        known_errors={
            # `add` needs an item directory and PTL-COL-001 forbids one for a
            # single file, so no layout of one COG passes both.
            "PTL-COL-001": SINGLE_COG_LAYOUT,
        },
        expected_advisories={**ONE_MIRROR, **_undescribed(7, too_few_rows=1), **NO_NODATA},
        marks=(pytest.mark.realdata,),
    ),
    "realdata-affordable-housing": _realdata_row(PHILADELPHIA / "affordable_housing.parquet"),
    "realdata-council-districts": _realdata_row(
        PHILADELPHIA / "council_districts_2024.parquet", too_few_rows=1
    ),
    # The GDAL autotest file declares no CRS on its one spatial layer, so `add`
    # refuses it. That refusal is correct, and the catalog stays empty.
    "realdata-field-alias-gdb": Scenario(
        _realdata(REALDATA / "filegdb" / "field_alias.gdb", expect=1),
        marks=(pytest.mark.realdata,),
    ),
    "extract-arcgis": Scenario(
        build_extract_arcgis,
        known_errors=HOST_UNSET,
        expected_advisories=_undescribed(10, columns=2, too_few_rows=1),
        marks=(pytest.mark.realdata,),
    ),
    "extract-wfs": Scenario(
        build_extract_wfs,
        known_errors=HOST_UNSET,
        expected_advisories=_undescribed(10, columns=2, too_few_rows=2),
        marks=(GPIO_WFS_OOM,),
    ),
    "extract-carto": Scenario(
        build_extract_carto,
        known_errors=HOST_UNSET,
        expected_advisories=_undescribed(4, columns=1, too_few_rows=1),
    ),
    "push": Scenario(build_push, expected_advisories=ONE_VECTOR),
    "clone": Scenario(build_clone, expected_advisories=ONE_VECTOR),
}


# Harness


@pytest.fixture(scope="module")
def build_scenario(
    tmp_path_factory: pytest.TempPathFactory, cli_harness: type[CliHarness]
) -> Builds:
    """Build each scenario once per module, in the first test that asks for it.

    A build runs in the test body so a strict xfail sees its failure; failures are cached.
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
            except BaseException as exc:  # cached and raised again
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
    """Run every offline rashid pass, the profile schema (PTL-SCH-001) included.

    ``LocalOnlyReader`` keeps data reads inside the catalog tree, so the gate makes no requests.
    """
    return validate(
        root,
        options=ValidationOptions(
            structural=True,
            schema=True,
            data=True,
            data_reader_factory=LocalOnlyReader,
            **kwargs,
        ),
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
    """Build a collection with three styles and PMTiles, apart because tippecanoe skips Windows.

    `--pmtiles` is the only offline route to styles; PTL-VIZ-006 needs two or more.
    """
    collection_dir = root / "places"
    (collection_dir / "styles").mkdir(parents=True)
    # tippecanoe needs two feature locations for a maxzoom, and the thumbnail
    # renderer needs a geographic CRS, so neither simple.parquet nor UTM fits.
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

    # Styles register even when tiling fails, so check the tiles first to
    # report the real defect.
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
        root, _ = build_scenario(name)

        assert _errors(root, SCENARIOS[name].known_errors) == []

    def test_known_gaps_are_still_real(self, name: str, build_scenario: Builds) -> None:
        root, _ = build_scenario(name)
        known = SCENARIOS[name].known_errors
        fired = {f.rule_id for f in _validate(root).findings}

        assert fired & set(known) == set(known)

    def test_no_unexpected_advisories(self, name: str, build_scenario: Builds) -> None:
        """A new advisory fails here, and a fixed one fails until its entry goes."""
        root, _ = build_scenario(name)
        row = SCENARIOS[name]
        observed = _rule_counts(root, Severity.WARNING, row.known_errors) + _rule_counts(
            root, Severity.INFO, row.known_errors
        )

        assert dict(observed) == dict(row.expected_advisories)

    def test_catalog_invariants_hold(self, name: str, build_scenario: Builds) -> None:
        """Each row pins its own codes, so a gap in one row cannot hide one in another."""
        _, roots = build_scenario(name)
        fired = {v.code for root in roots for v in check_catalog_invariants(root)}

        assert fired == set(SCENARIOS[name].known_invariants)

    def test_every_pass_actually_ran(self, name: str, build_scenario: Builds) -> None:
        """rashid reports a pass it cannot run as one ``-000`` warning, not a failure."""
        root, _ = build_scenario(name)
        skipped = sorted(f.rule_id for f in _validate(root).findings if f.rule_id.endswith("-000"))

        assert skipped == []


@pytest.mark.realdata
class TestPushedPartitionGlob:
    """``push`` writes the bulk-access glob of a partitioned collection."""

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
    """``add`` publishes a GeoParquet 1.0.0 realdata file as 1.1 with a bbox covering."""

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
        root, _ = build_scenario("realdata-field-alias-gdb")

        assert list(root.rglob("collection.json")) == []


class TestUnlicensedCatalogIsRefused:
    """``add`` writes no collection when metadata.yaml has no license."""

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
        """A ``license: "other"`` collection with no license link fires PTL-LIC-002."""
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
    """A full-SPDX-list license such as ``EUPL-1.2`` goes through generation and passes rashid."""

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
    """PTL-CNF-004 checks only the declared URI, so these tests read the schema itself."""

    def test_the_item_declares_raster_v2(self, generated_catalog: Path) -> None:
        item = _load(generated_catalog / "imagery" / "scene-a" / "scene-a.json")

        from portolan_cli.stac import EXTENSION_URLS

        assert EXTENSION_URLS["raster"] in item["stac_extensions"]

    def test_the_item_validates_against_that_schema(self, generated_catalog: Path) -> None:
        """raster v2.0.0 needs a ``raster:`` field in an item that declares it."""
        import jsonschema

        for scene in ("scene-a", "scene-b"):
            item = _load(generated_catalog / "imagery" / scene / f"{scene}.json")
            jsonschema.validate(item, _load(RASTER_SCHEMA))

    def test_a_raster_field_is_actually_present(self, generated_catalog: Path) -> None:
        """The schema binds only items that declare the extension, so check the field."""
        item = _load(generated_catalog / "imagery" / "scene-a" / "scene-a.json")
        raster_fields = [key for key in item["properties"] if key.startswith("raster:")]

        assert raster_fields == ["raster:spatial_resolution"]

    def test_the_data_asset_carries_the_cog_media_type(self, generated_catalog: Path) -> None:
        """PTL-AST-006."""
        item = _load(generated_catalog / "imagery" / "scene-a" / "scene-a.json")

        assert item["assets"]["data"]["type"] == COG_MEDIA_TYPE

    def test_the_collection_uses_proj_code_not_proj_epsg(self, generated_catalog: Path) -> None:
        """projection v2.0.0 deleted ``proj:epsg``."""
        collection = _load(generated_catalog / "imagery" / "collection.json")

        assert "proj:epsg" not in collection
        assert collection["summaries"]["proj:code"] == ["EPSG:4326"]


class TestTheItemMirrorHasItsCanonicalShape:
    """The item mirror is an asset alone; only the raster scenes give it items."""

    def test_the_mirror_is_generated_by_default(self, generated_catalog: Path) -> None:
        assert (generated_catalog / "imagery" / "items.parquet").is_file()

    def test_the_mirror_asset_is_the_whole_registration(self, generated_catalog: Path) -> None:
        collection = _load(generated_catalog / "imagery" / "collection.json")
        mirror = collection["assets"]["geoparquet-items"]

        assert mirror["href"] == "./items.parquet"
        assert mirror["type"] == "application/vnd.apache.parquet"
        assert mirror["roles"] == ["collection-mirror"]

    def test_no_items_link_and_no_stac_items_role(self, generated_catalog: Path) -> None:
        """Neither is in the spec, and no rashid rule catches either."""
        collection = _load(generated_catalog / "imagery" / "collection.json")

        assert [link for link in collection["links"] if link.get("rel") == "items"] == []
        for key, asset in collection["assets"].items():
            assert "stac-items" not in asset.get("roles", []), key

    def test_the_mirror_is_not_re_ingested_as_data(self, generated_catalog: Path) -> None:
        """items.parquet is a GeoParquet inside the collection, so a scan finds it."""
        collection = _load(generated_catalog / "imagery" / "collection.json")
        data_assets = {
            key for key, asset in collection["assets"].items() if "data" in asset.get("roles", [])
        }

        assert "geoparquet-items" not in data_assets


class TestTheCatalogLogoConforms:
    """PORTO-CORE-074..077 (PTL-LNK-007/008/009).

    The advisory counts in ``test_no_unexpected_advisories`` cover warnings 008 and 009.
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
        """core.md scopes the logo to the root, but rashid accepts one on any node."""
        for collection_json in generated_catalog.rglob("collection.json"):
            links = _load(collection_json)["links"]
            assert [link for link in links if link.get("rel") == "icon"] == [], collection_json


class TestThumbnailsSurviveARealPass:
    """Thumbnail shape, and the file extension wherever ``file:`` fields appear."""

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
        # A geometry-less collection is not geospatial, so a thumbnail would lie.
        collection = _load(generated_catalog / "demographics" / "collection.json")

        assert "thumbnail" not in collection["assets"]


class TestTheTabularCollectionDocumentsItsTable:
    """A tabular collection answers both Tabular Data SHOULDs.

    rashid skips an unreadable Parquet, so these read the emitted values.
    """

    def test_columns_land_on_the_collection(self, generated_catalog: Path) -> None:
        """``readme`` and ``metadata`` read ``table:columns`` from the collection."""

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
        """The bounds are the fixture's values, so a fallback to "now" or null fails."""
        collection = _load(generated_catalog / "demographics" / "collection.json")

        assert collection["extent"]["temporal"]["interval"] == [
            ["2020-04-01T00:00:00Z", "2020-09-30T00:00:00Z"]
        ]

    def test_the_geo_collections_keep_their_own_schema(self, generated_catalog: Path) -> None:
        """The tabular writer must not stamp ``table:columns`` on a geo collection."""
        collection = _load(generated_catalog / "roads" / "collection.json")

        assert "geometry" in {column["name"] for column in collection["table:columns"]}
        assert "tract_id" not in {column["name"] for column in collection["table:columns"]}


class TestNoUndefinedPortolanFieldsAreEmitted:
    """No ``portolan:`` field the spec does not define, which rashid's open schema allows."""

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
        """Catch a prefix in a value, and prove the profile URI still carries it."""
        catalog_text = (generated_catalog / "catalog.json").read_text(encoding="utf-8")

        assert self.SCHEMA_URI_PREFIX in catalog_text
        assert "portolan:" not in catalog_text.replace(self.SCHEMA_URI_PREFIX, "")


@pytest.mark.skipif(
    shutil.which("tippecanoe") is None,
    reason="tippecanoe not installed (required for PMTiles generation)",
)
class TestStylesAndPMTilesConform:
    """PMTiles and styles, the two paths a plain `add` never reaches."""

    def test_the_catalog_has_no_errors(self, visual_catalog: Path) -> None:
        assert _errors(visual_catalog) == []

    def test_exactly_one_style_is_the_default(self, visual_catalog: Path) -> None:
        """PORTO-CORE-070, a MUST once a collection registers more than one style."""
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
    """Each test puts back a removed defect and asserts the gate goes red."""

    @pytest.fixture
    def damaged(self, generated_catalog: Path, tmp_path: Path) -> Path:
        # A copy, not a CLI build, so the run_cli check never sees it.
        root = tmp_path / "damaged-catalog"
        shutil.copytree(generated_catalog, root)
        return root

    def test_a_stale_extension_version_fails(
        self, damaged: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """PTL-CNF-004 reports a raster version behind as a warning, so the count catches it.

        The stub fails the unpinned schema fetch, so PTL-EXT-000 fires offline everywhere.
        """
        import rashid_core.extensions

        def offline(uri: str) -> dict[str, Any]:
            raise OSError(f"offline test: {uri}")

        monkeypatch.setattr(rashid_core.extensions, "fetch_schema", offline)
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
        """The schema check reads the document that PTL-CNF-004 only names."""
        import jsonschema

        item_path = damaged / "imagery" / "scene-a" / "scene-a.json"
        item = _load(item_path)
        del item["properties"]["raster:spatial_resolution"]

        with pytest.raises(jsonschema.ValidationError):
            jsonschema.validate(item, _load(RASTER_SCHEMA))

    def test_the_profile_schema_pass_is_doing_work(self, damaged: Path) -> None:
        """Only PTL-SCH-001 rejects a malformed ``extent``, so ``schema=True`` adds coverage."""
        collection_path = damaged / "roads" / "collection.json"
        collection = _load(collection_path)
        collection["extent"] = "not an object"
        collection_path.write_text(json.dumps(collection, indent=2), encoding="utf-8")

        fired = {f.rule_id for f in _validate(damaged).findings if f.severity is Severity.ERROR}

        assert "PTL-SCH-001" in fired

    def test_a_reinstated_items_link_is_visible(self, damaged: Path) -> None:
        """No rashid rule forbids the link, so the gate asserts on it directly."""

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
