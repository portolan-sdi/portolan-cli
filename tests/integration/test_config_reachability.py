"""Each documented setting reaches the code that it controls (#1020, #834).

A test sets one key, runs the command that reads it, and reads the effect
from the output files. It sets the key in three forms:

- ``config-set``: ``portolan config set <key> <value>``, the documented command.
- ``yaml``: the nested block in ``.portolan/config.yaml``, the documented file form.
- ``env``: the ``PORTOLAN_<KEY>`` environment variable, which overrides the file.

The forms separate three defects. A setting that works in the ``yaml``
form and fails in the ``config-set`` form has a broken ``config set``. A
setting that fails in the ``env`` form has a reader that skips the
environment. A setting that fails in every form has a reader that ignores it.

Every key in ``KNOWN_SETTINGS`` is tested here or listed as a
``config-key`` line in ``tests/guardrail_exemptions.txt`` with the reason
that no output shows it. ``test_every_known_setting_is_covered`` enforces it.
"""

from __future__ import annotations

import json
import os
import re
import shutil
import sys
from pathlib import Path
from typing import TYPE_CHECKING, Any
from unittest import mock

import pyarrow.parquet as pq
import pytest
import yaml

from portolan_cli.config import KNOWN_SETTINGS
from tests.catalog_invariants import load_exemptions

if TYPE_CHECKING:
    from collections.abc import Callable, Iterator

    from tests.catalog_invariants import RunCli

pytestmark = [pytest.mark.integration]

FIXTURES = Path(__file__).resolve().parents[1] / "fixtures"
POINTS_GEOJSON = FIXTURES / "vector" / "valid" / "points.geojson"
POINTS_PARQUET = FIXTURES / "vector" / "valid" / "points.parquet"
SINGLEBAND_COG = FIXTURES / "raster" / "valid" / "singleband.tif"
RGB_COG = FIXTURES / "raster" / "valid" / "rgb.tif"
NON_COG = FIXTURES / "raster" / "natural-earth-non-cog.tif"
OPEN_BUILDINGS = FIXTURES / "realdata" / "open-buildings.parquet"
PHILADELPHIA = FIXTURES / "realdata" / "philadelphia-housing"

FORMS = ("config-set", "yaml", "env")

# Key -> the test that observes it.
TESTED_KEYS: dict[str, str] = {}


def _tests(*keys: str) -> Callable[[Callable[..., None]], Callable[..., None]]:
    """Record that the decorated test observes ``keys``."""

    def register(test: Callable[..., None]) -> Callable[..., None]:
        for key in keys:
            TESTED_KEYS[key] = test.__name__
        return test

    return register


def _forms(**xfail: str) -> pytest.MarkDecorator:
    """Parametrize ``form``, with a strict xfail on the forms named in ``xfail``.

    ``_forms(config_set="#N: reason")`` marks the ``config-set`` form.
    """
    params = []
    for form in FORMS:
        reason = xfail.get(form.replace("-", "_"))
        marks = [pytest.mark.xfail(strict=True, reason=reason)] if reason else []
        params.append(pytest.param(form, marks=marks, id=form))
    return pytest.mark.parametrize("form", params)


@pytest.fixture
def catalog(tmp_path: Path, run_cli: RunCli) -> Iterator[Path]:
    """A catalog from ``init --auto``. The ``env`` form variables go at teardown."""
    root = tmp_path / "catalog"
    root.mkdir()
    run_cli("init", str(root), "--auto", "--license", "CC-BY-4.0")
    with mock.patch.dict(os.environ):
        yield root


def _nest(config: dict[str, Any], key: str, value: Any) -> None:
    *parents, leaf = key.split(".")
    node = config
    for part in parents:
        node = node.setdefault(part, {})
    node[leaf] = value


def configure(run_cli: RunCli, root: Path, form: str, settings: dict[str, tuple[str, Any]]) -> None:
    """Apply ``settings`` (key -> (command-line text, YAML value)) in ``form``."""
    if form == "env":
        for key, (text, _) in settings.items():
            os.environ["PORTOLAN_" + re.sub(r"[^A-Za-z0-9]", "_", key).upper()] = text
        return
    if form == "config-set":
        for key, (text, _) in settings.items():
            run_cli("config", "set", key, text, cwd=root)
        return
    path = root / ".portolan" / "config.yaml"
    config = yaml.safe_load(path.read_text(encoding="utf-8")) or {}
    for key, (_, value) in settings.items():
        _nest(config, key, value)
    path.write_text(yaml.safe_dump(config), encoding="utf-8")


def _copy(source: Path, target: Path) -> Path:
    target.parent.mkdir(parents=True, exist_ok=True)
    if source.is_dir():
        shutil.copytree(source, target)
    else:
        shutil.copy(source, target)
    return target


def _load(path: Path) -> dict[str, Any]:
    loaded: dict[str, Any] = json.loads(path.read_text(encoding="utf-8"))
    return loaded


def _add(run_cli: RunCli, root: Path, *paths: Path) -> None:
    run_cli("add", "--portolan-dir", str(root), *(str(p) for p in paths))


def _band_statistics(item: Path) -> list[Any]:
    return [band.get("statistics") for band in _load(item)["assets"]["data"]["bands"]]


# ---------------------------------------------------------------------------
# statistics.*
# ---------------------------------------------------------------------------


@_tests("statistics.enabled")
@_forms()
def test_statistics_enabled_false_writes_no_band_statistics(
    form: str, catalog: Path, run_cli: RunCli
) -> None:
    scene = _copy(SINGLEBAND_COG, catalog / "imagery" / "scene" / "scene.tif")
    configure(run_cli, catalog, form, {"statistics.enabled": ("false", False)})

    _add(run_cli, catalog, scene)

    assert _band_statistics(catalog / "imagery" / "scene" / "scene.json") == [None]


@_tests("statistics.raster_mode")
@_forms()
def test_statistics_raster_mode_exact_reads_every_pixel(
    form: str, catalog: Path, run_cli: RunCli
) -> None:
    """The approximate statistics read an overview. The exact ones read the full raster."""
    scene = _copy(NON_COG, catalog / "imagery" / "scene" / "scene.tif")
    configure(run_cli, catalog, form, {"statistics.raster_mode": ("exact", "exact")})

    _add(run_cli, catalog, scene)

    assert _band_statistics(catalog / "imagery" / "scene" / "scene.json") == EXACT_STATISTICS


# Measured on natural-earth-non-cog.tif, one band. The approximate mode reads
# an overview, so its mean and stddev differ from the full read in the fourth
# significant digit. The relative tolerance absorbs float noise across GDAL
# builds and stays far below that gap.
EXACT_STATISTICS = [
    {
        "minimum": 53.0,
        "maximum": 249.0,
        "mean": pytest.approx(204.917273, rel=1e-7),
        "stddev": pytest.approx(10.830305777930326, rel=1e-7),
    }
]
APPROX_STATISTICS = [
    {
        "minimum": 53.0,
        "maximum": 249.0,
        "mean": pytest.approx(204.915208, rel=1e-7),
        "stddev": pytest.approx(10.819868128435576, rel=1e-7),
    }
]


def test_the_default_raster_mode_is_approximate(catalog: Path, run_cli: RunCli) -> None:
    """Pin the default, so the exact test above cannot pass by accident."""
    scene = _copy(NON_COG, catalog / "imagery" / "scene" / "scene.tif")

    _add(run_cli, catalog, scene)

    assert _band_statistics(catalog / "imagery" / "scene" / "scene.json") == APPROX_STATISTICS


# ---------------------------------------------------------------------------
# parquet.enabled
# ---------------------------------------------------------------------------


@_tests("parquet.enabled")
@_forms()
def test_parquet_enabled_false_writes_no_item_mirror(
    form: str, catalog: Path, run_cli: RunCli
) -> None:
    scene_a = _copy(RGB_COG, catalog / "imagery" / "scene-a" / "rgb.tif")
    scene_b = _copy(SINGLEBAND_COG, catalog / "imagery" / "scene-b" / "singleband.tif")
    configure(run_cli, catalog, form, {"parquet.enabled": ("false", False)})

    _add(run_cli, catalog, scene_a, scene_b)

    assert (catalog / "imagery" / "collection.json").is_file()
    assert not (catalog / "imagery" / "items.parquet").exists()
    assert "geoparquet-items" not in _load(catalog / "imagery" / "collection.json")["assets"]


# ---------------------------------------------------------------------------
# partitioning.*
# ---------------------------------------------------------------------------

PARTITION_SETTINGS: dict[str, tuple[str, Any]] = {
    "partitioning.enabled": ("true", True),
    "partitioning.threshold_gb": ("0.0001", 0.0001),
    "partitioning.target_rows": ("200", 200),
}


@_tests("partitioning.enabled", "partitioning.threshold_gb", "partitioning.target_rows")
@_forms()
def test_partitioning_splits_a_file_over_the_threshold(
    form: str, catalog: Path, run_cli: RunCli
) -> None:
    """open-buildings.parquet is 1,000 rows. 200 rows a partition gives 4 KD-tree cells."""
    source = _copy(OPEN_BUILDINGS, catalog / "buildings" / "buildings.parquet")
    configure(run_cli, catalog, form, PARTITION_SETTINGS)

    _add(run_cli, catalog, source)

    cells = sorted(p.name for p in (catalog / "buildings").glob("kdtree_cell=*") if p.is_dir())
    assert cells == ["kdtree_cell=000", "kdtree_cell=001", "kdtree_cell=010", "kdtree_cell=011"]
    collection = _load(catalog / "buildings" / "collection.json")
    assert sorted(k for k in collection if k.startswith("partition:")) == PARTITION_FIELDS


PARTITION_FIELDS = [
    "partition:file_count",
    "partition:keys",
    "partition:scheme",
    "partition:strategy",
]


@_tests("partitioning.strategy")
@_forms()
def test_partitioning_strategy_reaches_the_partitioner(
    form: str, catalog: Path, run_cli: RunCli
) -> None:
    """Only kdtree exists, so the observable is the refusal of another strategy."""
    source = _copy(OPEN_BUILDINGS, catalog / "buildings" / "buildings.parquet")
    configure(
        run_cli,
        catalog,
        form,
        {**PARTITION_SETTINGS, "partitioning.strategy": ("h3", "h3")},
    )

    result = run_cli("add", "--portolan-dir", str(catalog), str(source), expect=1)

    assert "Strategy 'h3' not yet supported. Currently only 'kdtree' is implemented." in (
        result.output
    )
    assert not (catalog / "buildings" / "collection.json").exists()


def test_partitioning_stays_off_below_the_default_threshold(catalog: Path, run_cli: RunCli) -> None:
    """Without the settings the same file stays whole, so the tests above measure them."""
    source = _copy(OPEN_BUILDINGS, catalog / "buildings" / "buildings.parquet")

    _add(run_cli, catalog, source)

    assert sorted(p.name for p in (catalog / "buildings").glob("*.parquet")) == [
        "buildings.parquet"
    ]
    assert list((catalog / "buildings").glob("*=*")) == []


# ---------------------------------------------------------------------------
# pmtiles.*
# ---------------------------------------------------------------------------

needs_tippecanoe = pytest.mark.skipif(
    shutil.which("tippecanoe") is None,
    reason="tippecanoe not installed (required for PMTiles generation)",
)


@needs_tippecanoe
@_tests(
    "pmtiles.enabled",
    "pmtiles.min_zoom",
    "pmtiles.max_zoom",
    "pmtiles.layer",
    "pmtiles.attribution",
)
@_forms()
def test_pmtiles_settings_reach_the_tiles(form: str, catalog: Path, run_cli: RunCli) -> None:
    from pmtiles.reader import MmapSource, Reader

    source = _copy(POINTS_PARQUET, catalog / "places" / "places.parquet")
    configure(
        run_cli,
        catalog,
        form,
        {
            "pmtiles.enabled": ("true", True),
            "pmtiles.min_zoom": ("3", 3),
            "pmtiles.max_zoom": ("7", 7),
            "pmtiles.layer": ("venues", "venues"),
            "pmtiles.attribution": ("Example Agency", "Example Agency"),
        },
    )

    _add(run_cli, catalog, source)

    with (catalog / "places" / "places.pmtiles").open("rb") as handle:
        reader = Reader(MmapSource(handle))
        header = reader.header()
        metadata = reader.metadata()
    assert (header["min_zoom"], header["max_zoom"]) == (3, 7)
    assert [layer["id"] for layer in metadata["vector_layers"]] == ["venues"]
    assert metadata["attribution"] == "Example Agency"


# ---------------------------------------------------------------------------
# push.exclude
# ---------------------------------------------------------------------------


def _push_and_list(run_cli: RunCli, catalog: Path, s3_bucket: tuple[str, str]) -> list[str]:
    import boto3

    bucket, endpoint = s3_bucket
    run_cli("push", f"s3://{bucket}/catalog", "--catalog", str(catalog))
    client = boto3.client("s3", endpoint_url=endpoint, region_name="us-east-1")
    return sorted(obj["Key"] for obj in client.list_objects_v2(Bucket=bucket)["Contents"])


def _catalog_with_notes(run_cli: RunCli, catalog: Path) -> None:
    """Add one collection, then put two untracked notes beside it.

    ``push`` uploads README.md in its own phase, whatever the patterns say,
    so the pattern is measured on AGENTS.md and notes.md.
    """
    source = _copy(POINTS_PARQUET, catalog / "places" / "places.parquet")
    _add(run_cli, catalog, source)
    (catalog / "places" / "notes.md").write_text("# Notes\n", encoding="utf-8")
    (catalog / "places" / "notes.txt").write_text("Notes\n", encoding="utf-8")


PUSHED = [
    "catalog/AGENTS.md",
    "catalog/README.md",
    "catalog/catalog.json",
    "catalog/places/AGENTS.md",
    "catalog/places/README.md",
    "catalog/places/collection.json",
    "catalog/places/notes.md",
    "catalog/places/notes.txt",
    "catalog/places/places.parquet",
    "catalog/places/places.thumb.jpg",
    "catalog/places/versions.json",
    "catalog/versions.json",
]


@_tests("push.exclude")
@_forms()
def test_push_exclude_keeps_matching_files_out_of_the_bucket(
    form: str,
    catalog: Path,
    run_cli: RunCli,
    s3_bucket: tuple[str, str],
    _aws_env: None,
) -> None:
    _catalog_with_notes(run_cli, catalog)
    configure(run_cli, catalog, form, {"push.exclude": ('["*.md"]', ["*.md"])})

    keys = _push_and_list(run_cli, catalog, s3_bucket)

    # README.md goes up in its own phase. AGENTS.md and notes.md match the pattern.
    assert keys == [key for key in PUSHED if not key.endswith(".md") or key.endswith("/README.md")]


def test_push_uploads_untracked_notes_by_default(
    catalog: Path, run_cli: RunCli, s3_bucket: tuple[str, str], _aws_env: None
) -> None:
    """The default patterns let notes.md through, so the test above measures the key."""
    _catalog_with_notes(run_cli, catalog)

    assert _push_and_list(run_cli, catalog, s3_bucket) == PUSHED


# ---------------------------------------------------------------------------
# tabular.*
# ---------------------------------------------------------------------------


def _write_csv(path: Path) -> Path:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text("tract_id,population\n001,5000\n002,7500\n", encoding="utf-8")
    return path


@_tests("tabular.enabled")
@_forms()
def test_tabular_enabled_tracks_a_table_as_a_collection(
    form: str, catalog: Path, run_cli: RunCli
) -> None:
    table = _write_csv(catalog / "census" / "census.csv")
    configure(run_cli, catalog, form, {"tabular.enabled": ("true", True)})

    _add(run_cli, catalog, table)

    collection = _load(catalog / "census" / "collection.json")
    assert [column["name"] for column in collection["table:columns"]] == [
        "tract_id",
        "population",
    ]


def test_tabular_data_is_refused_by_default(catalog: Path, run_cli: RunCli) -> None:
    """The default refuses the table, so the test above measures the setting."""
    table = _write_csv(catalog / "census" / "census.csv")

    run_cli("add", "--portolan-dir", str(catalog), str(table), expect=1)

    assert not (catalog / "census" / "collection.json").exists()


@_tests("tabular.convert")
@_forms()
def test_tabular_convert_false_keeps_the_csv(form: str, catalog: Path, run_cli: RunCli) -> None:
    table = _write_csv(catalog / "census" / "census.csv")
    configure(
        run_cli,
        catalog,
        form,
        {"tabular.enabled": ("true", True), "tabular.convert": ("false", False)},
    )

    _add(run_cli, catalog, table)

    assert not (catalog / "census" / "census.parquet").exists()
    hrefs = [
        asset["href"] for asset in _load(catalog / "census" / "collection.json")["assets"].values()
    ]
    assert "./census.csv" in hrefs


def test_tabular_convert_defaults_to_parquet(catalog: Path, run_cli: RunCli) -> None:
    table = _write_csv(catalog / "census" / "census.csv")
    configure(run_cli, catalog, "yaml", {"tabular.enabled": ("true", True)})

    _add(run_cli, catalog, table)

    assert pq.read_schema(catalog / "census" / "census.parquet").names == [
        "tract_id",
        "population",
    ]


# ---------------------------------------------------------------------------
# ignored_files
# ---------------------------------------------------------------------------


@_tests("ignored_files")
@_forms()
def test_ignored_files_skips_the_matching_file(form: str, catalog: Path, run_cli: RunCli) -> None:
    _copy(POINTS_PARQUET, catalog / "roads" / "roads.parquet")
    _copy(POINTS_GEOJSON, catalog / "places" / "places.geojson")
    configure(run_cli, catalog, form, {"ignored_files": ('["*.geojson"]', ["*.geojson"])})

    _add(run_cli, catalog, catalog)

    assert (catalog / "roads" / "collection.json").is_file()
    assert not (catalog / "places" / "collection.json").exists()


def test_without_ignored_files_the_geojson_is_added(catalog: Path, run_cli: RunCli) -> None:
    _copy(POINTS_PARQUET, catalog / "roads" / "roads.parquet")
    _copy(POINTS_GEOJSON, catalog / "places" / "places.geojson")

    _add(run_cli, catalog, catalog)

    assert (catalog / "places" / "collection.json").is_file()


# ---------------------------------------------------------------------------
# backend
# ---------------------------------------------------------------------------


@_tests("backend")
@_forms()
@pytest.mark.skipif(
    sys.platform == "win32", reason="PyIceberg SQL catalog warehouse paths break on Windows"
)
def test_backend_iceberg_writes_the_iceberg_fields(
    form: str, catalog: Path, run_cli: RunCli
) -> None:
    pytest.importorskip("pyiceberg")
    source = _copy(POINTS_PARQUET, catalog / "roads" / "roads.parquet")
    configure(run_cli, catalog, form, {"backend": ("iceberg", "iceberg")})

    _add(run_cli, catalog, source)

    collection = _load(catalog / "roads" / "collection.json")
    assert sorted(k for k in collection if k.startswith("iceberg:")) == ICEBERG_FIELDS


# The default SQLite catalog URI is a local path, so the collection omits
# iceberg:catalog_uri (#939).
ICEBERG_FIELDS = [
    "iceberg:catalog_type",
    "iceberg:current_snapshot_id",
    "iceberg:format_version",
    "iceberg:table_id",
]


# ---------------------------------------------------------------------------
# conversion.vector.*
# ---------------------------------------------------------------------------


def _converted_columns(path: Path) -> list[str]:
    return list(pq.read_schema(path).names)


@_tests("conversion.vector.add_bbox")
@_forms()
def test_conversion_vector_add_bbox_false_drops_the_bbox_column(
    form: str, catalog: Path, run_cli: RunCli
) -> None:
    source = _copy(POINTS_GEOJSON, catalog / "places" / "places.geojson")
    configure(run_cli, catalog, form, {"conversion.vector.add_bbox": ("false", False)})

    _add(run_cli, catalog, source)

    assert "bbox" not in _converted_columns(catalog / "places" / "places.parquet")


def test_add_writes_a_bbox_column_by_default(catalog: Path, run_cli: RunCli) -> None:
    source = _copy(POINTS_GEOJSON, catalog / "places" / "places.geojson")

    _add(run_cli, catalog, source)

    assert "bbox" in _converted_columns(catalog / "places" / "places.parquet")


@_tests(
    "conversion.vector.sort",
    "conversion.vector.spatial_index",
    "conversion.vector.resolution",
)
@_forms()
def test_conversion_vector_layout_settings_reach_the_writer(
    form: str, catalog: Path, run_cli: RunCli
) -> None:
    source = _copy(POINTS_GEOJSON, catalog / "places" / "places.geojson")
    configure(
        run_cli,
        catalog,
        form,
        {
            "conversion.vector.sort": ("none", "none"),
            "conversion.vector.spatial_index": ("h3", "h3"),
            "conversion.vector.resolution": ("5", 5),
        },
    )

    _add(run_cli, catalog, source)

    table = pq.read_table(catalog / "places" / "places.parquet")
    # sort: none keeps the order of the GeoJSON features.
    assert table.column("name").to_pylist() == SOURCE_ORDER
    # The H3 index stores its resolution in bits 52 to 55.
    cells = table.column("h3_cell").to_pylist()
    assert {int(cell, 16) >> 52 & 0xF for cell in cells} == {5}


SOURCE_ORDER = [
    "Ferry Building",
    "Coit Tower",
    "Golden Gate Park",
    "Fisherman's Wharf",
    "Alcatraz View",
    "Mission District",
    "Castro",
    "SOMA",
    "Financial District",
    "Embarcadero",
]


def test_the_default_layout_sorts_and_adds_no_index(catalog: Path, run_cli: RunCli) -> None:
    """Hilbert order differs from the source order, so the test above measures sort."""
    source = _copy(POINTS_GEOJSON, catalog / "places" / "places.geojson")

    _add(run_cli, catalog, source)

    table = pq.read_table(catalog / "places" / "places.parquet")
    assert table.column("name").to_pylist() != SOURCE_ORDER
    assert "h3_cell" not in table.schema.names


@_tests("conversion.vector.partition")
@_forms()
def test_conversion_vector_partition_reaches_the_partitioner(
    form: str, catalog: Path, run_cli: RunCli
) -> None:
    """Only check --fix applies this key. geoparquet-io refuses 10 rows a partition."""
    _copy(POINTS_GEOJSON, catalog / "places" / "places.geojson")
    configure(
        run_cli,
        catalog,
        form,
        {
            "conversion.vector.partition": ("true", True),
            "conversion.vector.spatial_index": ("h3", "h3"),
            "conversion.vector.resolution": ("3", 3),
        },
    )

    result = run_cli("check", str(catalog), "--fix", "--geo-assets", expect=1)

    assert "Tiny partitions: Average of 10 rows per partition" in result.output
    assert not (catalog / "places" / "places.parquet").exists()


def test_check_fix_converts_the_geojson_by_default(catalog: Path, run_cli: RunCli) -> None:
    _copy(POINTS_GEOJSON, catalog / "places" / "places.geojson")

    run_cli("check", str(catalog), "--fix", "--geo-assets")

    assert (catalog / "places" / "places.parquet").is_file()


# ---------------------------------------------------------------------------
# conversion.extensions.*, conversion.paths.*, conversion.handlers.*
# ---------------------------------------------------------------------------

# `add` does not read the extension and path overrides. Only check --fix
# --geo-assets applies them, so these tests observe them through check.


@_tests("conversion.extensions.preserve")
@_forms()
def test_conversion_extensions_preserve_keeps_the_geojson(
    form: str, catalog: Path, run_cli: RunCli
) -> None:
    _copy(POINTS_GEOJSON, catalog / "places" / "places.geojson")
    configure(run_cli, catalog, form, {"conversion.extensions.preserve": ("geojson", ["geojson"])})

    run_cli("check", str(catalog), "--fix", "--geo-assets")

    assert not (catalog / "places" / "places.parquet").exists()


@_tests("conversion.paths.preserve")
@_forms()
def test_conversion_paths_preserve_keeps_the_matching_file(
    form: str, catalog: Path, run_cli: RunCli
) -> None:
    _copy(POINTS_GEOJSON, catalog / "places" / "places.geojson")
    configure(run_cli, catalog, form, {"conversion.paths.preserve": ("places/*", ["places/*"])})

    run_cli("check", str(catalog), "--fix", "--geo-assets")

    assert not (catalog / "places" / "places.parquet").exists()


@_tests("conversion.extensions.convert")
@_forms()
def test_conversion_extensions_convert_converts_a_flatgeobuf(
    form: str, catalog: Path, run_cli: RunCli
) -> None:
    _copy(FLATGEOBUF, catalog / "places" / "places.fgb")
    configure(run_cli, catalog, form, {"conversion.extensions.convert": ("fgb", ["fgb"])})

    run_cli("check", str(catalog), "--fix", "--geo-assets")

    assert (catalog / "places" / "places.parquet").is_file()


FLATGEOBUF = FIXTURES / "cloud_native" / "sample.fgb"


@_tests("conversion.handlers.*")
@_forms()
def test_conversion_handlers_pin_selects_the_handler(
    form: str, catalog: Path, run_cli: RunCli
) -> None:
    """A pin of .tif to the PMTiles handler makes add read the GeoTIFF as PMTiles."""
    scene = _copy(SINGLEBAND_COG, catalog / "imagery" / "scene" / "scene.tif")
    configure(run_cli, catalog, form, {"conversion.handlers.tif": ("pmtiles", "pmtiles")})

    result = run_cli("add", "--portolan-dir", str(catalog), str(scene), expect=1)

    assert "Invalid PMTiles file" in result.output
    assert not (catalog / "imagery" / "collection.json").exists()


@pytest.mark.realdata
@_forms()
def test_extract_arcgis_reads_conversion_vector(
    form: str, catalog: Path, run_cli: RunCli, arcgis_url: str
) -> None:
    configure(run_cli, catalog, form, {"conversion.vector.add_bbox": ("false", False)})

    run_cli("extract", "arcgis", arcgis_url, str(catalog), "--auto", "--license", "CC-BY-4.0")

    outputs = sorted(catalog.rglob("*.parquet"))
    assert outputs != []
    assert {"bbox" in _converted_columns(path) for path in outputs} == {False}


# ---------------------------------------------------------------------------
# conversion.cog.*
# ---------------------------------------------------------------------------


def _cog_profile(path: Path) -> tuple[str, int, int]:
    import rasterio

    with rasterio.open(path) as source:
        return str(source.compression.name), source.block_shapes[0][0], source.block_shapes[0][1]


@_tests("conversion.cog.compression", "conversion.cog.tile_size")
@_forms()
def test_conversion_cog_settings_reach_the_cog(form: str, catalog: Path, run_cli: RunCli) -> None:
    """#774 is closed. This is its regression check on the yaml form."""
    scene = _copy(NON_COG, catalog / "imagery" / "scene" / "scene.tif")
    configure(
        run_cli,
        catalog,
        form,
        {
            "conversion.cog.compression": ("LZW", "LZW"),
            "conversion.cog.tile_size": ("256", 256),
        },
    )

    _add(run_cli, catalog, scene)

    assert _cog_profile(catalog / "imagery" / "scene" / "scene.tif") == ("lzw", 256, 256)


def test_the_default_cog_is_deflate_with_512_blocks(catalog: Path, run_cli: RunCli) -> None:
    scene = _copy(NON_COG, catalog / "imagery" / "scene" / "scene.tif")

    _add(run_cli, catalog, scene)

    assert _cog_profile(catalog / "imagery" / "scene" / "scene.tif") == ("deflate", 512, 512)


def _structure_tags(path: Path) -> dict[str, str]:
    import rasterio

    with rasterio.open(path) as source:
        tags: dict[str, str] = source.tags(ns="IMAGE_STRUCTURE")
        tags["OVERVIEW_RESAMPLING"] = source.tags().get("OVERVIEW_RESAMPLING", "")
        return tags


@_tests("conversion.cog.predictor", "conversion.cog.resampling")
@_forms()
def test_conversion_cog_predictor_and_resampling_reach_the_cog(
    form: str, catalog: Path, run_cli: RunCli
) -> None:
    scene = _copy(NON_COG, catalog / "imagery" / "scene" / "scene.tif")
    configure(
        run_cli,
        catalog,
        form,
        {
            "conversion.cog.predictor": ("1", 1),
            "conversion.cog.resampling": ("average", "average"),
        },
    )

    _add(run_cli, catalog, scene)

    tags = _structure_tags(catalog / "imagery" / "scene" / "scene.tif")
    assert "PREDICTOR" not in tags
    assert tags["OVERVIEW_RESAMPLING"] == "AVERAGE"


def test_the_default_cog_has_predictor_2_and_nearest_overviews(
    catalog: Path, run_cli: RunCli
) -> None:
    scene = _copy(NON_COG, catalog / "imagery" / "scene" / "scene.tif")

    _add(run_cli, catalog, scene)

    tags = _structure_tags(catalog / "imagery" / "scene" / "scene.tif")
    assert (tags["PREDICTOR"], tags["OVERVIEW_RESAMPLING"]) == ("2", "NEAREST")


@_tests("conversion.cog.quality")
@_forms()
def test_conversion_cog_quality_reaches_a_jpeg_cog(
    form: str, catalog: Path, run_cli: RunCli
) -> None:
    scene = _copy(NON_COG, catalog / "imagery" / "scene" / "scene.tif")
    configure(
        run_cli,
        catalog,
        form,
        {"conversion.cog.compression": ("JPEG", "JPEG"), "conversion.cog.quality": ("40", 40)},
    )

    _add(run_cli, catalog, scene)

    assert _structure_tags(catalog / "imagery" / "scene" / "scene.tif")["JPEG_QUALITY"] == "40"


def _jpeg_quality(path: Path) -> list[int]:
    """Return each quality whose standard JPEG tables match the tables in ``path``."""
    import io

    from PIL import Image

    def tables(quality: int, mode: str) -> dict[int, list[int]]:
        buffer = io.BytesIO()
        Image.new(mode, (16, 16)).save(buffer, "JPEG", quality=quality)
        with Image.open(buffer) as image:
            return {key: list(table) for key, table in image.quantization.items()}

    with Image.open(path) as image:
        found = {key: list(table) for key, table in image.quantization.items()}
        mode = image.mode
    return [quality for quality in range(1, 101) if tables(quality, mode) == found]


def _image_size(path: Path) -> tuple[int, int]:
    from PIL import Image

    with Image.open(path) as image:
        width, height = image.size
        return int(width), int(height)


@_tests("conversion.cog.thumbnail_max_size", "conversion.cog.thumbnail_quality")
@_forms()
def test_conversion_cog_thumbnail_settings_reach_the_thumbnail(
    form: str, catalog: Path, run_cli: RunCli
) -> None:
    scene = _copy(NON_COG, catalog / "imagery" / "scene" / "scene.tif")
    configure(
        run_cli,
        catalog,
        form,
        {
            "conversion.cog.thumbnail_max_size": ("64", 64),
            "conversion.cog.thumbnail_quality": ("20", 20),
        },
    )

    _add(run_cli, catalog, scene)

    thumbnail = catalog / "imagery" / "scene" / "scene.thumb.jpg"
    assert _image_size(thumbnail) == (64, 64)
    assert _jpeg_quality(thumbnail) == [20]


def test_the_default_cog_thumbnail_is_512_at_quality_75(catalog: Path, run_cli: RunCli) -> None:
    scene = _copy(NON_COG, catalog / "imagery" / "scene" / "scene.tif")

    _add(run_cli, catalog, scene)

    thumbnail = catalog / "imagery" / "scene" / "scene.thumb.jpg"
    assert _image_size(thumbnail) == (512, 512)
    assert _jpeg_quality(thumbnail) == [75]


@_tests("conversion.cog.generate_thumbnail")
@_forms()
def test_conversion_cog_generate_thumbnail_false_writes_no_thumbnail(
    form: str, catalog: Path, run_cli: RunCli
) -> None:
    scene = _copy(NON_COG, catalog / "imagery" / "scene" / "scene.tif")
    configure(run_cli, catalog, form, {"conversion.cog.generate_thumbnail": ("false", False)})

    _add(run_cli, catalog, scene)

    assert not (catalog / "imagery" / "scene" / "scene.thumb.jpg").exists()
    assert "thumbnail" not in _load(catalog / "imagery" / "scene" / "scene.json")["assets"]


# ---------------------------------------------------------------------------
# styles.*
# ---------------------------------------------------------------------------


def _render_fields(item: Path) -> dict[str, Any]:
    data = _load(item)["assets"]["data"]
    return {key: value for key, value in data.items() if key.startswith("render:")}


@_tests("styles.raster.colormap", "styles.raster.rescale")
@_forms()
def test_styles_raster_settings_reach_the_render_fields(
    form: str, catalog: Path, run_cli: RunCli
) -> None:
    scene = _copy(NON_COG, catalog / "imagery" / "scene" / "scene.tif")
    configure(
        run_cli,
        catalog,
        form,
        {
            "styles.raster.colormap": ("terrain", "terrain"),
            "styles.raster.rescale": ("[0, 255]", [0, 255]),
        },
    )

    _add(run_cli, catalog, scene)

    assert _render_fields(catalog / "imagery" / "scene" / "scene.json") == {
        "render:colormap_name": "terrain",
        "render:rescale": [[0.0, 255.0]],
    }


def test_the_default_raster_render_field_is_viridis(catalog: Path, run_cli: RunCli) -> None:
    scene = _copy(NON_COG, catalog / "imagery" / "scene" / "scene.tif")

    _add(run_cli, catalog, scene)

    assert _render_fields(catalog / "imagery" / "scene" / "scene.json") == {
        "render:colormap_name": "viridis"
    }


VECTOR_STYLES = {
    "point": (
        POINTS_PARQUET,
        {"circle-color": "#ff0000", "circle-radius": 9, "circle-opacity": 0.5},
    ),
    "line": (
        FIXTURES / "vector" / "valid" / "lines.geojson",
        {"line-color": "#00ff00", "line-width": 5, "line-opacity": 0.4},
    ),
    "polygon": (
        FIXTURES / "vector" / "valid" / "polygons.geojson",
        {"fill-color": "#0000ff", "fill-opacity": 0.3, "fill-outline-color": "#111111"},
    ),
}


@needs_tippecanoe
@_tests(
    *(
        f"styles.vector.{kind}.{name}"
        for kind, (_, paint) in VECTOR_STYLES.items()
        for name in paint
    )
)
@_forms()
@pytest.mark.parametrize("kind", sorted(VECTOR_STYLES))
def test_styles_vector_settings_reach_the_default_style(
    kind: str, form: str, catalog: Path, run_cli: RunCli
) -> None:
    fixture, paint = VECTOR_STYLES[kind]
    source = _copy(fixture, catalog / "places" / f"places{fixture.suffix}")
    settings: dict[str, tuple[str, Any]] = {"pmtiles.enabled": ("true", True)}
    for name, value in paint.items():
        settings[f"styles.vector.{kind}.{name}"] = (str(value), value)
    configure(run_cli, catalog, form, settings)

    _add(run_cli, catalog, source)

    style = _load(catalog / "places" / "styles" / "default.json")
    assert style["layers"][0]["paint"] == paint


# ---------------------------------------------------------------------------
# thumbnails.*
# ---------------------------------------------------------------------------


@_tests("thumbnails.enabled")
@_forms()
def test_thumbnails_enabled_false_draws_no_collection_thumbnail(
    form: str, catalog: Path, run_cli: RunCli
) -> None:
    source = _copy(POINTS_GEOJSON, catalog / "places" / "places.geojson")
    configure(run_cli, catalog, form, {"thumbnails.enabled": ("false", False)})

    _add(run_cli, catalog, source)

    assert not (catalog / "places" / "places.thumb.jpg").exists()


@_tests("thumbnails.max_size")
@_forms()
def test_thumbnails_max_size_bounds_the_drawing(form: str, catalog: Path, run_cli: RunCli) -> None:
    source = _copy(POINTS_GEOJSON, catalog / "places" / "places.geojson")
    configure(run_cli, catalog, form, {"thumbnails.max_size": ("128", 128)})

    _add(run_cli, catalog, source)

    # The drawing is cropped to the data, so each side is at most the limit.
    assert max(_image_size(catalog / "places" / "places.thumb.jpg")) <= 128


@_tests("thumbnails.quality")
@_forms()
def test_thumbnails_quality_reaches_the_jpeg(form: str, catalog: Path, run_cli: RunCli) -> None:
    source = _copy(POINTS_GEOJSON, catalog / "places" / "places.geojson")
    configure(run_cli, catalog, form, {"thumbnails.quality": ("20", 20)})

    _add(run_cli, catalog, source)

    assert _jpeg_quality(catalog / "places" / "places.thumb.jpg") == [20]


def test_the_default_vector_thumbnail_is_over_128_at_quality_75(
    catalog: Path, run_cli: RunCli
) -> None:
    source = _copy(POINTS_GEOJSON, catalog / "places" / "places.geojson")

    _add(run_cli, catalog, source)

    thumbnail = catalog / "places" / "places.thumb.jpg"
    assert max(_image_size(thumbnail)) > 128
    assert _jpeg_quality(thumbnail) == [75]


# ---------------------------------------------------------------------------
# check.*, publish.*
# ---------------------------------------------------------------------------


def _findings(run_cli: RunCli, catalog: Path, expect: int) -> list[tuple[str, str]]:
    result = run_cli("--format", "json", "check", str(catalog), expect=expect)
    findings = json.loads(result.output)["data"]["findings"]
    return [(finding["rule_id"], finding["severity"]) for finding in findings]


@_tests("check.disabled", "check.severity.*")
@_forms()
def test_check_settings_reach_the_validator(form: str, catalog: Path, run_cli: RunCli) -> None:
    _add(run_cli, catalog, _copy(POINTS_PARQUET, catalog / "places" / "places.parquet"))
    configure(
        run_cli,
        catalog,
        form,
        {
            "check.disabled": ('["PTL-FIL-005"]', ["PTL-FIL-005"]),
            "check.severity.PTL-PRV-001": ("warning", "warning"),
        },
    )

    assert _findings(run_cli, catalog, expect=0) == [("PTL-PRV-001", "warning")]


def test_check_reports_both_findings_by_default(catalog: Path, run_cli: RunCli) -> None:
    _add(run_cli, catalog, _copy(POINTS_PARQUET, catalog / "places" / "places.parquet"))

    assert _findings(run_cli, catalog, expect=1) == [
        ("PTL-FIL-005", "warning"),
        ("PTL-PRV-001", "error"),
    ]


@_tests("publish.public_url")
@_forms()
def test_publish_public_url_reaches_the_live_check(
    form: str, catalog: Path, run_cli: RunCli
) -> None:
    """check --live rejects a plain-HTTP base URL and names the setting."""
    configure(
        run_cli,
        catalog,
        form,
        {"publish.public_url": ("http://example.com/c", "http://example.com/c")},
    )

    result = run_cli("check", str(catalog), "--live", expect=2)

    assert "[PRTLN-VAL005]" in result.output
    assert "publish.public_url" in result.output


def test_check_live_needs_no_url_without_the_setting(catalog: Path, run_cli: RunCli) -> None:
    result = run_cli("check", str(catalog), "--live", expect=None)

    assert "[PRTLN-VAL005]" not in result.output


# ---------------------------------------------------------------------------
# A collection config.yaml
# ---------------------------------------------------------------------------


def test_a_catalog_only_key_in_a_collection_config_warns(
    catalog: Path, run_cli: RunCli, caplog: pytest.LogCaptureFixture
) -> None:
    """No reader sees thumbnails.enabled in a collection file, so add warns once and goes on."""
    scene = _copy(SINGLEBAND_COG, catalog / "imagery" / "scene" / "scene.tif")
    config_file = catalog / "imagery" / ".portolan" / "config.yaml"
    config_file.parent.mkdir()
    config_file.write_text("thumbnails:\n  enabled: false\n", encoding="utf-8")

    with caplog.at_level("WARNING", logger="portolan_cli.config"):
        _add(run_cli, catalog, scene)

    assert [(r.levelname, r.getMessage()) for r in caplog.records] == [
        (
            "WARNING",
            f"{config_file.resolve()}: 'thumbnails.enabled' applies to the whole catalog, "
            "so no reader sees it here. Move it to the catalog config.yaml.",
        )
    ]
    assert (catalog / "imagery" / "scene" / "scene.json").is_file()


# ---------------------------------------------------------------------------
# Coverage of KNOWN_SETTINGS
# ---------------------------------------------------------------------------


def test_every_known_setting_is_covered() -> None:
    """A key is tested here, or a config-key line says why no output shows it."""
    exempt = {row[0].split(": ", 1)[0] for row in load_exemptions("config-key")}

    assert set(TESTED_KEYS) & exempt == set()
    assert set(KNOWN_SETTINGS) - (set(TESTED_KEYS) | exempt) == set()
    assert set(TESTED_KEYS) | exempt <= set(KNOWN_SETTINGS)
