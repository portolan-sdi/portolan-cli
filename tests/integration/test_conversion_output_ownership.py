"""Conversion outputs belong to one source, including during parallel add."""

from __future__ import annotations

import json
import shutil
import threading
from concurrent.futures import ThreadPoolExecutor
from typing import TYPE_CHECKING, Any
from unittest.mock import patch

import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from portolan_cli import api
from portolan_cli.conversion_config import CogSettings, VectorSettings
from portolan_cli.convert import (
    PROVENANCE_KEY,
    ConversionStatus,
    claim_output,
    convert_file,
    copy_to_cog,
    place_cog,
    write_cog,
)
from portolan_cli.errors import OutputExistsError
from portolan_cli.run_state import RunState
from tests.conftest import _init_catalog

if TYPE_CHECKING:
    from pathlib import Path

pytestmark = pytest.mark.integration


def _geojson(path: Path, value: int) -> Path:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(
        json.dumps(
            {
                "type": "FeatureCollection",
                "features": [
                    {
                        "type": "Feature",
                        "properties": {"value": value},
                        "geometry": {"type": "Point", "coordinates": [1, 2]},
                    }
                ],
            }
        )
    )
    return path


@pytest.mark.parametrize("suffix", [".parquet", ".tif"])
def test_parallel_claims_reserve_free_names(tmp_path: Path, suffix: str) -> None:
    """A claim must exclude a second source before either converter writes."""
    state = RunState.open(tmp_path)
    ready = threading.Barrier(2)
    claimed = threading.Barrier(2)
    output = tmp_path / f"roads{suffix}"

    def claim(source: Path) -> Path:
        ready.wait(timeout=10)
        target = claim_output(source, output, state=state)
        claimed.wait(timeout=10)
        return target

    sources = [tmp_path / "roads.geojson", tmp_path / "roads.csv"]
    with ThreadPoolExecutor(max_workers=2) as executor:
        targets = list(executor.map(claim, sources))
    assert len(set(targets)) == 2, "both sources claimed the same unwritten output"
    assert not any(target.exists() for target in targets)
    # The nested built-in handler can reclaim its caller's reservation.
    for source, target in zip(sources, targets, strict=True):
        assert claim_output(source, output, state=state) == target


def test_parallel_add_keeps_both_same_stem_outputs(tmp_path: Path) -> None:
    """Phase-2 workers must publish both converted values, in one version."""
    import portolan_cli.preparation as preparation
    from portolan_cli.add import add_files
    from portolan_cli.versions import read_versions

    root = _init_catalog(tmp_path / "catalog")
    collection = root / "roads"
    paths = [_geojson(collection / "roads.geojson", 111), collection / "roads.csv"]
    paths[1].write_text('WKT,value\n"POINT (3 4)",222\n')
    barrier = threading.Barrier(2)
    conversion_lock = threading.Lock()
    original_claim = preparation.claim_output
    original_convert = preparation._convert_and_extract_metadata

    def synchronized_claim(*args: Any, **kwargs: Any) -> Path:
        target = original_claim(*args, **kwargs)
        if kwargs.get("state") is not None:
            barrier.wait(timeout=10)
        return target

    def serial_convert(*args: Any, **kwargs: Any) -> Any:
        with conversion_lock:
            return original_convert(*args, **kwargs)

    with (
        patch.object(preparation, "claim_output", synchronized_claim),
        patch.object(preparation, "_convert_and_extract_metadata", serial_convert),
    ):
        added, _skipped, failed = add_files(paths=paths, catalog_root=root, workers=2)

    assert failed == []
    assert len(added) == 2
    outputs = sorted(collection.glob("*.parquet"))
    assert len(outputs) == 2, "one successful conversion overwrote the other"
    assert sorted(pq.read_table(p).column("value")[0].as_py() for p in outputs) == [111, 222]
    assert len(read_versions(collection / "versions.json").versions) == 1


def test_same_basename_sources_have_distinct_owners(tmp_path: Path) -> None:
    output = tmp_path / "out"
    output.mkdir()
    sources = [
        _geojson(tmp_path / name / "points.geojson", value)
        for name, value in [("a", 111), ("b", 222)]
    ]
    results = [convert_file(source, output_dir=output) for source in sources]
    assert [result.status for result in results] == [ConversionStatus.SUCCESS] * 2
    assert results[0].output != results[1].output, "basename provenance conflated two sources"
    assert [pq.read_table(result.output).column("value")[0].as_py() for result in results] == [
        111,
        222,
    ]
    # Updating a source retains its output ownership even though its checksum changes.
    _geojson(sources[0], 333)
    updated = convert_file(sources[0], output_dir=output)
    assert updated.output == results[0].output
    assert pq.read_table(updated.output).column("value").to_pylist() == [333]


def test_legacy_basename_record_requires_matching_checksum(tmp_path: Path) -> None:
    source = _geojson(tmp_path / "a" / "points.geojson", 111)
    output = tmp_path / "points.parquet"
    pq.write_table(
        pa.table({"operator": [999]}).replace_schema_metadata(
            {
                PROVENANCE_KEY: json.dumps({"path": source.name, "sha256": "different source"}),
            }
        ),
        output,
    )
    before = output.read_bytes()
    assert claim_output(source, output) == tmp_path / "points-geojson.parquet"
    assert output.read_bytes() == before


@pytest.mark.parametrize("source_kind", ["geojson", "parquet"])
def test_api_vector_preserves_unowned_output(tmp_path: Path, source_kind: str) -> None:
    source = tmp_path / "src" / f"roads.{source_kind}"
    source.parent.mkdir()
    if source_kind == "geojson":
        _geojson(source, 111)
    else:
        pq.write_table(pa.table({"value": [111]}), source)
    output = tmp_path / "roads.parquet"
    pq.write_table(pa.table({"operator": [999]}), output)
    before = output.read_bytes()
    converted = api.convert_vector(
        source, tmp_path, settings=VectorSettings(sort="none", add_bbox=False)
    )
    assert output.read_bytes() == before, "the public API overwrote the operator's Parquet"
    assert converted == tmp_path / f"roads-{source_kind}.parquet"
    assert pq.read_table(converted).column("value").to_pylist() == [111]


@pytest.mark.parametrize("mode", ["copy", "rewrite", "write_cog"])
def test_api_raster_preserves_unowned_output(
    tmp_path: Path,
    valid_singleband_cog: Path,
    mode: str,
) -> None:
    source = tmp_path / "src" / "scene.tiff"
    source.parent.mkdir()
    shutil.copy2(valid_singleband_cog, source)
    output = tmp_path / "scene.tif"
    output.write_bytes(b"operator's raster")
    if mode == "write_cog":
        converted = write_cog(source, tmp_path)
    else:
        converted = api.convert_raster(source, tmp_path, reoptimize=mode == "rewrite")
    assert output.read_bytes() == b"operator's raster", (
        "the public API overwrote the operator's raster"
    )
    assert converted == tmp_path / "scene-tiff.tif"
    import rasterio

    with rasterio.open(source) as src, rasterio.open(converted) as dst:
        assert (src.read() == dst.read()).all()


@pytest.mark.parametrize("compression", ["DEFLATE", "LZMA"])
def test_raster_scratch_preserves_another_pending_conversion(
    tmp_path: Path,
    valid_singleband_cog: Path,
    compression: str,
) -> None:
    """Distinct .tif/.tiff destinations must not share a scratch .tif or VRT."""
    import rasterio

    from portolan_cli.convert import _rio_cogeo_cog

    source = valid_singleband_cog
    with rasterio.open(source) as src:
        if compression == "DEFLATE":
            first = copy_to_cog(src, tmp_path / "scene.tif", {}, {})
            second = copy_to_cog(src, tmp_path / "scene.tiff", {}, {})
        else:
            settings = CogSettings(compression=compression, predictor=2, resampling="nearest")
            first = _rio_cogeo_cog(src, source, tmp_path / "scene.tif", settings, record=False)
            second = _rio_cogeo_cog(src, source, tmp_path / "scene.tiff", settings, record=False)
    assert first != second, "two pending COG writes shared the same scratch file"
    place_cog(first, tmp_path / "scene.tif")
    place_cog(second, tmp_path / "scene.tiff")


def test_reserved_fallback_cannot_be_claimed_by_another_source(tmp_path: Path) -> None:
    state = RunState.open(tmp_path)
    output = tmp_path / "points.parquet"
    output.write_bytes(b"operator data")
    first = _geojson(tmp_path / "a" / "points.geojson", 111)
    second = _geojson(tmp_path / "b" / "points.geojson", 222)
    assert claim_output(first, output, state=state) == tmp_path / "points-geojson.parquet"
    with pytest.raises(OutputExistsError):
        claim_output(second, output, state=state)


def test_catalog_provenance_survives_a_move(tmp_path: Path) -> None:
    """A new run uses catalog-relative source identity, even after an edit."""
    from portolan_cli.formats import in_run
    from portolan_cli.preparation import convert_tabular

    original = tmp_path / "original"
    original.mkdir()
    source = original / "roads.csv"
    source.write_text("value\n111\n")
    with in_run(RunState.open(original)):
        output = convert_tabular(source, original)
    record = json.loads(pq.read_metadata(output).metadata[PROVENANCE_KEY.encode()])
    assert record["base"] == "catalog"
    assert record["path"] == "roads.csv"
    moved = tmp_path / "moved"
    original.rename(moved)
    source = moved / "roads.csv"
    source.write_text("value\n222\n")
    state = RunState.open(moved)
    assert claim_output(source, moved / "roads.parquet", state=state) == moved / "roads.parquet"


def test_api_can_update_an_output_created_in_a_catalog_run(tmp_path: Path) -> None:
    """An explicit catalog root must allow the API to recognize a prior add's output."""
    from portolan_cli.formats import in_run

    source = _geojson(tmp_path / "roads.geojson", 111)
    settings = VectorSettings(sort="none", add_bbox=False)
    with in_run(RunState.open(tmp_path)):
        first = api.convert_vector(source, tmp_path, tmp_path, settings=settings)
    _geojson(source, 222)
    second = api.convert_vector(source, tmp_path, tmp_path, settings=settings)
    assert second == first, "the API did not recognize catalog-relative provenance"
    assert pq.read_table(second).column("value").to_pylist() == [222]


def test_nested_claims_read_provenance_once_per_run(tmp_path: Path) -> None:
    """The API's guard must share its enclosing add guard's provenance read."""
    from portolan_cli.formats import in_run

    source = _geojson(tmp_path / "roads.geojson", 111)
    output = api.convert_vector(
        source, tmp_path, settings=VectorSettings(sort="none", add_bbox=False)
    )
    state = RunState.open(tmp_path)
    with in_run(state), patch("pyarrow.parquet.read_metadata", wraps=pq.read_metadata) as read:
        assert claim_output(source, output, state=state) == output
        assert claim_output(source, output) == output
    assert read.call_count == 1, "nested output guards reread the same provenance footer"


def test_parallel_crash_recovery_keeps_the_existing_output(
    tmp_path: Path,
    fixtures_dir: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A generated file discovered with its source must not become a new output."""
    from portolan_cli.add import add_files
    from portolan_cli.sync.checksums import compute_checksum
    from portolan_cli.versions import read_versions
    from tests.integration.test_collection_level_asset_workflow import _crashed_add

    with monkeypatch.context() as patched:
        root, collection = _crashed_add(tmp_path, fixtures_dir, patched)
    _geojson(collection / "roads.geojson", 333)
    _added, _skipped, failed = add_files(paths=[collection], catalog_root=root, workers=2)
    assert failed == []
    output = collection / "roads.parquet"
    assert sorted(collection.glob("*.parquet")) == [output]
    assert pq.read_table(output).column("value").to_pylist() == [333]
    versions = read_versions(collection / "versions.json").versions
    assert len(versions) == 1
    assert versions[0].assets[output.name].sha256 == compute_checksum(output)


def test_api_can_repeat_a_copy_of_a_previously_converted_parquet(tmp_path: Path) -> None:
    """An exact copy remains recognizable when its footer names an earlier source."""
    source = _geojson(tmp_path / "source" / "roads.geojson", 111)
    settings = VectorSettings(sort="none", add_bbox=False)
    parquet = api.convert_vector(source, source.parent, settings=settings)
    destination = tmp_path / "destination"
    destination.mkdir()
    first = api.convert_vector(parquet, destination, settings=settings)
    second = api.convert_vector(parquet, destination, settings=settings)
    assert second == first, "the API treated its byte copy as an unrelated conversion"
    assert second.read_bytes() == parquet.read_bytes()
