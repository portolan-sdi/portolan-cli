"""GeoParquet detection reads the raw Parquet footer, not the Arrow schema.

pyarrow rebuilds the Arrow schema metadata from the ``ARROW:schema`` footer blob
when a file carries one, and drops every other footer key while it does so. A
writer that appends ``geo`` at close produces a valid GeoParquet whose ``geo``
key pyarrow hides. Portolan must still classify the file as GeoParquet
(issue #864).
"""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any

import pyarrow as pa
import pyarrow.parquet as pq
import pytest
from shapely import to_wkb
from shapely.geometry import Point

pytestmark = pytest.mark.unit

GEO_METADATA: dict[str, Any] = {
    "version": "1.1.0",
    "primary_column": "geometry",
    "columns": {
        "geometry": {
            "encoding": "WKB",
            "geometry_types": ["Point"],
            "crs": None,
            "bbox": [1.0, 2.0, 3.0, 4.0],
        }
    },
}


def _write_appended_geo_parquet(path: Path, *, extra: dict[str, str] | None = None) -> Path:
    """Write a GeoParquet whose ``geo`` key lands after the writer opened.

    ``store_schema`` stays at its default, so pyarrow writes ``ARROW:schema``.
    ``add_key_value_metadata`` then appends ``geo`` at close, which is the only
    point at which a writer knows bbox and geometry_types.
    """
    table = pa.table(
        {
            "name": ["a", "b"],
            "geometry": pa.array([to_wkb(Point(1, 2)), to_wkb(Point(3, 4))], pa.binary()),
        }
    )
    writer = pq.ParquetWriter(str(path), table.schema)
    writer.write_table(table)
    writer.add_key_value_metadata({"geo": json.dumps(GEO_METADATA), **(extra or {})})
    writer.close()
    return path


@pytest.fixture
def appended_geo_parquet(tmp_path: Path) -> Path:
    """A GeoParquet with both an ``ARROW:schema`` blob and an appended ``geo`` key."""
    return _write_appended_geo_parquet(tmp_path / "appended.parquet")


def test_footer_hides_geo_from_the_arrow_schema(appended_geo_parquet: Path) -> None:
    """The fixture reproduces the shape the bug needs.

    This guards the fixture itself. If pyarrow ever surfaces the appended key on
    the reconstructed schema, the other tests stop covering issue #864.
    """
    metadata = pq.read_metadata(str(appended_geo_parquet))
    assert b"geo" in metadata.metadata
    assert b"ARROW:schema" in metadata.metadata
    assert b"geo" not in (metadata.schema.to_arrow_schema().metadata or {})


def test_formats_is_geoparquet(appended_geo_parquet: Path) -> None:
    """``formats.is_geoparquet`` reports True. It returned False before the fix."""
    from portolan_cli.formats import is_geoparquet

    assert is_geoparquet(appended_geo_parquet) is True


def test_classify_is_geoparquet(appended_geo_parquet: Path) -> None:
    """``scan.classify.is_geoparquet`` reports True. It returned False before the fix."""
    from portolan_cli.scan.classify import is_geoparquet

    assert is_geoparquet(appended_geo_parquet) is True


def test_cloud_native_status_is_geoparquet(appended_geo_parquet: Path) -> None:
    """``get_cloud_native_status`` names the format GeoParquet, not Parquet."""
    from portolan_cli.formats import CloudNativeStatus, get_cloud_native_status

    info = get_cloud_native_status(appended_geo_parquet)
    assert info.display_name == "GeoParquet"
    assert info.status == CloudNativeStatus.CLOUD_NATIVE


def test_scan_classifies_the_file_as_a_geo_asset(appended_geo_parquet: Path) -> None:
    """``classify_file`` returns GEO_ASSET. It returned TABULAR_DATA before the fix."""
    from portolan_cli.scan.classify import FileCategory, classify_file

    category, _, _ = classify_file(appended_geo_parquet)
    assert category == FileCategory.GEO_ASSET


def test_extract_geoparquet_metadata_reads_the_geo_key(appended_geo_parquet: Path) -> None:
    """The extractor finds bbox and geometry type. Both were None before the fix."""
    from portolan_cli.metadata.geoparquet import extract_geoparquet_metadata

    metadata = extract_geoparquet_metadata(appended_geo_parquet)
    assert metadata.bbox == (1.0, 2.0, 3.0, 4.0)
    assert metadata.geometry_type == "Point"


def test_spatial_layout_sees_geoparquet(appended_geo_parquet: Path) -> None:
    """``read_spatial_layout`` reports a GeoParquet. It reported False before the fix."""
    from portolan_cli.metadata.geoparquet import read_spatial_layout

    assert read_spatial_layout(appended_geo_parquet).is_geoparquet is True


def test_rewrite_fidelity_reads_the_crs(tmp_path: Path) -> None:
    """``read_rewrite_fidelity`` reads the declared CRS out of the footer."""
    from portolan_cli.metadata.geoparquet import read_rewrite_fidelity

    path = _write_appended_geo_parquet(tmp_path / "fidelity.parquet")
    fidelity = read_rewrite_fidelity(path)
    assert fidelity is not None
    assert fidelity.row_count == 2
    assert fidelity.columns == frozenset({"name", "geometry"})


def test_extra_schema_metadata_keeps_publisher_keys(tmp_path: Path) -> None:
    """A publisher key in the footer survives. It was invisible before the fix."""
    from portolan_cli.metadata.geoparquet import read_extra_schema_metadata

    path = _write_appended_geo_parquet(tmp_path / "extra.parquet", extra={"source": "kiln"})
    preserved = read_extra_schema_metadata(path)
    assert preserved[b"source"] == b"kiln"


def test_extra_schema_metadata_drops_geo_and_arrow_schema(tmp_path: Path) -> None:
    """``geo`` and ``ARROW:schema`` never come back onto a rewritten file.

    geoparquet-io writes a fresh ``geo`` key, and a restored ``ARROW:schema``
    blob would describe the columns the source had, not the ones the rewrite
    produced.
    """
    from portolan_cli.metadata.geoparquet import read_extra_schema_metadata

    path = _write_appended_geo_parquet(tmp_path / "reserved.parquet", extra={"source": "kiln"})
    preserved = read_extra_schema_metadata(path)
    assert b"geo" not in preserved
    assert b"ARROW:schema" not in preserved


def test_tabular_metadata_skips_the_file(appended_geo_parquet: Path) -> None:
    """The tabular extractor returns None. It returned a schema before the fix."""
    from portolan_cli.metadata.tabular import extract_tabular_metadata

    assert extract_tabular_metadata(appended_geo_parquet) is None


def test_thumbnail_bounds_come_from_the_declared_bbox(
    appended_geo_parquet: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """The thumbnail bbox comes from metadata, not from a data read.

    The fallback is replaced by a raiser. Before the fix the metadata lookup
    found no ``geo`` key and the fallback ran, so this test failed.
    """
    from portolan_cli.viz import thumbnail

    def _no_fallback(_: Path) -> None:
        raise AssertionError("bounds fell back to a data read")

    monkeypatch.setattr(thumbnail, "_read_geoparquet_bounds_from_data", _no_fallback)
    assert thumbnail._read_geoparquet_bounds(appended_geo_parquet) == (1.0, 2.0, 3.0, 4.0)


def test_thumbnail_sample_reads_the_crs(appended_geo_parquet: Path) -> None:
    """The thumbnail sampler builds a frame.

    This site is not affected. pyarrow merges the extra footer keys into the
    table schema when it reads row groups, unlike ``schema_arrow``. The test
    pins that difference so a later refactor does not lose it.
    """
    pytest.importorskip("geopandas")
    from portolan_cli.viz.thumbnail import _sample_geoparquet

    frame = _sample_geoparquet(appended_geo_parquet, max_features=2)
    assert frame is not None
    assert len(frame) == 2
