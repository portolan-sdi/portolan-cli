"""CodSpeed benchmarks for the pure-Python paths that every add or check runs.
They catch a slower body of the same call; the tests/perf gates count the calls.
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Any

import numpy as np
import pytest

from portolan_core.config import load_merged_config, load_merged_metadata
from portolan_core.versions import (
    Asset,
    VersionsFile,
    add_version,
    read_versions,
    write_versions,
)
from tests.perf.scenarios import seeded_rng, write_vector

if TYPE_CHECKING:
    from collections.abc import Callable
    from pathlib import Path

pytestmark = pytest.mark.benchmark

VERSIONS_ASSETS = 5_000
THUMBNAIL_POLYGONS = 1_000


@pytest.fixture
def nested_catalog(tmp_path: Path) -> Path:
    """A catalog with config.yaml and metadata.yaml at the root and three levels below."""
    level = tmp_path
    for depth in range(4):
        portolan = level / ".portolan"
        portolan.mkdir(parents=True)
        (portolan / "config.yaml").write_text(
            f"conversion:\n  vector:\n    sort: hilbert\n    compression_level: {depth + 1}\n"
            "thumbnails:\n  max_size: 512\n",
            encoding="utf-8",
        )
        (portolan / "metadata.yaml").write_text(
            f'license: "CC-BY-4.0"\ntitle: "Level {depth}"\n'
            "contact:\n  name: Data team\n  email: data@example.org\n"
            "keywords: [buildings, footprints, open data]\n",
            encoding="utf-8",
        )
        level /= f"level{depth + 1}"
    return tmp_path


def test_merged_config(nested_catalog: Path, benchmark: Callable[..., Any]) -> None:
    collection = nested_catalog / "level1" / "level2" / "level3"

    merged = benchmark(load_merged_config, collection, nested_catalog)

    assert merged["conversion"]["vector"]["compression_level"] == 4


def test_merged_metadata(nested_catalog: Path, benchmark: Callable[..., Any]) -> None:
    collection = nested_catalog / "level1" / "level2" / "level3"

    merged = benchmark(load_merged_metadata, collection, nested_catalog)

    assert merged["title"] == "Level 3"


@pytest.fixture
def large_versions_file(tmp_path: Path) -> Path:
    """A versions.json whose current version lists 5,000 assets."""
    assets = {
        f"item{i:05d}/data.tif": Asset(
            sha256=f"{i:064x}",
            size_bytes=1_000 + i,
            href=f"scenes/item{i:05d}/data.tif",
            mtime=1_700_000_000.0 + i,
        )
        for i in range(VERSIONS_ASSETS)
    }
    versions = add_version(
        VersionsFile(spec_version="1.0.0", current_version=None, versions=[]),
        version="1.0.0",
        assets=assets,
        breaking=False,
    )
    path = tmp_path / "versions.json"
    write_versions(path, versions)
    return path


def test_read_versions_5000_assets(
    large_versions_file: Path, benchmark: Callable[..., Any]
) -> None:
    parsed = benchmark(read_versions, large_versions_file)

    assert isinstance(parsed, VersionsFile)
    assert len(parsed.versions[-1].assets) == VERSIONS_ASSETS


def test_vector_thumbnail_render(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, benchmark: Callable[..., Any]
) -> None:
    import contextily

    from portolan_cli.viz import thumbnail

    tile = np.full((256, 256, 4), 235, dtype=np.uint8)
    monkeypatch.setattr(thumbnail, "_get_tile", lambda _url: tile.copy())
    monkeypatch.setattr(thumbnail, "_ensure_contextily", lambda: contextily)
    source = tmp_path / "buildings.parquet"
    write_vector(source, THUMBNAIL_POLYGONS, seeded_rng(992, 0))

    output = benchmark(
        thumbnail.generate_thumbnail_from_geoparquet, source, thumbnail.ThumbnailConfig()
    )

    assert output is not None
    assert output.stat().st_size > 0
