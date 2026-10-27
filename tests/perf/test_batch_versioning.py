"""Batch versioning writes versions.json once per collection (Issue #281).

The add API, not only the CLI, must batch the writes. A per-file write made
the old add O(n squared) in the size of versions.json. The scale gates in
``test_scale_invariance.py`` hold the same property for the CLI at any size.
This file once also compared the wall time of 10 and 20 files. That ratio
measured the runner, so the counts below replace it.
"""

from __future__ import annotations

import json
from typing import TYPE_CHECKING

import pytest

from portolan_cli import trace

if TYPE_CHECKING:
    from pathlib import Path

pytestmark = pytest.mark.integration


@pytest.fixture
def benchmark_catalog(tmp_path: Path) -> Path:
    """Create a catalog for benchmarking."""
    portolan_dir = tmp_path / ".portolan"
    portolan_dir.mkdir()
    (portolan_dir / "config.yaml").write_text("version: 1\n")
    # add_files rejects a collection with no license (issue #842).
    (portolan_dir / "metadata.yaml").write_text('license: "CC-BY-4.0"\n')

    catalog_data = {
        "type": "Catalog",
        "stac_version": "1.0.0",
        "id": "benchmark-catalog",
        "description": "Catalog for benchmarking",
        "links": [],
    }
    (tmp_path / "catalog.json").write_text(json.dumps(catalog_data, indent=2))

    return tmp_path


def create_geojson_files(directory: Path, count: int) -> list[Path]:
    """Create N minimal GeoJSON files for benchmarking."""
    files = []
    for i in range(count):
        item_dir = directory / f"item-{i:04d}"
        item_dir.mkdir(parents=True, exist_ok=True)

        geojson = item_dir / f"data-{i:04d}.geojson"
        geojson.write_text(
            json.dumps(
                {
                    "type": "FeatureCollection",
                    "features": [
                        {
                            "type": "Feature",
                            "geometry": {
                                "type": "Point",
                                "coordinates": [-122.4 + (i * 0.001), 37.8 + (i * 0.001)],
                            },
                            "properties": {"id": i, "name": f"Feature {i}"},
                        }
                    ],
                }
            )
        )
        files.append(geojson)

    return files


def _version_work(counts: dict[str, int]) -> dict[str, int]:
    return {name: counts[name] for name in ("versions_json.writes", "versions.published")}


class TestBatchVersioning:
    """Each add API call writes versions.json once per collection."""

    def test_batch_writes_versions_once_per_collection(
        self,
        benchmark_catalog: Path,
    ) -> None:
        """Verify batch approach writes versions.json once per collection.

        This is the key metric: regardless of file count, we should only
        write versions.json once per collection.
        """
        from portolan_cli.add import add_files

        collection_dir = benchmark_catalog / "test-collection"
        collection_dir.mkdir()

        # Create 20 files (enough to see the pattern)
        files = create_geojson_files(collection_dir, 20)

        with trace.tracing():
            add_files(
                paths=files,
                catalog_root=benchmark_catalog,
                collection_id="test-collection",
            )
            counts = trace.counters()

        # One write of the collection versions.json for 20 files. The fixture
        # catalog has no catalog-level versions.json to update.
        assert _version_work(counts) == {"versions_json.writes": 1, "versions.published": 1}

    def test_add_directory_uses_batch_versioning(
        self,
        benchmark_catalog: Path,
    ) -> None:
        """Verify add_directory also uses batch versioning."""
        from portolan_cli.add import add_directory

        collection_dir = benchmark_catalog / "dir-collection"
        collection_dir.mkdir()

        # Create files directly in collection dir (add_directory scans for them)
        for i in range(10):
            geojson = collection_dir / f"data-{i:03d}.geojson"
            geojson.write_text(
                json.dumps(
                    {
                        "type": "FeatureCollection",
                        "features": [
                            {
                                "type": "Feature",
                                "geometry": {"type": "Point", "coordinates": [i, i]},
                                "properties": {},
                            }
                        ],
                    }
                )
            )

        with trace.tracing():
            add_directory(
                path=collection_dir,
                catalog_root=benchmark_catalog,
                collection_id="dir-collection",
                recursive=False,
            )
            counts = trace.counters()

        assert _version_work(counts) == {"versions_json.writes": 1, "versions.published": 1}
