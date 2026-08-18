"""The add flow never re-ingests the item mirror it publishes (issue #654).

With mirror generation on by default, every item-bearing collection gains an
``items.parquet`` in its collection root after the first add. A re-add used
to collect that derived file as vector data, which broke metadata
aggregation on partitioned collections ("no items have valid bboxes"). The
spec reserves ``items.parquet`` in the collection root for the mirror, so
discovery skips the name outright.
"""

from __future__ import annotations

from pathlib import Path

import pytest

from portolan_cli.add import _collect_files_for_add


def _touch_parquet(path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_bytes(b"PAR1")


class TestAddSkipsItemMirror:
    @pytest.mark.unit
    def test_items_parquet_is_not_collected(self, tmp_path: Path) -> None:
        """items.parquet in the collection root is skipped; real data is kept."""
        collection_dir = tmp_path / "sites"
        _touch_parquet(collection_dir / "items.parquet")
        _touch_parquet(collection_dir / "sites.parquet")

        skipped: list[Path] = []
        collected = _collect_files_for_add(
            [collection_dir],
            tmp_path,
            None,
            skipped,
            set(),
            force=True,
        )

        collected_names = [p.name for p, _ in collected]
        assert "items.parquet" not in collected_names
        assert "sites.parquet" in collected_names

    @pytest.mark.unit
    def test_mirror_asset_excluded_from_table_aggregation(self, tmp_path: Path) -> None:
        """The tracked mirror never joins parquet metadata aggregation.

        On a partitioned collection the data is tracked by a glob href, so
        the mirror was the only asset the disk scan matched. Its metadata
        carries no bbox, and aggregation then failed with "no items have
        valid bboxes".
        """
        import pyarrow as pa
        import pyarrow.parquet as pq
        import pystac

        from portolan_cli.finalization import _collect_parquet_metadata_from_disk

        collection_dir = tmp_path / "sites"
        collection_dir.mkdir()
        pq.write_table(pa.table({"id": [1, 2]}), collection_dir / "items.parquet")

        collection = pystac.Collection(
            id="sites",
            description="d",
            extent=pystac.Extent(
                spatial=pystac.SpatialExtent([[0, 0, 1, 1]]),
                temporal=pystac.TemporalExtent([[None, None]]),
            ),
        )
        collection.add_asset(
            "geoparquet-items",
            pystac.Asset(href="./items.parquet", roles=["collection-mirror"]),
        )

        assert _collect_parquet_metadata_from_disk(collection_dir, collection) == []

    @pytest.mark.unit
    def test_items_parquet_passed_directly_is_skipped(self, tmp_path: Path) -> None:
        """An explicit path to the mirror is skipped, not ingested."""
        collection_dir = tmp_path / "sites"
        mirror = collection_dir / "items.parquet"
        _touch_parquet(mirror)

        skipped: list[Path] = []
        collected = _collect_files_for_add(
            [mirror],
            tmp_path,
            None,
            skipped,
            set(),
            force=True,
        )

        assert collected == []
