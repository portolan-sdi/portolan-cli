"""Unit tests for plain-Parquet table metadata (issue #749).

Covers the three pieces the tabular path needs to answer PTL-DAT-015:
reading columns and temporal bounds from a Parquet footer, writing
``table:columns`` to the right place in ``collection.json``, and filling a
temporal extent without overwriting one that is already set.
"""

from __future__ import annotations

from datetime import date, datetime, timezone
from pathlib import Path
from typing import Any

import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from portolan_cli.metadata.tabular import extract_tabular_metadata
from portolan_cli.stac import document_tabular_table, set_temporal_extent

TABLE_EXTENSION = "https://stac-extensions.github.io/table/v1.2.0/schema.json"


def _write(path: Path, table: pa.Table) -> Path:
    pq.write_table(table, path)
    return path


class TestExtractTabularMetadata:
    """What the extractor reads, and what it refuses to read."""

    @pytest.mark.unit
    def test_reads_column_names_types_and_row_count(self, tmp_path: Path) -> None:
        source = _write(
            tmp_path / "census.parquet",
            pa.table({"tract_id": ["001", "002"], "population": [5000, 7500]}),
        )

        metadata = extract_tabular_metadata(source)

        assert metadata is not None
        assert metadata.schema == {"tract_id": "string", "population": "int64"}
        assert metadata.row_count == 2

    @pytest.mark.unit
    def test_returns_none_for_geoparquet(self, tmp_path: Path) -> None:
        """GeoParquet is not tabular: the geospatial storage rules own it.

        A GeoParquet stamped here would overwrite the schema
        ``add_table_extension`` derives from the same file during finalization.
        """
        table = pa.table({"id": ["a"]}).replace_schema_metadata(
            {b"geo": b'{"version": "1.1.0", "primary_column": "geometry", "columns": {}}'}
        )
        source = _write(tmp_path / "roads.parquet", table)

        assert extract_tabular_metadata(source) is None

    @pytest.mark.unit
    def test_returns_none_for_a_non_parquet_suffix(self, tmp_path: Path) -> None:
        source = tmp_path / "census.csv"
        source.write_text("tract_id,population\n001,5000\n", encoding="utf-8")

        assert extract_tabular_metadata(source) is None

    @pytest.mark.unit
    def test_returns_none_for_an_unreadable_parquet(self, tmp_path: Path) -> None:
        """A corrupt file yields nothing rather than raising: `add` stays atomic."""
        source = tmp_path / "broken.parquet"
        source.write_bytes(b"not parquet at all")

        assert extract_tabular_metadata(source) is None


class TestTemporalInterval:
    """The interval comes from column statistics, so no row data is read."""

    @pytest.mark.unit
    def test_is_none_without_a_temporal_column(self, tmp_path: Path) -> None:
        source = _write(tmp_path / "census.parquet", pa.table({"population": [5000]}))

        metadata = extract_tabular_metadata(source)

        assert metadata is not None
        assert metadata.temporal_interval is None

    @pytest.mark.unit
    def test_spans_the_min_and_max_of_a_timestamp_column(self, tmp_path: Path) -> None:
        source = _write(
            tmp_path / "census.parquet",
            pa.table(
                {
                    "surveyed_at": pa.array(
                        [datetime(2020, 9, 30), datetime(2020, 4, 1), datetime(2020, 6, 15)],
                        pa.timestamp("us"),
                    )
                }
            ),
        )

        metadata = extract_tabular_metadata(source)

        assert metadata is not None
        assert metadata.temporal_interval == (
            datetime(2020, 4, 1, tzinfo=timezone.utc),
            datetime(2020, 9, 30, tzinfo=timezone.utc),
        )

    @pytest.mark.unit
    def test_reads_a_date_column_as_midnight_utc(self, tmp_path: Path) -> None:
        """STAC needs RFC 3339, which a bare calendar date cannot express."""
        source = _write(
            tmp_path / "census.parquet",
            pa.table({"day": pa.array([date(2019, 1, 1), date(2022, 2, 2)], pa.date32())}),
        )

        metadata = extract_tabular_metadata(source)

        assert metadata is not None
        assert metadata.temporal_interval == (
            datetime(2019, 1, 1, tzinfo=timezone.utc),
            datetime(2022, 2, 2, tzinfo=timezone.utc),
        )

    @pytest.mark.unit
    def test_spans_every_temporal_column(self, tmp_path: Path) -> None:
        """Two time columns bound one extent between them, widest wins."""
        source = _write(
            tmp_path / "census.parquet",
            pa.table(
                {
                    "opened": pa.array([datetime(2020, 4, 1)], pa.timestamp("us")),
                    "closed": pa.array([datetime(2023, 8, 9)], pa.timestamp("us")),
                }
            ),
        )

        metadata = extract_tabular_metadata(source)

        assert metadata is not None
        assert metadata.temporal_interval == (
            datetime(2020, 4, 1, tzinfo=timezone.utc),
            datetime(2023, 8, 9, tzinfo=timezone.utc),
        )

    @pytest.mark.unit
    def test_converts_a_zoned_timestamp_to_utc(self, tmp_path: Path) -> None:
        source = _write(
            tmp_path / "census.parquet",
            pa.table(
                {
                    "surveyed_at": pa.array(
                        [datetime(2020, 4, 1, 12, 0)],
                        pa.timestamp("us", tz="America/New_York"),
                    )
                }
            ),
        )

        metadata = extract_tabular_metadata(source)

        assert metadata is not None
        assert metadata.temporal_interval is not None
        start, end = metadata.temporal_interval
        assert start.tzinfo is not None
        assert start.utcoffset() == end.utcoffset()

    @pytest.mark.unit
    def test_ignores_a_time_of_day_column(self, tmp_path: Path) -> None:
        """A clock time carries no calendar date, so it cannot bound an extent."""
        source = _write(
            tmp_path / "census.parquet",
            pa.table({"opens": pa.array([43200000000], pa.time64("us"))}),
        )

        metadata = extract_tabular_metadata(source)

        assert metadata is not None
        assert metadata.temporal_interval is None


def _collection(assets: dict[str, Any]) -> dict[str, Any]:
    return {"assets": assets, "stac_extensions": []}


class TestDocumentTabularTable:
    """Where the columns land, and what survives a re-add."""

    @pytest.mark.unit
    def test_writes_to_the_collection_when_the_parquet_stands_alone(self, tmp_path: Path) -> None:
        source = _write(tmp_path / "census.parquet", pa.table({"tract_id": ["001"]}))
        metadata = extract_tabular_metadata(source)
        assert metadata is not None
        collection = _collection({"census": {"href": "./census.parquet", "roles": ["data"]}})

        document_tabular_table(collection, "census", metadata)

        assert collection["table:columns"] == [{"name": "tract_id", "type": "string"}]
        assert collection["table:row_count"] == 1
        assert "table:columns" not in collection["assets"]["census"]
        assert collection["stac_extensions"] == [TABLE_EXTENSION]

    @pytest.mark.unit
    def test_writes_to_the_asset_when_another_parquet_owns_the_collection(
        self, tmp_path: Path
    ) -> None:
        """A geo collection's own schema must survive a companion tabular file."""
        source = _write(tmp_path / "lanes.parquet", pa.table({"road_id": ["a"]}))
        metadata = extract_tabular_metadata(source)
        assert metadata is not None
        collection = _collection(
            {
                "roads": {"href": "./roads.parquet", "roles": ["data"]},
                "lanes": {"href": "./lanes.parquet", "roles": ["data"]},
            }
        )
        collection["table:columns"] = [{"name": "geometry", "type": "binary"}]

        document_tabular_table(collection, "lanes", metadata)

        assert collection["table:columns"] == [{"name": "geometry", "type": "binary"}]
        assert collection["assets"]["lanes"]["table:columns"] == [
            {"name": "road_id", "type": "string"}
        ]

    @pytest.mark.unit
    def test_a_companion_csv_does_not_displace_the_collection(self, tmp_path: Path) -> None:
        """Only another Parquet claims the collection level.

        Converting a CSV tracks both files as data assets. Counting the CSV
        would push the schema onto the asset on re-add and strand a stale copy
        at collection level.
        """
        source = _write(tmp_path / "census.parquet", pa.table({"tract_id": ["001"]}))
        metadata = extract_tabular_metadata(source)
        assert metadata is not None
        collection = _collection(
            {
                "census": {"href": "./census.parquet", "roles": ["data"]},
                "census.csv": {"href": "./census.csv", "roles": ["data"]},
            }
        )

        document_tabular_table(collection, "census", metadata)

        assert collection["table:columns"] == [{"name": "tract_id", "type": "string"}]
        assert "table:columns" not in collection["assets"]["census"]

    @pytest.mark.unit
    def test_a_thumbnail_does_not_displace_the_collection(self, tmp_path: Path) -> None:
        source = _write(tmp_path / "census.parquet", pa.table({"tract_id": ["001"]}))
        metadata = extract_tabular_metadata(source)
        assert metadata is not None
        collection = _collection(
            {
                "census": {"href": "./census.parquet", "roles": ["data"]},
                "thumbnail": {"href": "./census.thumb.png", "roles": ["thumbnail"]},
            }
        )

        document_tabular_table(collection, "census", metadata)

        assert collection["table:columns"] == [{"name": "tract_id", "type": "string"}]

    @pytest.mark.unit
    def test_preserves_a_human_authored_column_description(self, tmp_path: Path) -> None:
        """`add` merges, it never regenerates."""
        source = _write(
            tmp_path / "census.parquet",
            pa.table({"tract_id": ["001"], "population": [5000]}),
        )
        metadata = extract_tabular_metadata(source)
        assert metadata is not None
        collection = _collection({"census": {"href": "./census.parquet", "roles": ["data"]}})
        collection["table:columns"] = [
            {"name": "tract_id", "type": "string", "description": "Census tract identifier"}
        ]

        document_tabular_table(collection, "census", metadata)

        assert collection["table:columns"] == [
            {"name": "tract_id", "type": "string", "description": "Census tract identifier"},
            {"name": "population", "type": "int64"},
        ]

    @pytest.mark.unit
    def test_declares_the_table_extension_once(self, tmp_path: Path) -> None:
        source = _write(tmp_path / "census.parquet", pa.table({"tract_id": ["001"]}))
        metadata = extract_tabular_metadata(source)
        assert metadata is not None
        collection = _collection({"census": {"href": "./census.parquet", "roles": ["data"]}})

        document_tabular_table(collection, "census", metadata)
        document_tabular_table(collection, "census", metadata)

        assert collection["stac_extensions"].count(TABLE_EXTENSION) == 1


class TestSetTemporalExtent:
    """A derived interval fills a gap; it never overrules a stated bound."""

    @pytest.mark.unit
    def test_fills_the_open_sentinel(self) -> None:
        collection: dict[str, Any] = {"extent": {"temporal": {"interval": [[None, None]]}}}

        set_temporal_extent(
            collection,
            (
                datetime(2020, 4, 1, tzinfo=timezone.utc),
                datetime(2020, 9, 30, tzinfo=timezone.utc),
            ),
        )

        assert collection["extent"]["temporal"]["interval"] == [
            ["2020-04-01T00:00:00Z", "2020-09-30T00:00:00Z"]
        ]

    @pytest.mark.unit
    def test_leaves_an_existing_bound_alone(self) -> None:
        """A stated start came from a human or from items, and outranks statistics."""
        collection: dict[str, Any] = {
            "extent": {"temporal": {"interval": [["1999-01-01T00:00:00Z", None]]}}
        }

        set_temporal_extent(
            collection,
            (
                datetime(2020, 4, 1, tzinfo=timezone.utc),
                datetime(2020, 9, 30, tzinfo=timezone.utc),
            ),
        )

        assert collection["extent"]["temporal"]["interval"] == [["1999-01-01T00:00:00Z", None]]

    @pytest.mark.unit
    def test_builds_the_extent_when_the_collection_has_none(self) -> None:
        collection: dict[str, Any] = {}

        set_temporal_extent(
            collection,
            (
                datetime(2020, 4, 1, tzinfo=timezone.utc),
                datetime(2020, 9, 30, tzinfo=timezone.utc),
            ),
        )

        assert collection["extent"]["temporal"]["interval"] == [
            ["2020-04-01T00:00:00Z", "2020-09-30T00:00:00Z"]
        ]
