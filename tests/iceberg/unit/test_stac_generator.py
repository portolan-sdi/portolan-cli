"""Tests for STAC metadata generation from Iceberg tables (Phase 3 + 4).

Phase 3: table:* fields (STAC Table Extension) from Iceberg schema.
Phase 4: iceberg:* fields (STAC Iceberg Extension) from catalog/table state.
"""

import struct

import pyarrow as pa
import pyarrow.parquet as pq
import pytest


def _make_wkb_point(x: float, y: float) -> bytes:
    """Create a WKB point (little-endian)."""
    return struct.pack("<BIdd", 1, 1, x, y)


# --- Phase 3: STAC Table Extension fields ---


@pytest.mark.integration
def test_table_columns_from_iceberg_schema(iceberg_backend, iceberg_catalog, tmp_path):
    """table:columns should list all non-spatial columns with types."""
    table_data = pa.table(
        {
            "id": pa.array([1, 2], type=pa.int64()),
            "name": pa.array(["a", "b"], type=pa.string()),
            "value": pa.array([1.5, 2.5], type=pa.float64()),
        }
    )
    path = tmp_path / "data.parquet"
    pq.write_table(table_data, path)

    iceberg_backend.publish(
        collection="buildings",
        assets={"data.parquet": str(path)},
        schema={"columns": ["id", "name", "value"], "types": {}, "hash": "h1"},
        breaking=False,
        message="v1",
    )

    from portolan_cli.backends.iceberg.stac_generator import generate_table_metadata

    table = iceberg_catalog.load_table("portolake.buildings")
    metadata = generate_table_metadata(table)

    assert "table:columns" in metadata
    columns = metadata["table:columns"]
    col_names = [c["name"] for c in columns]
    assert "id" in col_names
    assert "name" in col_names
    assert "value" in col_names

    # Each column should have name and type
    for col in columns:
        assert "name" in col
        assert "type" in col


@pytest.mark.integration
def test_table_columns_have_correct_types(iceberg_backend, iceberg_catalog, tmp_path):
    """table:columns types should map from Iceberg types."""
    table_data = pa.table(
        {
            "id": pa.array([1], type=pa.int64()),
            "score": pa.array([9.5], type=pa.float64()),
            "label": pa.array(["x"], type=pa.string()),
        }
    )
    path = tmp_path / "data.parquet"
    pq.write_table(table_data, path)

    iceberg_backend.publish(
        collection="typed",
        assets={"data.parquet": str(path)},
        schema={"columns": [], "types": {}, "hash": "h1"},
        breaking=False,
        message="v1",
    )

    from portolan_cli.backends.iceberg.stac_generator import generate_table_metadata

    table = iceberg_catalog.load_table("portolake.typed")
    metadata = generate_table_metadata(table)

    col_map = {c["name"]: c["type"] for c in metadata["table:columns"]}
    assert col_map["id"] == "int64"
    assert col_map["score"] == "float64"
    assert col_map["label"] == "string"


@pytest.mark.integration
def test_table_row_count(iceberg_backend, iceberg_catalog, tmp_path):
    """table:row_count should reflect the number of rows in the current snapshot."""
    table_data = pa.table({"id": pa.array([1, 2, 3, 4, 5], type=pa.int64())})
    path = tmp_path / "data.parquet"
    pq.write_table(table_data, path)

    iceberg_backend.publish(
        collection="counted",
        assets={"data.parquet": str(path)},
        schema={"columns": ["id"], "types": {}, "hash": "h1"},
        breaking=False,
        message="v1",
    )

    from portolan_cli.backends.iceberg.stac_generator import generate_table_metadata

    table = iceberg_catalog.load_table("portolake.counted")
    metadata = generate_table_metadata(table)

    assert metadata["table:row_count"] == 5


@pytest.mark.integration
def test_table_primary_geometry_detected(iceberg_backend, iceberg_catalog, tmp_path):
    """table:primary_geometry should be set when a geometry column exists."""
    wkb_values = [_make_wkb_point(2.35, 48.85), _make_wkb_point(-73.99, 40.75)]
    table_data = pa.table(
        {
            "id": pa.array([1, 2], type=pa.int64()),
            "geometry": pa.array(wkb_values, type=pa.binary()),
        }
    )
    path = tmp_path / "geo.parquet"
    pq.write_table(table_data, path)

    iceberg_backend.publish(
        collection="geoplaces",
        assets={"geo.parquet": str(path)},
        schema={"columns": ["id", "geometry"], "types": {}, "hash": "h1"},
        breaking=False,
        message="v1",
    )

    from portolan_cli.backends.iceberg.stac_generator import generate_table_metadata

    table = iceberg_catalog.load_table("portolake.geoplaces")
    metadata = generate_table_metadata(table)

    assert metadata["table:primary_geometry"] == "geometry"


@pytest.mark.integration
def test_table_primary_geometry_none_without_geometry(iceberg_backend, iceberg_catalog, tmp_path):
    """table:primary_geometry should be None when no geometry column exists."""
    table_data = pa.table({"id": pa.array([1, 2], type=pa.int64())})
    path = tmp_path / "plain.parquet"
    pq.write_table(table_data, path)

    iceberg_backend.publish(
        collection="plain",
        assets={"plain.parquet": str(path)},
        schema={"columns": ["id"], "types": {}, "hash": "h1"},
        breaking=False,
        message="v1",
    )

    from portolan_cli.backends.iceberg.stac_generator import generate_table_metadata

    table = iceberg_catalog.load_table("portolake.plain")
    metadata = generate_table_metadata(table)

    assert metadata["table:primary_geometry"] is None


@pytest.mark.integration
def test_table_columns_exclude_spatial_derived(iceberg_backend, iceberg_catalog, tmp_path):
    """table:columns should exclude portolake-derived columns (geohash_*, bbox_*)."""
    wkb_values = [_make_wkb_point(2.35, 48.85)]
    table_data = pa.table(
        {
            "id": pa.array([1], type=pa.int64()),
            "geometry": pa.array(wkb_values, type=pa.binary()),
        }
    )
    path = tmp_path / "geo.parquet"
    pq.write_table(table_data, path)

    iceberg_backend.publish(
        collection="filtered",
        assets={"geo.parquet": str(path)},
        schema={"columns": ["id", "geometry"], "types": {}, "hash": "h1"},
        breaking=False,
        message="v1",
    )

    from portolan_cli.backends.iceberg.stac_generator import generate_table_metadata

    table = iceberg_catalog.load_table("portolake.filtered")
    metadata = generate_table_metadata(table)

    col_names = [c["name"] for c in metadata["table:columns"]]
    assert not any(n.startswith("geohash_") for n in col_names)
    assert not any(n.startswith("bbox_") for n in col_names)


@pytest.mark.integration
def test_table_row_count_updates_after_publish(iceberg_backend, iceberg_catalog, tmp_path):
    """table:row_count should update after publishing more data."""
    t1 = pa.table({"id": pa.array([1, 2], type=pa.int64())})
    p1 = tmp_path / "v1.parquet"
    pq.write_table(t1, p1)

    iceberg_backend.publish(
        collection="growing",
        assets={"data.parquet": str(p1)},
        schema={"columns": ["id"], "types": {}, "hash": "h1"},
        breaking=False,
        message="v1",
    )

    t2 = pa.table({"id": pa.array([3, 4, 5], type=pa.int64())})
    p2 = tmp_path / "v2.parquet"
    pq.write_table(t2, p2)

    iceberg_backend.publish(
        collection="growing",
        assets={"data.parquet": str(p2)},
        schema={"columns": ["id"], "types": {}, "hash": "h1"},
        breaking=False,
        message="v2",
    )

    from portolan_cli.backends.iceberg.stac_generator import generate_table_metadata

    table = iceberg_catalog.load_table("portolake.growing")
    metadata = generate_table_metadata(table)

    # Should have all rows from both publishes (append mode)
    assert metadata["table:row_count"] == 5


# --- Phase 4: STAC Iceberg Extension fields ---


@pytest.mark.integration
def test_iceberg_metadata_has_required_fields(iceberg_backend, iceberg_catalog, tmp_path):
    """generate_collection_metadata should include all iceberg:* fields."""
    table_data = pa.table({"id": pa.array([1, 2], type=pa.int64())})
    path = tmp_path / "data.parquet"
    pq.write_table(table_data, path)

    iceberg_backend.publish(
        collection="buildings",
        assets={"data.parquet": str(path)},
        schema={"columns": ["id"], "types": {}, "hash": "h1"},
        breaking=False,
        message="v1",
    )

    from portolan_cli.backends.iceberg.stac_generator import generate_collection_metadata

    table = iceberg_catalog.load_table("portolake.buildings")
    metadata = generate_collection_metadata(table)

    assert "iceberg:catalog_type" in metadata
    assert "iceberg:table_id" in metadata
    assert "iceberg:format_version" in metadata
    assert "iceberg:current_snapshot_id" in metadata
    assert "iceberg:partition_spec" in metadata


@pytest.mark.integration
def test_iceberg_catalog_type(iceberg_backend, iceberg_catalog, tmp_path):
    """iceberg:catalog_type should reflect the catalog backend type."""
    table_data = pa.table({"id": pa.array([1], type=pa.int64())})
    path = tmp_path / "data.parquet"
    pq.write_table(table_data, path)

    iceberg_backend.publish(
        collection="typed",
        assets={"data.parquet": str(path)},
        schema={"columns": [], "types": {}, "hash": "h1"},
        breaking=False,
        message="v1",
    )

    from portolan_cli.backends.iceberg.stac_generator import generate_collection_metadata

    table = iceberg_catalog.load_table("portolake.typed")
    metadata = generate_collection_metadata(table)

    assert metadata["iceberg:catalog_type"] == "sql"


@pytest.mark.integration
def test_iceberg_table_id(iceberg_backend, iceberg_catalog, tmp_path):
    """iceberg:table_id should be the fully qualified table identifier."""
    table_data = pa.table({"id": pa.array([1], type=pa.int64())})
    path = tmp_path / "data.parquet"
    pq.write_table(table_data, path)

    iceberg_backend.publish(
        collection="mydata",
        assets={"data.parquet": str(path)},
        schema={"columns": [], "types": {}, "hash": "h1"},
        breaking=False,
        message="v1",
    )

    from portolan_cli.backends.iceberg.stac_generator import generate_collection_metadata

    table = iceberg_catalog.load_table("portolake.mydata")
    metadata = generate_collection_metadata(table)

    assert metadata["iceberg:table_id"] == "portolake.mydata"


@pytest.mark.integration
def test_iceberg_format_version(iceberg_backend, iceberg_catalog, tmp_path):
    """iceberg:format_version should be an integer (1 or 2)."""
    table_data = pa.table({"id": pa.array([1], type=pa.int64())})
    path = tmp_path / "data.parquet"
    pq.write_table(table_data, path)

    iceberg_backend.publish(
        collection="versioned",
        assets={"data.parquet": str(path)},
        schema={"columns": [], "types": {}, "hash": "h1"},
        breaking=False,
        message="v1",
    )

    from portolan_cli.backends.iceberg.stac_generator import generate_collection_metadata

    table = iceberg_catalog.load_table("portolake.versioned")
    metadata = generate_collection_metadata(table)

    assert metadata["iceberg:format_version"] in (1, 2)


@pytest.mark.integration
def test_iceberg_current_snapshot_id(iceberg_backend, iceberg_catalog, tmp_path):
    """iceberg:current_snapshot_id should be a positive integer."""
    table_data = pa.table({"id": pa.array([1], type=pa.int64())})
    path = tmp_path / "data.parquet"
    pq.write_table(table_data, path)

    iceberg_backend.publish(
        collection="snapped",
        assets={"data.parquet": str(path)},
        schema={"columns": [], "types": {}, "hash": "h1"},
        breaking=False,
        message="v1",
    )

    from portolan_cli.backends.iceberg.stac_generator import generate_collection_metadata

    table = iceberg_catalog.load_table("portolake.snapped")
    metadata = generate_collection_metadata(table)

    assert isinstance(metadata["iceberg:current_snapshot_id"], int)
    assert metadata["iceberg:current_snapshot_id"] > 0


@pytest.mark.integration
def test_iceberg_partition_spec_empty_for_unpartitioned(iceberg_backend, iceberg_catalog, tmp_path):
    """iceberg:partition_spec should be empty list for unpartitioned tables."""
    table_data = pa.table({"id": pa.array([1], type=pa.int64())})
    path = tmp_path / "data.parquet"
    pq.write_table(table_data, path)

    iceberg_backend.publish(
        collection="unpart",
        assets={"data.parquet": str(path)},
        schema={"columns": [], "types": {}, "hash": "h1"},
        breaking=False,
        message="v1",
    )

    from portolan_cli.backends.iceberg.stac_generator import generate_collection_metadata

    table = iceberg_catalog.load_table("portolake.unpart")
    metadata = generate_collection_metadata(table)

    assert metadata["iceberg:partition_spec"] == []


@pytest.mark.integration
def test_collection_metadata_includes_table_fields(iceberg_backend, iceberg_catalog, tmp_path):
    """generate_collection_metadata should include both table:* and iceberg:* fields."""
    table_data = pa.table(
        {
            "id": pa.array([1, 2], type=pa.int64()),
            "name": pa.array(["a", "b"], type=pa.string()),
        }
    )
    path = tmp_path / "data.parquet"
    pq.write_table(table_data, path)

    iceberg_backend.publish(
        collection="combined",
        assets={"data.parquet": str(path)},
        schema={"columns": ["id", "name"], "types": {}, "hash": "h1"},
        breaking=False,
        message="v1",
    )

    from portolan_cli.backends.iceberg.stac_generator import generate_collection_metadata

    table = iceberg_catalog.load_table("portolake.combined")
    metadata = generate_collection_metadata(table)

    # Layer 1: table:*
    assert "table:columns" in metadata
    assert "table:row_count" in metadata
    assert "table:primary_geometry" in metadata

    # Layer 2: iceberg:*
    assert "iceberg:catalog_type" in metadata
    assert "iceberg:table_id" in metadata

    # stac_extensions and assets are no longer in the return dict --
    # they are set via pystac APIs in on_post_add()
    assert "stac_extensions" not in metadata
    assert "assets" not in metadata


@pytest.mark.integration
def test_iceberg_snapshot_id_updates_after_publish(iceberg_backend, iceberg_catalog, tmp_path):
    """iceberg:current_snapshot_id should change after a new publish."""
    t1 = pa.table({"id": pa.array([1], type=pa.int64())})
    p1 = tmp_path / "v1.parquet"
    pq.write_table(t1, p1)

    iceberg_backend.publish(
        collection="updating",
        assets={"data.parquet": str(p1)},
        schema={"columns": ["id"], "types": {}, "hash": "h1"},
        breaking=False,
        message="v1",
    )

    from portolan_cli.backends.iceberg.stac_generator import generate_collection_metadata

    table = iceberg_catalog.load_table("portolake.updating")
    snap1 = generate_collection_metadata(table)["iceberg:current_snapshot_id"]

    t2 = pa.table({"id": pa.array([2], type=pa.int64())})
    p2 = tmp_path / "v2.parquet"
    pq.write_table(t2, p2)

    iceberg_backend.publish(
        collection="updating",
        assets={"data.parquet": str(p2)},
        schema={"columns": ["id"], "types": {}, "hash": "h1"},
        breaking=False,
        message="v2",
    )

    table = iceberg_catalog.load_table("portolake.updating")
    snap2 = generate_collection_metadata(table)["iceberg:current_snapshot_id"]

    assert snap1 != snap2


@pytest.mark.integration
def test_get_stac_metadata_from_backend(iceberg_backend, tmp_path):
    """IcebergBackend.get_stac_metadata() should return combined metadata."""
    table_data = pa.table({"id": pa.array([1, 2, 3], type=pa.int64())})
    path = tmp_path / "data.parquet"
    pq.write_table(table_data, path)

    iceberg_backend.publish(
        collection="via-backend",
        assets={"data.parquet": str(path)},
        schema={"columns": ["id"], "types": {}, "hash": "h1"},
        breaking=False,
        message="v1",
    )

    metadata = iceberg_backend.get_stac_metadata("via-backend")

    assert metadata["table:row_count"] == 3
    assert "iceberg:table_id" in metadata
    # stac_extensions no longer in return dict (set via pystac API in on_post_add)
    assert "stac_extensions" not in metadata


@pytest.mark.integration
def test_generate_collection_metadata_excludes_stac_extensions(
    iceberg_backend, iceberg_catalog, tmp_path
):
    """generate_collection_metadata should not include stac_extensions (pystac manages them)."""
    table_data = pa.table({"id": pa.array([1], type=pa.int64())})
    path = tmp_path / "data.parquet"
    pq.write_table(table_data, path)

    iceberg_backend.publish(
        collection="no-ext",
        assets={"data.parquet": str(path)},
        schema={"columns": ["id"], "types": {}, "hash": "h1"},
        breaking=False,
        message="v1",
    )

    from portolan_cli.backends.iceberg.stac_generator import generate_collection_metadata

    table = iceberg_catalog.load_table("portolake.no-ext")
    metadata = generate_collection_metadata(table)

    assert "stac_extensions" not in metadata


@pytest.mark.integration
def test_generate_collection_metadata_excludes_assets(iceberg_backend, iceberg_catalog, tmp_path):
    """generate_collection_metadata should not include assets (pystac manages them)."""
    table_data = pa.table({"id": pa.array([1], type=pa.int64())})
    path = tmp_path / "data.parquet"
    pq.write_table(table_data, path)

    iceberg_backend.publish(
        collection="no-assets",
        assets={"data.parquet": str(path)},
        schema={"columns": ["id"], "types": {}, "hash": "h1"},
        breaking=False,
        message="v1",
    )

    from portolan_cli.backends.iceberg.stac_generator import generate_collection_metadata

    table = iceberg_catalog.load_table("portolake.no-assets")
    metadata = generate_collection_metadata(table)

    assert "assets" not in metadata
