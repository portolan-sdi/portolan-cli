"""Integration tests for export after CLI add.

Verifies that exported GeoParquet matches original data and excludes
derived columns (geohash, bbox).
"""

import pyarrow.parquet as pq
import pytest

from tests.iceberg.integration.conftest import (
    invoke_add,
    load_test_catalog,
    place_geojson_in_collection,
)


@pytest.mark.integration
def test_export_after_add_excludes_derived_columns(initialized_iceberg_catalog, runner, tmp_path):
    """Exported GeoParquet should not contain geohash_* or bbox_* columns."""
    catalog_root = initialized_iceberg_catalog
    geojson = place_geojson_in_collection(catalog_root, "export_test")

    result = invoke_add(runner, catalog_root, geojson)
    assert result.exit_code == 0, f"Add failed: {result.output}"

    # Export
    catalog = load_test_catalog(catalog_root)
    table = catalog.load_table("portolake.export_test")

    from portolan_cli.backends.iceberg.export import export_current_snapshot

    output_path = tmp_path / "exported.parquet"
    export_current_snapshot(table, output_path)

    assert output_path.exists()
    exported = pq.read_table(output_path)
    col_names = set(exported.column_names)

    assert not any(n.startswith("geohash_") for n in col_names), (
        f"Derived geohash columns should be excluded: {col_names}"
    )
    assert "bbox_xmin" not in col_names, f"Derived bbox columns should be excluded: {col_names}"


@pytest.mark.integration
def test_export_after_add_has_correct_row_count(initialized_iceberg_catalog, runner, tmp_path):
    """Exported file should have same row count as input."""
    catalog_root = initialized_iceberg_catalog
    geojson = place_geojson_in_collection(catalog_root, "rowcount_export")

    result = invoke_add(runner, catalog_root, geojson)
    assert result.exit_code == 0, f"Add failed: {result.output}"

    catalog = load_test_catalog(catalog_root)
    table = catalog.load_table("portolake.rowcount_export")

    from portolan_cli.backends.iceberg.export import export_current_snapshot

    output_path = tmp_path / "exported.parquet"
    export_current_snapshot(table, output_path)

    exported = pq.read_table(output_path)
    assert exported.num_rows == 3, f"Expected 3 rows (from GeoJSON), got {exported.num_rows}"
