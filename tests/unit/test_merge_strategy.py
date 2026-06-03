"""Tests for merge strategy functionality in metadata preservation.

Issue #446: portolan add strips human-authored asset and column metadata.

These tests verify that the merge strategy correctly preserves human-enrichable
fields while updating machine-derivable fields.
"""

from __future__ import annotations

import pystac
import pytest

from portolan_cli.metadata.geoparquet import GeoParquetMetadata
from portolan_cli.stac import (
    MergeStrategy,
    add_asset_to_collection,
    add_table_extension,
    create_collection,
)


class TestMergeStrategyEnum:
    """Tests for MergeStrategy enum definition."""

    @pytest.mark.unit
    def test_merge_strategy_has_smart_default(self) -> None:
        """MergeStrategy.SMART should be the default strategy."""
        assert MergeStrategy.SMART is not None

    @pytest.mark.unit
    def test_merge_strategy_has_all_options(self) -> None:
        """MergeStrategy should have smart, keep, overwrite, interactive options."""
        assert hasattr(MergeStrategy, "SMART")
        assert hasattr(MergeStrategy, "KEEP")
        assert hasattr(MergeStrategy, "OVERWRITE")
        assert hasattr(MergeStrategy, "INTERACTIVE")


class TestAddAssetToCollectionMerge:
    """Tests for asset merge behavior in add_asset_to_collection."""

    def _create_collection_with_asset(self) -> pystac.Collection:
        """Create a collection with a pre-existing asset with human-authored fields."""
        collection = create_collection(
            collection_id="test-merge",
            description="Test merge behavior",
        )
        existing_asset = pystac.Asset(
            href="data.parquet",
            media_type="application/vnd.apache.parquet",
            roles=["data"],
            title="Census Demographics 2020",
            description="Detailed census data with population demographics by tract.",
        )
        collection.add_asset("data", existing_asset)
        return collection

    @pytest.mark.unit
    def test_smart_strategy_preserves_title(self) -> None:
        """SMART strategy preserves existing asset title."""
        collection = self._create_collection_with_asset()

        new_asset = pystac.Asset(
            href="data.parquet",
            media_type="application/vnd.apache.parquet",
            roles=["data"],
            title=None,  # Auto-detected has no title
        )

        add_asset_to_collection(collection, "data", new_asset, merge_strategy=MergeStrategy.SMART)

        assert collection.assets["data"].title == "Census Demographics 2020"

    @pytest.mark.unit
    def test_smart_strategy_preserves_description(self) -> None:
        """SMART strategy preserves existing asset description."""
        collection = self._create_collection_with_asset()

        new_asset = pystac.Asset(
            href="data.parquet",
            media_type="application/vnd.apache.parquet",
            roles=["data"],
            description=None,
        )

        add_asset_to_collection(collection, "data", new_asset, merge_strategy=MergeStrategy.SMART)

        assert "census data with population" in collection.assets["data"].description

    @pytest.mark.unit
    def test_smart_strategy_updates_href(self) -> None:
        """SMART strategy updates href (machine-derivable field)."""
        collection = self._create_collection_with_asset()

        new_asset = pystac.Asset(
            href="updated/path/data.parquet",
            media_type="application/vnd.apache.parquet",
            roles=["data"],
        )

        add_asset_to_collection(collection, "data", new_asset, merge_strategy=MergeStrategy.SMART)

        assert collection.assets["data"].href == "updated/path/data.parquet"

    @pytest.mark.unit
    def test_smart_strategy_updates_media_type(self) -> None:
        """SMART strategy updates media_type (machine-derivable field)."""
        collection = self._create_collection_with_asset()

        new_asset = pystac.Asset(
            href="data.parquet",
            media_type="application/vnd.apache.parquet",  # Correct MIME
            roles=["data"],
        )

        add_asset_to_collection(collection, "data", new_asset, merge_strategy=MergeStrategy.SMART)

        assert collection.assets["data"].media_type == "application/vnd.apache.parquet"

    @pytest.mark.unit
    def test_keep_strategy_preserves_all_existing(self) -> None:
        """KEEP strategy preserves all existing fields, only adds missing."""
        collection = self._create_collection_with_asset()
        original_href = collection.assets["data"].href

        new_asset = pystac.Asset(
            href="different/path.parquet",
            media_type="application/vnd.apache.parquet",
            roles=["data", "overview"],
            title="New Title",
            description="New description",
        )

        add_asset_to_collection(collection, "data", new_asset, merge_strategy=MergeStrategy.KEEP)

        asset = collection.assets["data"]
        assert asset.href == original_href
        assert asset.title == "Census Demographics 2020"
        assert "census data with population" in asset.description

    @pytest.mark.unit
    def test_overwrite_strategy_replaces_everything(self) -> None:
        """OVERWRITE strategy replaces all fields with new values."""
        collection = self._create_collection_with_asset()

        new_asset = pystac.Asset(
            href="new/data.parquet",
            media_type="application/vnd.apache.parquet",
            roles=["data"],
            title=None,
            description=None,
        )

        add_asset_to_collection(
            collection, "data", new_asset, merge_strategy=MergeStrategy.OVERWRITE
        )

        asset = collection.assets["data"]
        assert asset.href == "new/data.parquet"
        assert asset.title is None
        assert asset.description is None

    @pytest.mark.unit
    def test_new_asset_added_without_merge(self) -> None:
        """Adding a new asset key doesn't require merge logic."""
        collection = create_collection(
            collection_id="test-new",
            description="Test new asset",
        )

        new_asset = pystac.Asset(
            href="data.parquet",
            media_type="application/vnd.apache.parquet",
            roles=["data"],
            title="New Asset",
        )

        add_asset_to_collection(collection, "data", new_asset, merge_strategy=MergeStrategy.SMART)

        assert collection.assets["data"].title == "New Asset"

    @pytest.mark.unit
    def test_smart_strategy_preserves_extra_fields(self) -> None:
        """SMART strategy preserves human-authored extra_fields."""
        collection = create_collection(
            collection_id="test-extra",
            description="Test extra fields",
        )
        existing_asset = pystac.Asset(
            href="data.parquet",
            media_type="application/vnd.apache.parquet",
            roles=["data"],
            extra_fields={"custom:note": "Hand-authored annotation"},
        )
        collection.add_asset("data", existing_asset)

        new_asset = pystac.Asset(
            href="data.parquet",
            media_type="application/vnd.apache.parquet",
            roles=["data"],
            extra_fields={"file:size": 12345},
        )

        add_asset_to_collection(collection, "data", new_asset, merge_strategy=MergeStrategy.SMART)

        asset = collection.assets["data"]
        # Machine-derivable extra_fields updated
        assert asset.extra_fields.get("file:size") == 12345
        # Custom fields preserved (not in machine-derivable list)
        assert asset.extra_fields.get("custom:note") == "Hand-authored annotation"


class TestAddTableExtensionMerge:
    """Tests for table:columns merge behavior in add_table_extension."""

    def _create_collection_with_columns(self) -> pystac.Collection:
        """Create a collection with pre-existing table:columns with descriptions."""
        collection = create_collection(
            collection_id="test-table-merge",
            description="Test table merge",
        )
        collection.extra_fields["table:columns"] = [
            {
                "name": "id",
                "type": "int64",
                "description": "Unique identifier for each census tract.",
            },
            {
                "name": "population",
                "type": "int64",
                "description": "Total population count from 2020 census.",
            },
            {
                "name": "geometry",
                "type": "binary",
                "description": "Tract boundary polygon in WGS84.",
            },
        ]
        collection.extra_fields["table:row_count"] = 1000
        return collection

    @pytest.mark.unit
    def test_smart_strategy_preserves_column_descriptions(self) -> None:
        """SMART strategy preserves existing column descriptions."""
        collection = self._create_collection_with_columns()

        metadata = GeoParquetMetadata(
            bbox=(-180, -90, 180, 90),
            crs="EPSG:4326",
            geometry_type="Polygon",
            geometry_column="geometry",
            feature_count=1500,  # Updated row count
            schema={"id": "int64", "population": "int64", "geometry": "binary"},
        )

        add_table_extension(collection, metadata, merge_strategy=MergeStrategy.SMART)

        columns = collection.extra_fields["table:columns"]
        id_col = next(c for c in columns if c["name"] == "id")
        pop_col = next(c for c in columns if c["name"] == "population")

        assert id_col["description"] == "Unique identifier for each census tract."
        assert pop_col["description"] == "Total population count from 2020 census."

    @pytest.mark.unit
    def test_smart_strategy_updates_row_count(self) -> None:
        """SMART strategy updates table:row_count (machine-derivable)."""
        collection = self._create_collection_with_columns()

        metadata = GeoParquetMetadata(
            bbox=(-180, -90, 180, 90),
            crs="EPSG:4326",
            geometry_type="Polygon",
            geometry_column="geometry",
            feature_count=2000,
            schema={"id": "int64", "population": "int64", "geometry": "binary"},
        )

        add_table_extension(collection, metadata, merge_strategy=MergeStrategy.SMART)

        assert collection.extra_fields["table:row_count"] == 2000

    @pytest.mark.unit
    def test_smart_strategy_updates_column_types(self) -> None:
        """SMART strategy updates column types (machine-derivable)."""
        collection = self._create_collection_with_columns()

        metadata = GeoParquetMetadata(
            bbox=(-180, -90, 180, 90),
            crs="EPSG:4326",
            geometry_type="Polygon",
            geometry_column="geometry",
            feature_count=1000,
            schema={
                "id": "int32",  # Type changed
                "population": "float64",  # Type changed
                "geometry": "binary",
            },
        )

        add_table_extension(collection, metadata, merge_strategy=MergeStrategy.SMART)

        columns = collection.extra_fields["table:columns"]
        id_col = next(c for c in columns if c["name"] == "id")
        pop_col = next(c for c in columns if c["name"] == "population")

        assert id_col["type"] == "int32"
        assert pop_col["type"] == "float64"
        # Descriptions still preserved
        assert id_col["description"] == "Unique identifier for each census tract."

    @pytest.mark.unit
    def test_smart_strategy_handles_new_columns(self) -> None:
        """SMART strategy adds new columns from schema."""
        collection = self._create_collection_with_columns()

        metadata = GeoParquetMetadata(
            bbox=(-180, -90, 180, 90),
            crs="EPSG:4326",
            geometry_type="Polygon",
            geometry_column="geometry",
            feature_count=1000,
            schema={
                "id": "int64",
                "population": "int64",
                "geometry": "binary",
                "median_income": "float64",  # New column
            },
        )

        add_table_extension(collection, metadata, merge_strategy=MergeStrategy.SMART)

        columns = collection.extra_fields["table:columns"]
        column_names = {c["name"] for c in columns}

        assert "median_income" in column_names
        income_col = next(c for c in columns if c["name"] == "median_income")
        assert income_col["type"] == "float64"

    @pytest.mark.unit
    def test_smart_strategy_removes_deleted_columns(self) -> None:
        """SMART strategy removes columns no longer in schema."""
        collection = self._create_collection_with_columns()

        metadata = GeoParquetMetadata(
            bbox=(-180, -90, 180, 90),
            crs="EPSG:4326",
            geometry_type="Polygon",
            geometry_column="geometry",
            feature_count=1000,
            schema={
                "id": "int64",
                "geometry": "binary",
                # population column removed
            },
        )

        add_table_extension(collection, metadata, merge_strategy=MergeStrategy.SMART)

        columns = collection.extra_fields["table:columns"]
        column_names = {c["name"] for c in columns}

        assert "population" not in column_names
        assert len(columns) == 2

    @pytest.mark.unit
    def test_keep_strategy_preserves_all_columns(self) -> None:
        """KEEP strategy preserves all existing column metadata."""
        collection = self._create_collection_with_columns()
        original_row_count = collection.extra_fields["table:row_count"]

        metadata = GeoParquetMetadata(
            bbox=(-180, -90, 180, 90),
            crs="EPSG:4326",
            geometry_type="Polygon",
            geometry_column="geometry",
            feature_count=9999,
            schema={"id": "int32", "population": "float64", "geometry": "binary"},
        )

        add_table_extension(collection, metadata, merge_strategy=MergeStrategy.KEEP)

        # Row count preserved
        assert collection.extra_fields["table:row_count"] == original_row_count

        # Column types preserved
        columns = collection.extra_fields["table:columns"]
        id_col = next(c for c in columns if c["name"] == "id")
        assert id_col["type"] == "int64"  # Original type, not int32

    @pytest.mark.unit
    def test_overwrite_strategy_replaces_all_columns(self) -> None:
        """OVERWRITE strategy replaces all column metadata."""
        collection = self._create_collection_with_columns()

        metadata = GeoParquetMetadata(
            bbox=(-180, -90, 180, 90),
            crs="EPSG:4326",
            geometry_type="Polygon",
            geometry_column="geometry",
            feature_count=500,
            schema={"id": "int64", "geometry": "binary"},
        )

        add_table_extension(collection, metadata, merge_strategy=MergeStrategy.OVERWRITE)

        columns = collection.extra_fields["table:columns"]

        # Only 2 columns now
        assert len(columns) == 2

        # No descriptions (auto-detected only has name/type)
        id_col = next(c for c in columns if c["name"] == "id")
        assert "description" not in id_col


class TestMimeTypeStandardization:
    """Tests for MIME type standardization (application/vnd.apache.parquet)."""

    @pytest.mark.unit
    def test_parquet_mime_type_constant(self) -> None:
        """PARQUET_MEDIA_TYPE should be the spec-compliant value."""
        from portolan_cli.stac_parquet import PARQUET_MEDIA_TYPE

        assert PARQUET_MEDIA_TYPE == "application/vnd.apache.parquet"

    @pytest.mark.unit
    def test_dataset_media_type_parquet(self) -> None:
        """Dataset module should detect spec-compliant MIME for .parquet."""
        from portolan_cli.dataset import _MEDIA_TYPE_MAP

        assert _MEDIA_TYPE_MAP[".parquet"] == "application/vnd.apache.parquet"

    @pytest.mark.unit
    def test_item_media_type_parquet(self) -> None:
        """Item module should detect spec-compliant MIME for .parquet."""
        from pathlib import Path

        from portolan_cli.item import _get_media_type

        assert _get_media_type(Path("data.parquet")) == "application/vnd.apache.parquet"

    @pytest.mark.unit
    def test_item_media_type_geoparquet(self) -> None:
        """Item module should detect spec-compliant MIME for .geoparquet."""
        from pathlib import Path

        from portolan_cli.item import _get_media_type

        assert _get_media_type(Path("data.geoparquet")) == "application/vnd.apache.parquet"
