"""Tests for merge strategy functionality in metadata preservation.

Issue #446: portolan add strips human-authored asset and column metadata.

These tests verify that the merge strategy correctly preserves human-enrichable
fields while updating machine-derivable fields.
"""

from __future__ import annotations

import pystac
import pytest

from portolan_cli.errors import AssetKeyCollisionError
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


_PARQUET = "application/vnd.apache.parquet"
_PMTILES = "application/vnd.pmtiles"


class TestAssetKeyCollision:
    """Issue #933: add never lets one file replace a different file's asset."""

    @staticmethod
    def _collection(**assets: pystac.Asset) -> pystac.Collection:
        collection = create_collection(collection_id="zoning", description="Zoning")
        for key, asset in assets.items():
            collection.add_asset(key, asset)
        return collection

    @pytest.mark.unit
    @pytest.mark.parametrize("strategy", list(MergeStrategy))
    def test_different_file_and_media_type_raises(self, strategy: MergeStrategy) -> None:
        collection = self._collection(
            zoning=pystac.Asset(
                href="./legacy.fgb", media_type="application/vnd.flatgeobuf", roles=["data"]
            )
        )
        incoming = pystac.Asset(href="./zoning.parquet", media_type=_PARQUET, roles=["data"])

        with pytest.raises(AssetKeyCollisionError) as excinfo:
            add_asset_to_collection(collection, "zoning", incoming, merge_strategy=strategy)

        assert excinfo.value.code == "PRTLN-COL003"
        assert excinfo.value.asset_key == "zoning"
        assert excinfo.value.existing_href == "./legacy.fgb"
        assert excinfo.value.new_href == "./zoning.parquet"
        assert collection.assets["zoning"].href == "./legacy.fgb"

    @pytest.mark.unit
    @pytest.mark.parametrize(
        ("href", "media_type"),
        [
            ("./roads.shp", "application/x-shapefile"),
            ("./roads.dbf", "application/x-dbf"),
            ("./roads.fgb", "application/vnd.flatgeobuf"),
        ],
    )
    def test_parquet_moves_a_displaced_same_stem_file_to_its_filename_key(
        self, href: str, media_type: str
    ) -> None:
        """A source, sidecar, or other same-stem file under the stem key moves aside.

        0.8.0 renamed ``data`` to the stem after the scan keyed a same-stem
        file by the stem, so any of these files could hold the GeoParquet key.
        """
        collection = self._collection(
            roads=pystac.Asset(href=href, media_type=media_type, roles=["data"])
        )
        parquet = pystac.Asset(href="./roads.parquet", media_type=_PARQUET, roles=["data"])

        add_asset_to_collection(collection, "roads", parquet)

        filename = href.removeprefix("./")
        assert {k: (a.href, a.media_type) for k, a in collection.assets.items()} == {
            "roads": ("./roads.parquet", _PARQUET),
            filename: (href, media_type),
        }
        assert collection.assets[filename].roles == ["data"]

    @pytest.mark.unit
    def test_role_keyed_assets_merge_as_before(self) -> None:
        """A new thumbnail under the ``thumbnail`` key is no data collision."""
        collection = self._collection(
            thumbnail=pystac.Asset(
                href="./roads.thumb.jpg", media_type="image/jpeg", roles=["thumbnail"]
            )
        )
        preview = pystac.Asset(href="./preview.png", media_type="image/png", roles=["thumbnail"])

        add_asset_to_collection(collection, "thumbnail", preview)

        assert {k: a.href for k, a in collection.assets.items()} == {"thumbnail": "./preview.png"}

    @pytest.mark.unit
    def test_same_file_with_another_href_spelling_is_one_asset(self) -> None:
        """``roads.parquet`` and ``./roads.parquet`` name one file."""
        collection = self._collection(
            roads=pystac.Asset(
                href="roads.parquet", media_type="application/geo+parquet", roles=["data"]
            )
        )
        parquet = pystac.Asset(href="./roads.parquet", media_type=_PARQUET, roles=["data"])

        add_asset_to_collection(collection, "roads", parquet)

        assert {k: (a.href, a.media_type) for k, a in collection.assets.items()} == {
            "roads": ("./roads.parquet", _PARQUET)
        }

    @pytest.mark.unit
    def test_flatgeobuf_moves_a_same_stem_pmtiles_to_its_filename(self) -> None:
        """A FlatGeobuf outranks a PMTiles for the stem, as in a fresh add."""
        collection = self._collection(
            roads=pystac.Asset(href="./roads.pmtiles", media_type=_PMTILES, roles=["data"])
        )
        fgb = pystac.Asset(
            href="./roads.fgb", media_type="application/vnd.flatgeobuf", roles=["data"]
        )

        add_asset_to_collection(collection, "roads", fgb)

        assert {k: (a.href, a.roles) for k, a in collection.assets.items()} == {
            "roads": ("./roads.fgb", ["data"]),
            "roads.pmtiles": ("./roads.pmtiles", ["data"]),
        }

    @pytest.mark.unit
    def test_repair_raises_when_the_target_key_holds_another_file(self) -> None:
        collection = self._collection(
            roads=pystac.Asset(
                href="./roads.fgb", media_type="application/vnd.flatgeobuf", roles=["data"]
            ),
            **{
                "roads.fgb": pystac.Asset(
                    href="./other.fgb", media_type="application/vnd.flatgeobuf", roles=["data"]
                )
            },
        )
        parquet = pystac.Asset(href="./roads.parquet", media_type=_PARQUET, roles=["data"])

        with pytest.raises(AssetKeyCollisionError) as excinfo:
            add_asset_to_collection(collection, "roads", parquet)

        assert excinfo.value.asset_key == "roads.fgb"
        assert collection.assets["roads"].href == "./roads.fgb"

    @pytest.mark.unit
    def test_legacy_parquet_media_type_counts_as_the_same(self) -> None:
        collection = self._collection(
            zoning=pystac.Asset(href="./old.parquet", media_type="application/x-parquet")
        )
        incoming = pystac.Asset(href="./zoning.parquet", media_type=_PARQUET, roles=["data"])

        add_asset_to_collection(collection, "zoning", incoming)

        assert collection.assets["zoning"].href == "./zoning.parquet"

    @pytest.mark.unit
    def test_parquet_moves_the_displaced_pmtiles_to_its_tiles_key(self) -> None:
        """The exact damage #933 wrote into catalogs is repaired on re-add."""
        collection = self._collection(
            zoning=pystac.Asset(
                href="./zoning.pmtiles",
                media_type=_PMTILES,
                roles=["data"],
                title="Zoning districts",
                extra_fields={"file:size": 10},
            )
        )
        incoming = pystac.Asset(href="./zoning.parquet", media_type=_PARQUET, roles=["data"])

        add_asset_to_collection(collection, "zoning", incoming)

        assert set(collection.assets) == {"zoning", "zoning-tiles"}
        parquet = collection.assets["zoning"]
        assert (parquet.href, parquet.media_type, parquet.roles) == (
            "./zoning.parquet",
            _PARQUET,
            ["data"],
        )
        # The human title described the data, so it returns to the data asset.
        assert parquet.title == "Zoning districts"
        tiles = collection.assets["zoning-tiles"]
        assert (tiles.href, tiles.media_type, tiles.roles) == (
            "./zoning.pmtiles",
            _PMTILES,
            ["visual"],
        )
        assert tiles.title is None
        assert tiles.extra_fields == {"file:size": 10}

    @pytest.mark.unit
    def test_repair_converges_when_the_tiles_item_lands_first(self) -> None:
        collection = self._collection(
            zoning=pystac.Asset(href="./zoning.pmtiles", media_type=_PMTILES, roles=["data"])
        )
        tiles = pystac.Asset(href="./zoning.pmtiles", media_type=_PMTILES, roles=["visual"])
        parquet = pystac.Asset(href="./zoning.parquet", media_type=_PARQUET, roles=["data"])

        add_asset_to_collection(collection, "zoning-tiles", tiles)
        add_asset_to_collection(collection, "zoning", parquet)

        assert {k: a.href for k, a in collection.assets.items()} == {
            "zoning": "./zoning.parquet",
            "zoning-tiles": "./zoning.pmtiles",
        }
        assert collection.assets["zoning-tiles"].roles == ["visual"]

    @pytest.mark.unit
    def test_repair_drops_the_duplicate_when_the_tiles_key_holds_it(self) -> None:
        collection = self._collection(
            zoning=pystac.Asset(href="./zoning.pmtiles", media_type=_PMTILES, roles=["data"]),
            **{
                "zoning-tiles": pystac.Asset(
                    href="./zoning.pmtiles", media_type=_PMTILES, roles=["visual"]
                )
            },
        )
        parquet = pystac.Asset(href="./zoning.parquet", media_type=_PARQUET, roles=["data"])

        add_asset_to_collection(collection, "zoning", parquet)

        assert {k: a.href for k, a in collection.assets.items()} == {
            "zoning": "./zoning.parquet",
            "zoning-tiles": "./zoning.pmtiles",
        }

    @pytest.mark.unit
    def test_untyped_asset_never_moves_the_pmtiles(self) -> None:
        """Only a GeoParquet repairs the key. An untyped file merges as before."""
        collection = self._collection(
            zoning=pystac.Asset(href="./zoning.pmtiles", media_type=_PMTILES, roles=["data"])
        )
        incoming = pystac.Asset(href="./zoning.bin", roles=["data"])

        add_asset_to_collection(collection, "zoning", incoming)

        assert set(collection.assets) == {"zoning"}
        assert collection.assets["zoning"].href == "./zoning.bin"

    @pytest.mark.unit
    def test_pmtiles_of_another_stem_is_not_moved(self) -> None:
        """Only a same-stem PMTiles is the #933 damage. Anything else raises."""
        collection = self._collection(
            zoning=pystac.Asset(href="./basemap.pmtiles", media_type=_PMTILES, roles=["data"])
        )
        parquet = pystac.Asset(href="./zoning.parquet", media_type=_PARQUET, roles=["data"])

        with pytest.raises(AssetKeyCollisionError):
            add_asset_to_collection(collection, "zoning", parquet)


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
    def test_media_type_parquet(self) -> None:
        """Add module should detect spec-compliant MIME for .parquet."""
        from portolan_cli.add import _MEDIA_TYPE_MAP

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
