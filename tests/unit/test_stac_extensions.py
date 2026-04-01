"""Unit tests for STAC extension foundation (Issue #272).

Tests the STAC extension support including:
- STAC version 1.1.0
- stac_extensions array population
- Table extension for GeoParquet
- Projection extension fields (including raster-specific: proj:shape, proj:transform)
- Bbox WGS84 transformation with antimeridian handling
- Per-band nodata for COGs
"""

from __future__ import annotations

from typing import Any

import pytest

from portolan_cli.metadata.cog import COGMetadata
from portolan_cli.metadata.geoparquet import GeoParquetMetadata


class TestStacVersion:
    """Tests for STAC version update to 1.1.0."""

    @pytest.mark.unit
    def test_stac_version_is_1_1_0(self) -> None:
        """STAC_VERSION constant should be 1.1.0."""
        from portolan_cli.stac import STAC_VERSION

        assert STAC_VERSION == "1.1.0"


class TestStacExtensionsArray:
    """Tests for stac_extensions array population."""

    @pytest.mark.unit
    def test_build_stac_extensions_empty_when_no_extension_fields(self) -> None:
        """build_stac_extensions returns empty list when no extension fields present."""
        from portolan_cli.stac import build_stac_extensions

        properties: dict[str, Any] = {"title": "Test", "description": "No extensions"}
        result = build_stac_extensions(properties)

        assert result == []

    @pytest.mark.unit
    def test_build_stac_extensions_includes_table_extension(self) -> None:
        """build_stac_extensions includes table extension when table: fields present."""
        from portolan_cli.stac import build_stac_extensions

        properties = {
            "table:row_count": 1000,
            "table:columns": [{"name": "id", "type": "int64"}],
        }
        result = build_stac_extensions(properties)

        assert any("table" in ext for ext in result)

    @pytest.mark.unit
    def test_build_stac_extensions_includes_projection_extension(self) -> None:
        """build_stac_extensions includes projection extension when proj: fields present."""
        from portolan_cli.stac import build_stac_extensions

        properties = {
            "proj:code": "EPSG:4326",
            "proj:bbox": [-180, -90, 180, 90],
        }
        result = build_stac_extensions(properties)

        assert any("projection" in ext for ext in result)

    @pytest.mark.unit
    def test_build_stac_extensions_includes_raster_extension(self) -> None:
        """build_stac_extensions includes raster extension when raster: fields present."""
        from portolan_cli.stac import build_stac_extensions

        # STAC v1.1.0: bands is now unified, but raster:spatial_resolution triggers raster ext
        properties = {
            "raster:spatial_resolution": 10.0,
        }
        result = build_stac_extensions(properties)

        assert any("raster" in ext for ext in result)

    @pytest.mark.unit
    def test_build_stac_extensions_multiple_extensions(self) -> None:
        """build_stac_extensions returns multiple extensions when multiple prefixes present."""
        from portolan_cli.stac import build_stac_extensions

        properties = {
            "proj:code": "EPSG:32618",
            "table:row_count": 500,
        }
        result = build_stac_extensions(properties)

        assert len(result) >= 2
        assert any("projection" in ext for ext in result)
        assert any("table" in ext for ext in result)


class TestTableExtension:
    """Tests for Table extension fields on GeoParquet collections."""

    @pytest.mark.unit
    def test_add_table_extension_sets_row_count(self) -> None:
        """add_table_extension sets table:row_count from feature_count."""
        from portolan_cli.stac import add_table_extension, create_collection

        collection = create_collection(
            collection_id="test-table",
            description="Test table extension",
        )
        metadata = GeoParquetMetadata(
            bbox=(-180, -90, 180, 90),
            crs="EPSG:4326",
            geometry_type="Polygon",
            geometry_column="geometry",
            feature_count=1234,
            schema={"id": "int64", "geometry": "binary"},
        )

        add_table_extension(collection, metadata)

        assert collection.extra_fields.get("table:row_count") == 1234

    @pytest.mark.unit
    def test_add_table_extension_sets_primary_geometry(self) -> None:
        """add_table_extension sets table:primary_geometry from geometry_column."""
        from portolan_cli.stac import add_table_extension, create_collection

        collection = create_collection(
            collection_id="test-geom",
            description="Test geometry column",
        )
        metadata = GeoParquetMetadata(
            bbox=(-180, -90, 180, 90),
            crs="EPSG:4326",
            geometry_type="Point",
            geometry_column="geom",
            feature_count=100,
            schema={"id": "int64", "geom": "binary"},
        )

        add_table_extension(collection, metadata)

        assert collection.extra_fields.get("table:primary_geometry") == "geom"

    @pytest.mark.unit
    def test_add_table_extension_sets_columns(self) -> None:
        """add_table_extension sets table:columns from schema."""
        from portolan_cli.stac import add_table_extension, create_collection

        collection = create_collection(
            collection_id="test-columns",
            description="Test columns",
        )
        metadata = GeoParquetMetadata(
            bbox=(-180, -90, 180, 90),
            crs="EPSG:4326",
            geometry_type="Polygon",
            geometry_column="geometry",
            feature_count=100,
            schema={"id": "int64", "name": "string", "geom": "binary"},
        )

        add_table_extension(collection, metadata)

        columns = collection.extra_fields.get("table:columns", [])
        assert len(columns) == 3
        column_names = {col["name"] for col in columns}
        assert column_names == {"id", "name", "geom"}

    @pytest.mark.unit
    def test_add_table_extension_adds_stac_extensions_url(self) -> None:
        """add_table_extension adds table extension URL to stac_extensions."""
        from portolan_cli.stac import EXTENSION_URLS, add_table_extension, create_collection

        collection = create_collection(
            collection_id="test-ext-url",
            description="Test extension URL",
        )
        metadata = GeoParquetMetadata(
            bbox=(-180, -90, 180, 90),
            crs="EPSG:4326",
            geometry_type="Point",
            geometry_column="geometry",
            feature_count=100,
            schema={"id": "int64"},
        )

        add_table_extension(collection, metadata)

        assert EXTENSION_URLS["table"] in (collection.stac_extensions or [])


class TestProjectionExtension:
    """Tests for Projection extension fields."""

    @pytest.mark.unit
    def test_add_projection_extension_sets_proj_code(self) -> None:
        """add_projection_extension sets proj:code from CRS."""
        from portolan_cli.stac import add_projection_extension, create_item

        item = create_item(
            item_id="test-proj",
            bbox=[-122.5, 37.5, -122.0, 38.0],
        )
        metadata = GeoParquetMetadata(
            bbox=(-122.5, 37.5, -122.0, 38.0),
            crs="EPSG:32610",
            geometry_type="Polygon",
            geometry_column="geometry",
            feature_count=100,
            schema={"id": "int64"},
        )

        add_projection_extension(item, metadata)

        assert item.properties.get("proj:code") == "EPSG:32610"

    @pytest.mark.unit
    def test_add_projection_extension_normalizes_epsg_case(self) -> None:
        """add_projection_extension normalizes EPSG codes to uppercase."""
        from portolan_cli.stac import add_projection_extension, create_item

        item = create_item(item_id="test-case", bbox=[0, 0, 1, 1])
        metadata = GeoParquetMetadata(
            bbox=(0, 0, 1, 1),
            crs="epsg:4326",  # lowercase
            geometry_type="Point",
            geometry_column="geometry",
            feature_count=1,
            schema={},
        )

        add_projection_extension(item, metadata)

        assert item.properties.get("proj:code") == "EPSG:4326"

    @pytest.mark.unit
    def test_add_projection_extension_sets_proj_bbox(self) -> None:
        """add_projection_extension sets proj:bbox with native CRS bbox."""
        from portolan_cli.stac import add_projection_extension, create_item

        item = create_item(
            item_id="test-proj-bbox",
            bbox=[-122.5, 37.5, -122.0, 38.0],
        )
        native_bbox = (500000, 4150000, 510000, 4160000)  # UTM coords
        metadata = GeoParquetMetadata(
            bbox=native_bbox,
            crs="EPSG:32610",
            geometry_type="Polygon",
            geometry_column="geometry",
            feature_count=100,
            schema={"id": "int64"},
        )

        add_projection_extension(item, metadata)

        assert item.properties.get("proj:bbox") == list(native_bbox)

    @pytest.mark.unit
    def test_add_projection_extension_skips_when_no_crs(self) -> None:
        """add_projection_extension does nothing when CRS is None."""
        from portolan_cli.stac import add_projection_extension, create_item

        item = create_item(
            item_id="test-no-crs",
            bbox=[0, 0, 1, 1],
        )
        metadata = GeoParquetMetadata(
            bbox=(0, 0, 1, 1),
            crs=None,
            geometry_type="Point",
            geometry_column="geometry",
            feature_count=1,
            schema={},
        )

        add_projection_extension(item, metadata)

        assert "proj:code" not in item.properties

    @pytest.mark.unit
    def test_add_projection_extension_handles_projjson(self) -> None:
        """add_projection_extension handles PROJJSON CRS format."""
        from portolan_cli.stac import add_projection_extension, create_item

        item = create_item(item_id="test-projjson", bbox=[0, 0, 1, 1])
        # PROJJSON format with id containing authority and code
        projjson_crs: dict[str, Any] = {
            "type": "GeographicCRS",
            "name": "WGS 84",
            "id": {"authority": "EPSG", "code": 4326},
        }
        metadata = GeoParquetMetadata(
            bbox=(0, 0, 1, 1),
            crs=projjson_crs,  # type: ignore[arg-type]
            geometry_type="Point",
            geometry_column="geometry",
            feature_count=1,
            schema={},
        )

        add_projection_extension(item, metadata)

        assert item.properties.get("proj:code") == "EPSG:4326"

    @pytest.mark.unit
    def test_add_projection_extension_sets_proj_shape_for_raster(self) -> None:
        """add_projection_extension sets proj:shape for COGMetadata."""
        from portolan_cli.stac import add_projection_extension, create_item

        item = create_item(item_id="test-shape", bbox=[0, 0, 1, 1])
        metadata = COGMetadata(
            bbox=(0, 0, 1, 1),
            crs="EPSG:4326",
            width=256,
            height=512,
            band_count=3,
            dtype="uint8",
            nodata=None,
            resolution=(0.01, 0.01),
            transform=(0.0, 0.01, 0.0, 1.0, 0.0, -0.01),
        )

        add_projection_extension(item, metadata)

        # proj:shape is [height, width] per STAC spec
        assert item.properties.get("proj:shape") == [512, 256]

    @pytest.mark.unit
    def test_add_projection_extension_sets_proj_transform_for_raster(self) -> None:
        """add_projection_extension sets proj:transform for COGMetadata."""
        from portolan_cli.stac import add_projection_extension, create_item

        item = create_item(item_id="test-transform", bbox=[0, 0, 1, 1])
        transform = (0.0, 0.01, 0.0, 1.0, 0.0, -0.01)  # GDAL GeoTransform
        metadata = COGMetadata(
            bbox=(0, 0, 1, 1),
            crs="EPSG:4326",
            width=100,
            height=100,
            band_count=1,
            dtype="float32",
            nodata=-9999.0,
            resolution=(0.01, 0.01),
            transform=transform,
        )

        add_projection_extension(item, metadata)

        assert item.properties.get("proj:transform") == list(transform)

    @pytest.mark.unit
    def test_add_projection_extension_adds_stac_extensions_url(self) -> None:
        """add_projection_extension adds projection extension URL to stac_extensions."""
        from portolan_cli.stac import EXTENSION_URLS, add_projection_extension, create_item

        item = create_item(item_id="test-ext-url", bbox=[0, 0, 1, 1])
        metadata = GeoParquetMetadata(
            bbox=(0, 0, 1, 1),
            crs="EPSG:4326",
            geometry_type="Point",
            geometry_column="geometry",
            feature_count=1,
            schema={},
        )

        add_projection_extension(item, metadata)

        assert EXTENSION_URLS["projection"] in (item.stac_extensions or [])


class TestBboxWgs84Transformation:
    """Tests for bbox WGS84 transformation with antimeridian handling."""

    @pytest.mark.unit
    def test_transform_bbox_to_wgs84_passes_through_4326(self) -> None:
        """transform_bbox_to_wgs84 returns unchanged bbox for WGS84 input."""
        from portolan_cli.crs import transform_bbox_to_wgs84

        bbox = (-122.5, 37.5, -122.0, 38.0)
        result = transform_bbox_to_wgs84(bbox, "EPSG:4326")

        assert result == bbox

    @pytest.mark.unit
    def test_transform_bbox_to_wgs84_transforms_utm(self) -> None:
        """transform_bbox_to_wgs84 transforms UTM bbox to WGS84."""
        from portolan_cli.crs import transform_bbox_to_wgs84

        # UTM Zone 10N bbox (San Francisco area)
        utm_bbox = (545000, 4175000, 555000, 4185000)
        result = transform_bbox_to_wgs84(utm_bbox, "EPSG:32610")

        # Result should be in WGS84 range
        min_x, min_y, max_x, max_y = result
        assert -180 <= min_x <= 180
        assert -90 <= min_y <= 90
        assert -180 <= max_x <= 180
        assert -90 <= max_y <= 90
        # Should be roughly San Francisco area
        assert -123 < min_x < -121
        assert 37 < min_y < 38

    @pytest.mark.unit
    def test_transform_bbox_to_wgs84_handles_none_crs(self) -> None:
        """transform_bbox_to_wgs84 returns unchanged bbox when CRS is None."""
        from portolan_cli.crs import transform_bbox_to_wgs84

        bbox = (100, 200, 300, 400)
        result = transform_bbox_to_wgs84(bbox, None)

        assert result == bbox

    @pytest.mark.unit
    def test_transform_bbox_to_wgs84_handles_wkt_crs(self) -> None:
        """transform_bbox_to_wgs84 handles WKT CRS string."""
        from portolan_cli.crs import transform_bbox_to_wgs84

        # WKT for WGS84
        wkt = 'GEOGCS["WGS 84",DATUM["WGS_1984",SPHEROID["WGS 84",6378137,298.257223563]],PRIMEM["Greenwich",0],UNIT["degree",0.0174532925199433]]'
        bbox = (-122.5, 37.5, -122.0, 38.0)
        result = transform_bbox_to_wgs84(bbox, wkt)

        # Should pass through since it's WGS84
        assert abs(result[0] - bbox[0]) < 0.001
        assert abs(result[1] - bbox[1]) < 0.001

    @pytest.mark.unit
    def test_transform_bbox_to_wgs84_handles_invalid_crs(self) -> None:
        """transform_bbox_to_wgs84 logs warning and returns unchanged bbox for invalid CRS."""
        from portolan_cli.crs import transform_bbox_to_wgs84

        bbox = (100, 200, 300, 400)
        result = transform_bbox_to_wgs84(bbox, "NOT_A_VALID_CRS")

        # Should return unchanged bbox (with warning logged)
        assert result == bbox

    @pytest.mark.unit
    def test_transform_bbox_to_wgs84_antimeridian_crossing(self) -> None:
        """transform_bbox_to_wgs84 handles antimeridian crossing (west > east)."""
        from portolan_cli.crs import transform_bbox_to_wgs84

        # Bbox that crosses the antimeridian (Fiji area)
        # In WGS84, this should result in west > east per RFC 7946
        fiji_bbox = (177.0, -19.0, -179.0, -16.0)
        result = transform_bbox_to_wgs84(fiji_bbox, "EPSG:4326")

        # For antimeridian-crossing bbox, west (minx) > east (maxx)
        west, south, east, north = result
        # The antimeridian library should preserve or fix this
        assert south < north  # Latitude should be normal
        # Either west > east (crossing) or normal bbox
        assert -180 <= west <= 180
        assert -180 <= east <= 180


class TestPerBandNodata:
    """Tests for per-band nodata in COG metadata."""

    @pytest.mark.unit
    def test_cog_metadata_has_per_band_nodata(self) -> None:
        """COGMetadata.to_stac_properties returns per-band nodata values."""
        metadata = COGMetadata(
            bbox=(0, 0, 1, 1),
            crs="EPSG:4326",
            width=100,
            height=100,
            band_count=3,
            dtype="uint8",
            nodata=None,
            resolution=(1.0, 1.0),
            nodatavals=(0, 255, 128),
        )

        props = metadata.to_stac_properties()
        bands = props["bands"]

        assert len(bands) == 3
        assert bands[0]["nodata"] == 0
        assert bands[1]["nodata"] == 255
        assert bands[2]["nodata"] == 128

    @pytest.mark.unit
    def test_cog_metadata_handles_uniform_nodata(self) -> None:
        """COGMetadata handles uniform nodata across bands."""
        metadata = COGMetadata(
            bbox=(0, 0, 1, 1),
            crs="EPSG:4326",
            width=100,
            height=100,
            band_count=3,
            dtype="float32",
            nodata=-9999.0,
            resolution=(1.0, 1.0),
            nodatavals=(-9999.0, -9999.0, -9999.0),
        )

        props = metadata.to_stac_properties()
        bands = props["bands"]

        assert all(b["nodata"] == -9999.0 for b in bands)

    @pytest.mark.unit
    def test_cog_metadata_falls_back_to_uniform_nodata(self) -> None:
        """COGMetadata falls back to uniform nodata when nodatavals is None."""
        metadata = COGMetadata(
            bbox=(0, 0, 1, 1),
            crs="EPSG:4326",
            width=100,
            height=100,
            band_count=2,
            dtype="int16",
            nodata=-32768,
            resolution=(1.0, 1.0),
            nodatavals=None,  # Fall back to uniform nodata
        )

        props = metadata.to_stac_properties()
        bands = props["bands"]

        assert len(bands) == 2
        assert all(b["nodata"] == -32768 for b in bands)

    @pytest.mark.unit
    def test_cog_metadata_to_dict_includes_transform(self) -> None:
        """COGMetadata.to_dict includes transform when present."""
        transform = (0.0, 0.01, 0.0, 1.0, 0.0, -0.01)
        metadata = COGMetadata(
            bbox=(0, 0, 1, 1),
            crs="EPSG:4326",
            width=100,
            height=100,
            band_count=1,
            dtype="float32",
            nodata=None,
            resolution=(0.01, 0.01),
            transform=transform,
        )

        d = metadata.to_dict()

        assert d["transform"] == list(transform)

    @pytest.mark.unit
    def test_cog_metadata_to_dict_omits_transform_when_none(self) -> None:
        """COGMetadata.to_dict omits transform when None."""
        metadata = COGMetadata(
            bbox=(0, 0, 1, 1),
            crs="EPSG:4326",
            width=100,
            height=100,
            band_count=1,
            dtype="float32",
            nodata=None,
            resolution=(0.01, 0.01),
            transform=None,
        )

        d = metadata.to_dict()

        assert "transform" not in d


class TestStacVersionInModels:
    """Tests for STAC version consistency in model classes (Issue #305)."""

    @pytest.mark.unit
    def test_item_model_uses_stac_version_constant(self) -> None:
        """ItemModel.stac_version should use STAC_VERSION constant."""
        from portolan_cli.models.item import ItemModel
        from portolan_cli.stac import STAC_VERSION

        item = ItemModel(
            id="test-item",
            geometry={"type": "Point", "coordinates": [0, 0]},
            bbox=[0, 0, 1, 1],
            properties={"datetime": None},
            assets={},
            collection="test",
        )

        assert item.stac_version == STAC_VERSION

    @pytest.mark.unit
    def test_collection_model_uses_stac_version_constant(self) -> None:
        """CollectionModel.stac_version should use STAC_VERSION constant."""
        from portolan_cli.models.collection import CollectionModel, ExtentModel
        from portolan_cli.stac import STAC_VERSION

        collection = CollectionModel(
            id="test-collection",
            description="Test",
            extent=ExtentModel(
                spatial={"bbox": [[-180, -90, 180, 90]]},
                temporal={"interval": [[None, None]]},
            ),
        )

        assert collection.stac_version == STAC_VERSION

    @pytest.mark.unit
    def test_catalog_model_uses_stac_version_constant(self) -> None:
        """CatalogModel.stac_version should use STAC_VERSION constant."""
        from portolan_cli.models.catalog import CatalogModel
        from portolan_cli.stac import STAC_VERSION

        catalog = CatalogModel(
            id="test-catalog",
            description="Test catalog",
        )

        assert catalog.stac_version == STAC_VERSION


class TestTableExtensionAggregation:
    """Tests for Table extension aggregation in finalize_datasets (Issue #304)."""

    @pytest.mark.unit
    def test_aggregate_table_metadata_sums_row_counts(self) -> None:
        """aggregate_table_metadata should sum row_count across all vector items."""
        from portolan_cli.stac import aggregate_table_metadata

        metadata_list = [
            GeoParquetMetadata(
                bbox=(0, 0, 1, 1),
                crs="EPSG:4326",
                geometry_type="Polygon",
                geometry_column="geometry",
                feature_count=100,
                schema={"id": "int64", "geometry": "binary"},
            ),
            GeoParquetMetadata(
                bbox=(1, 1, 2, 2),
                crs="EPSG:4326",
                geometry_type="Polygon",
                geometry_column="geometry",
                feature_count=200,
                schema={"id": "int64", "geometry": "binary"},
            ),
            GeoParquetMetadata(
                bbox=(2, 2, 3, 3),
                crs="EPSG:4326",
                geometry_type="Polygon",
                geometry_column="geometry",
                feature_count=300,
                schema={"id": "int64", "geometry": "binary"},
            ),
        ]

        aggregated = aggregate_table_metadata(metadata_list)

        assert aggregated.feature_count == 600  # 100 + 200 + 300
        # Bbox should be union of all items
        assert aggregated.bbox == (0, 0, 3, 3)

    @pytest.mark.unit
    def test_aggregate_table_metadata_merges_schemas(self) -> None:
        """aggregate_table_metadata should merge schemas from all items."""
        from portolan_cli.stac import aggregate_table_metadata

        metadata_list = [
            GeoParquetMetadata(
                bbox=(0, 0, 1, 1),
                crs="EPSG:4326",
                geometry_type="Point",
                geometry_column="geometry",
                feature_count=10,
                schema={"id": "int64", "name": "string", "geometry": "binary"},
            ),
            GeoParquetMetadata(
                bbox=(1, 1, 2, 2),
                crs="EPSG:4326",
                geometry_type="Point",
                geometry_column="geometry",
                feature_count=20,
                schema={"id": "int64", "value": "float64", "geometry": "binary"},
            ),
        ]

        aggregated = aggregate_table_metadata(metadata_list)

        # Should have union of all column names
        assert "id" in aggregated.schema
        assert "name" in aggregated.schema
        assert "value" in aggregated.schema
        assert "geometry" in aggregated.schema

    @pytest.mark.unit
    def test_aggregate_table_metadata_uses_first_geometry_column(self) -> None:
        """aggregate_table_metadata should use first item's geometry column."""
        from portolan_cli.stac import aggregate_table_metadata

        metadata_list = [
            GeoParquetMetadata(
                bbox=(0, 0, 1, 1),
                crs="EPSG:4326",
                geometry_type="Point",
                geometry_column="geom",
                feature_count=10,
                schema={"geom": "binary"},
            ),
            GeoParquetMetadata(
                bbox=(1, 1, 2, 2),
                crs="EPSG:4326",
                geometry_type="Point",
                geometry_column="geometry",
                feature_count=20,
                schema={"geometry": "binary"},
            ),
        ]

        aggregated = aggregate_table_metadata(metadata_list)

        assert aggregated.geometry_column == "geom"

    @pytest.mark.unit
    def test_aggregate_table_metadata_single_item(self) -> None:
        """aggregate_table_metadata should handle single item."""
        from portolan_cli.stac import aggregate_table_metadata

        metadata_list = [
            GeoParquetMetadata(
                bbox=(0, 0, 1, 1),
                crs="EPSG:4326",
                geometry_type="Polygon",
                geometry_column="geometry",
                feature_count=500,
                schema={"id": "int64"},
            ),
        ]

        aggregated = aggregate_table_metadata(metadata_list)

        assert aggregated.feature_count == 500
        assert aggregated.geometry_column == "geometry"

    @pytest.mark.unit
    def test_aggregate_table_metadata_empty_list_raises(self) -> None:
        """aggregate_table_metadata should raise on empty list."""
        from portolan_cli.stac import aggregate_table_metadata

        with pytest.raises(ValueError, match="empty"):
            aggregate_table_metadata([])

    @pytest.mark.unit
    def test_aggregate_table_metadata_computes_bbox_union(self) -> None:
        """aggregate_table_metadata should compute union of all bboxes."""
        from portolan_cli.stac import aggregate_table_metadata

        # Three non-overlapping bboxes representing NYC, LA, and Chicago
        metadata_list = [
            GeoParquetMetadata(
                bbox=(-74.3, 40.5, -73.7, 40.9),  # NYC area
                crs="EPSG:4326",
                geometry_type="Point",
                geometry_column="geometry",
                feature_count=100,
                schema={"id": "int64"},
            ),
            GeoParquetMetadata(
                bbox=(-118.7, 33.7, -118.1, 34.3),  # LA area
                crs="EPSG:4326",
                geometry_type="Point",
                geometry_column="geometry",
                feature_count=200,
                schema={"id": "int64"},
            ),
            GeoParquetMetadata(
                bbox=(-87.9, 41.6, -87.5, 42.0),  # Chicago area
                crs="EPSG:4326",
                geometry_type="Point",
                geometry_column="geometry",
                feature_count=150,
                schema={"id": "int64"},
            ),
        ]

        aggregated = aggregate_table_metadata(metadata_list)

        # Union should encompass all three cities
        assert aggregated.bbox is not None
        minx, miny, maxx, maxy = aggregated.bbox
        assert minx == pytest.approx(-118.7)  # LA west
        assert miny == pytest.approx(33.7)  # LA south
        assert maxx == pytest.approx(-73.7)  # NYC east
        assert maxy == pytest.approx(42.0)  # Chicago north

    @pytest.mark.unit
    def test_aggregate_table_metadata_warns_on_crs_mismatch(self) -> None:
        """aggregate_table_metadata should warn when CRS values differ."""
        import warnings

        from portolan_cli.stac import aggregate_table_metadata

        metadata_list = [
            GeoParquetMetadata(
                bbox=(0, 0, 1, 1),
                crs="EPSG:4326",
                geometry_type="Point",
                geometry_column="geometry",
                feature_count=100,
                schema={"id": "int64"},
            ),
            GeoParquetMetadata(
                bbox=(1, 1, 2, 2),
                crs="EPSG:32618",  # Different CRS (UTM zone 18N)
                geometry_type="Point",
                geometry_column="geometry",
                feature_count=200,
                schema={"id": "int64"},
            ),
        ]

        with warnings.catch_warnings(record=True) as w:
            warnings.simplefilter("always")
            aggregated = aggregate_table_metadata(metadata_list)

            # Should have raised a warning
            assert len(w) == 1
            assert "CRS mismatch" in str(w[0].message)
            assert "EPSG:4326" in str(w[0].message)
            assert "EPSG:32618" in str(w[0].message)

        # Should use first item's CRS
        assert aggregated.crs == "EPSG:4326"

    @pytest.mark.unit
    def test_aggregate_table_metadata_warns_on_schema_type_conflict(self) -> None:
        """aggregate_table_metadata should warn when same column has different types."""
        import warnings

        from portolan_cli.stac import aggregate_table_metadata

        metadata_list = [
            GeoParquetMetadata(
                bbox=(0, 0, 1, 1),
                crs="EPSG:4326",
                geometry_type="Point",
                geometry_column="geometry",
                feature_count=100,
                schema={"id": "int64", "name": "string"},
            ),
            GeoParquetMetadata(
                bbox=(1, 1, 2, 2),
                crs="EPSG:4326",
                geometry_type="Point",
                geometry_column="geometry",
                feature_count=200,
                schema={"id": "string", "value": "float64"},  # id is string here!
            ),
        ]

        with warnings.catch_warnings(record=True) as w:
            warnings.simplefilter("always")
            aggregated = aggregate_table_metadata(metadata_list)

            # Should have raised a warning about type conflict
            assert len(w) == 1
            assert "Schema type conflicts" in str(w[0].message)
            assert "id" in str(w[0].message)
            assert "int64" in str(w[0].message)
            assert "string" in str(w[0].message)

        # First occurrence wins
        assert aggregated.schema["id"] == "int64"

    @pytest.mark.unit
    def test_aggregate_table_metadata_warns_on_geometry_type_mismatch(self) -> None:
        """aggregate_table_metadata should warn when geometry types differ."""
        import warnings

        from portolan_cli.stac import aggregate_table_metadata

        metadata_list = [
            GeoParquetMetadata(
                bbox=(0, 0, 1, 1),
                crs="EPSG:4326",
                geometry_type="Point",
                geometry_column="geometry",
                feature_count=100,
                schema={"id": "int64"},
            ),
            GeoParquetMetadata(
                bbox=(1, 1, 2, 2),
                crs="EPSG:4326",
                geometry_type="Polygon",  # Different geometry type
                geometry_column="geometry",
                feature_count=200,
                schema={"id": "int64"},
            ),
        ]

        with warnings.catch_warnings(record=True) as w:
            warnings.simplefilter("always")
            aggregated = aggregate_table_metadata(metadata_list)

            # Should have raised a warning about mixed geometry types
            assert len(w) == 1
            assert "Mixed geometry types" in str(w[0].message)
            assert "Point" in str(w[0].message)
            assert "Polygon" in str(w[0].message)

        # First item's type is used
        assert aggregated.geometry_type == "Point"

    @pytest.mark.unit
    def test_aggregate_table_metadata_raises_on_no_valid_bboxes(self) -> None:
        """aggregate_table_metadata should raise when no items have valid bboxes."""
        from portolan_cli.stac import aggregate_table_metadata

        metadata_list = [
            GeoParquetMetadata(
                bbox=None,  # No bbox
                crs="EPSG:4326",
                geometry_type="Point",
                geometry_column="geometry",
                feature_count=100,
                schema={"id": "int64"},
            ),
            GeoParquetMetadata(
                bbox=None,  # No bbox either
                crs="EPSG:4326",
                geometry_type="Point",
                geometry_column="geometry",
                feature_count=200,
                schema={"id": "int64"},
            ),
        ]

        with pytest.raises(ValueError, match="no items have valid bboxes"):
            aggregate_table_metadata(metadata_list)

    @pytest.mark.unit
    def test_aggregate_table_metadata_handles_partial_bboxes(self) -> None:
        """aggregate_table_metadata should ignore items without bboxes when computing union."""
        from portolan_cli.stac import aggregate_table_metadata

        metadata_list = [
            GeoParquetMetadata(
                bbox=(0, 0, 1, 1),
                crs="EPSG:4326",
                geometry_type="Point",
                geometry_column="geometry",
                feature_count=100,
                schema={"id": "int64"},
            ),
            GeoParquetMetadata(
                bbox=None,  # No bbox - should be ignored for union
                crs="EPSG:4326",
                geometry_type="Point",
                geometry_column="geometry",
                feature_count=200,
                schema={"id": "int64"},
            ),
            GeoParquetMetadata(
                bbox=(5, 5, 10, 10),
                crs="EPSG:4326",
                geometry_type="Point",
                geometry_column="geometry",
                feature_count=150,
                schema={"id": "int64"},
            ),
        ]

        aggregated = aggregate_table_metadata(metadata_list)

        # Union should be computed from items 0 and 2 only
        assert aggregated.bbox == (0, 0, 10, 10)
        # But row count should include all items
        assert aggregated.feature_count == 450
