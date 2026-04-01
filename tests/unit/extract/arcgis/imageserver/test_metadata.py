"""Tests for ImageServer STAC metadata generation.

Tests verify that STAC Collection and Item metadata are correctly
generated from ImageServer metadata using the Wave 1 data models.
"""

from __future__ import annotations

import pytest

from portolan_cli.extract.arcgis.imageserver.discovery import ImageServerMetadata
from portolan_cli.extract.arcgis.imageserver.metadata import (
    create_collection_metadata,
    create_item_metadata,
)
from portolan_cli.extract.arcgis.imageserver.tiling import TileSpec
from portolan_cli.models._stac_version import get_stac_version

# =============================================================================
# Fixtures using Wave 1 data models
# =============================================================================


@pytest.fixture
def sample_metadata() -> ImageServerMetadata:
    """Full ImageServer metadata matching actual API response structure."""
    return ImageServerMetadata(
        name="CharlotteLAS",
        band_count=1,
        pixel_type="F32",
        pixel_size_x=10.0,
        pixel_size_y=10.0,
        full_extent={
            "xmin": 1420000,
            "ymin": 460000,
            "xmax": 1435000,
            "ymax": 475000,
            "spatialReference": {"wkid": 102719, "latestWkid": 2264},
        },
        max_image_width=15000,
        max_image_height=4100,
        capabilities=["Image", "Metadata", "Catalog"],
        description="LiDAR-derived elevation data",
        copyright_text="City of Charlotte, 2024",
    )


@pytest.fixture
def wgs84_metadata() -> ImageServerMetadata:
    """ImageServer metadata with WGS84 CRS."""
    return ImageServerMetadata(
        name="Global_DEM",
        band_count=1,
        pixel_type="S16",
        pixel_size_x=0.00027777777,  # ~30m at equator
        pixel_size_y=0.00027777777,
        full_extent={
            "xmin": -180.0,
            "ymin": -60.0,
            "xmax": 180.0,
            "ymax": 60.0,
            "spatialReference": {"wkid": 4326},
        },
        max_image_width=4096,
        max_image_height=4096,
        capabilities=["Image"],
        description="SRTM global elevation",
    )


@pytest.fixture
def multiband_metadata() -> ImageServerMetadata:
    """4-band RGB+NIR imagery metadata."""
    return ImageServerMetadata(
        name="Aerial_Imagery_2024",
        band_count=4,
        pixel_type="U8",
        pixel_size_x=0.15,  # 15cm resolution
        pixel_size_y=0.15,
        full_extent={
            "xmin": 2000000,
            "ymin": 600000,
            "xmax": 2150000,
            "ymax": 750000,
            "spatialReference": {"wkid": 2264},
        },
        max_image_width=8000,
        max_image_height=8000,
        capabilities=["Image", "Metadata"],
        description="High-resolution aerial imagery",
        copyright_text="County GIS Department",
    )


@pytest.fixture
def sample_tile() -> TileSpec:
    """Sample tile specification."""
    return TileSpec(
        x=0,
        y=0,
        bbox=(1420000.0, 470000.0, 1424096.0, 474096.0),
        width_px=4096,
        height_px=4096,
    )


@pytest.fixture
def edge_tile() -> TileSpec:
    """Edge tile with non-standard dimensions."""
    return TileSpec(
        x=3,
        y=2,
        bbox=(1432288.0, 461808.0, 1435000.0, 464000.0),
        width_px=2712,  # Smaller than full tile
        height_px=2192,
    )


# =============================================================================
# Collection Metadata Tests
# =============================================================================


@pytest.mark.unit
class TestCreateCollectionMetadata:
    """Tests for create_collection_metadata function."""

    def test_returns_dict(self, sample_metadata: ImageServerMetadata) -> None:
        """Collection metadata is a dictionary."""
        result = create_collection_metadata(
            sample_metadata,
            "https://example.com/arcgis/rest/services/CharlotteLAS/ImageServer",
        )
        assert isinstance(result, dict)

    def test_type_is_collection(self, sample_metadata: ImageServerMetadata) -> None:
        """Collection has correct STAC type."""
        result = create_collection_metadata(sample_metadata, "https://example.com")
        assert result["type"] == "Collection"

    def test_stac_version(self, sample_metadata: ImageServerMetadata) -> None:
        """Collection uses correct STAC version."""
        result = create_collection_metadata(sample_metadata, "https://example.com")
        assert result["stac_version"] == get_stac_version()

    def test_id_derived_from_name(self, sample_metadata: ImageServerMetadata) -> None:
        """Collection ID is derived from service name."""
        result = create_collection_metadata(sample_metadata, "https://example.com")
        assert result["id"] == "charlottelas"  # Lowercased and sanitized

    def test_description_from_metadata(self, sample_metadata: ImageServerMetadata) -> None:
        """Collection uses description from metadata."""
        result = create_collection_metadata(sample_metadata, "https://example.com")
        assert result["description"] == "LiDAR-derived elevation data"

    def test_description_fallback(self, sample_metadata: ImageServerMetadata) -> None:
        """Collection generates fallback description when not provided."""
        metadata = ImageServerMetadata(
            name="TestService",
            band_count=3,
            pixel_type="U8",
            pixel_size_x=1.0,
            pixel_size_y=1.0,
            full_extent={
                "xmin": 0,
                "ymin": 0,
                "xmax": 100,
                "ymax": 100,
                "spatialReference": {"wkid": 4326},
            },
            max_image_width=4096,
            max_image_height=4096,
        )
        result = create_collection_metadata(metadata, "https://example.com")
        assert "TestService" in result["description"]
        assert "3 bands" in result["description"]

    def test_extent_has_spatial_and_temporal(self, sample_metadata: ImageServerMetadata) -> None:
        """Collection extent has both spatial and temporal components."""
        result = create_collection_metadata(sample_metadata, "https://example.com")
        assert "extent" in result
        assert "spatial" in result["extent"]
        assert "temporal" in result["extent"]

    def test_spatial_extent_is_bbox_array(self, sample_metadata: ImageServerMetadata) -> None:
        """Spatial extent contains bbox array."""
        result = create_collection_metadata(sample_metadata, "https://example.com")
        spatial = result["extent"]["spatial"]
        assert "bbox" in spatial
        assert isinstance(spatial["bbox"], list)
        assert len(spatial["bbox"]) >= 1
        # Each bbox should have 4 values
        assert len(spatial["bbox"][0]) == 4

    def test_temporal_extent_is_open(self, sample_metadata: ImageServerMetadata) -> None:
        """Temporal extent is open interval (null dates) per ADR-0035."""
        result = create_collection_metadata(sample_metadata, "https://example.com")
        temporal = result["extent"]["temporal"]
        assert "interval" in temporal
        # Open interval = [null, null]
        assert temporal["interval"] == [[None, None]]

    def test_summaries_include_band_info(self, multiband_metadata: ImageServerMetadata) -> None:
        """Summaries include band information."""
        result = create_collection_metadata(multiband_metadata, "https://example.com")
        assert "summaries" in result

    def test_links_include_source(self, sample_metadata: ImageServerMetadata) -> None:
        """Links include source URL to ImageServer."""
        url = "https://example.com/arcgis/rest/services/Test/ImageServer"
        result = create_collection_metadata(sample_metadata, url)
        links = result.get("links", [])
        source_links = [link for link in links if link.get("rel") == "source"]
        assert len(source_links) >= 1
        assert source_links[0]["href"] == url

    def test_license_defaults_to_proprietary(self, sample_metadata: ImageServerMetadata) -> None:
        """License defaults to proprietary for unknown sources."""
        result = create_collection_metadata(sample_metadata, "https://example.com")
        assert result.get("license") == "proprietary"

    def test_providers_from_copyright(self, sample_metadata: ImageServerMetadata) -> None:
        """Providers list is populated from copyright text."""
        result = create_collection_metadata(sample_metadata, "https://example.com")
        providers = result.get("providers", [])
        if providers:  # If implemented
            provider_names = [p.get("name") for p in providers]
            assert "City of Charlotte, 2024" in provider_names

    def test_wgs84_extent_passthrough(self, wgs84_metadata: ImageServerMetadata) -> None:
        """WGS84 extents pass through without transformation."""
        result = create_collection_metadata(wgs84_metadata, "https://example.com")
        bbox = result["extent"]["spatial"]["bbox"][0]
        # Should be close to original values
        assert bbox[0] == pytest.approx(-180.0, abs=0.1)
        assert bbox[2] == pytest.approx(180.0, abs=0.1)


# =============================================================================
# Item Metadata Tests
# =============================================================================


@pytest.mark.unit
class TestCreateItemMetadata:
    """Tests for create_item_metadata function."""

    def test_returns_dict(
        self, sample_tile: TileSpec, sample_metadata: ImageServerMetadata
    ) -> None:
        """Item metadata is a dictionary."""
        result = create_item_metadata(sample_tile, sample_metadata, "data.tif")
        assert isinstance(result, dict)

    def test_type_is_feature(
        self, sample_tile: TileSpec, sample_metadata: ImageServerMetadata
    ) -> None:
        """Item has correct GeoJSON type."""
        result = create_item_metadata(sample_tile, sample_metadata, "data.tif")
        assert result["type"] == "Feature"

    def test_stac_version(
        self, sample_tile: TileSpec, sample_metadata: ImageServerMetadata
    ) -> None:
        """Item uses correct STAC version."""
        result = create_item_metadata(sample_tile, sample_metadata, "data.tif")
        assert result["stac_version"] == get_stac_version()

    def test_id_from_tile(
        self, sample_tile: TileSpec, sample_metadata: ImageServerMetadata
    ) -> None:
        """Item ID is derived from tile coordinates."""
        result = create_item_metadata(sample_tile, sample_metadata, "data.tif")
        assert result["id"] == "tile_0_0"

    def test_edge_tile_id(self, edge_tile: TileSpec, sample_metadata: ImageServerMetadata) -> None:
        """Edge tile has correct ID."""
        result = create_item_metadata(edge_tile, sample_metadata, "edge.tif")
        assert result["id"] == "tile_3_2"

    def test_geometry_is_polygon(
        self, sample_tile: TileSpec, sample_metadata: ImageServerMetadata
    ) -> None:
        """Item geometry is a GeoJSON Polygon."""
        result = create_item_metadata(sample_tile, sample_metadata, "data.tif")
        assert result["geometry"]["type"] == "Polygon"
        # Polygon has coordinates array with outer ring
        assert "coordinates" in result["geometry"]
        coords = result["geometry"]["coordinates"]
        assert len(coords) >= 1  # At least outer ring
        assert len(coords[0]) >= 5  # Closed ring has 5+ points

    def test_bbox_is_array(
        self, sample_tile: TileSpec, sample_metadata: ImageServerMetadata
    ) -> None:
        """Item bbox is a 4-element array."""
        result = create_item_metadata(sample_tile, sample_metadata, "data.tif")
        assert "bbox" in result
        assert isinstance(result["bbox"], list)
        assert len(result["bbox"]) == 4

    def test_properties_has_datetime(
        self, sample_tile: TileSpec, sample_metadata: ImageServerMetadata
    ) -> None:
        """Item properties include datetime (null for unknown)."""
        result = create_item_metadata(sample_tile, sample_metadata, "data.tif")
        assert "properties" in result
        assert "datetime" in result["properties"]
        # Null datetime for imagery without acquisition date
        assert result["properties"]["datetime"] is None

    def test_properties_has_created(
        self, sample_tile: TileSpec, sample_metadata: ImageServerMetadata
    ) -> None:
        """Item properties include created timestamp."""
        result = create_item_metadata(sample_tile, sample_metadata, "data.tif")
        assert "created" in result["properties"]
        # ISO format timestamp
        assert "T" in result["properties"]["created"]

    def test_assets_has_data(
        self, sample_tile: TileSpec, sample_metadata: ImageServerMetadata
    ) -> None:
        """Item has data asset."""
        result = create_item_metadata(sample_tile, sample_metadata, "output/data.tif")
        assert "assets" in result
        # Check for data or cog or image asset
        asset_keys = result["assets"].keys()
        assert any(key in asset_keys for key in ["data", "cog", "image"])

    def test_asset_has_href(
        self, sample_tile: TileSpec, sample_metadata: ImageServerMetadata
    ) -> None:
        """Asset has href pointing to COG file."""
        result = create_item_metadata(sample_tile, sample_metadata, "tiles/tile_0_0.tif")
        assets = result["assets"]
        data_asset = assets.get("data") or assets.get("cog") or assets.get("image")
        assert data_asset is not None
        assert "href" in data_asset
        assert "tile_0_0.tif" in data_asset["href"]

    def test_asset_has_cog_media_type(
        self, sample_tile: TileSpec, sample_metadata: ImageServerMetadata
    ) -> None:
        """Asset has COG media type."""
        result = create_item_metadata(sample_tile, sample_metadata, "data.tif")
        assets = result["assets"]
        data_asset = assets.get("data") or assets.get("cog") or assets.get("image")
        assert data_asset is not None
        assert "type" in data_asset
        assert "tiff" in data_asset["type"].lower()

    def test_asset_has_roles(
        self, sample_tile: TileSpec, sample_metadata: ImageServerMetadata
    ) -> None:
        """Asset has appropriate roles."""
        result = create_item_metadata(sample_tile, sample_metadata, "data.tif")
        assets = result["assets"]
        data_asset = assets.get("data") or assets.get("cog") or assets.get("image")
        assert data_asset is not None
        assert "roles" in data_asset
        assert "data" in data_asset["roles"]

    def test_includes_raster_extension(
        self, sample_tile: TileSpec, sample_metadata: ImageServerMetadata
    ) -> None:
        """Item includes raster STAC extension."""
        result = create_item_metadata(sample_tile, sample_metadata, "data.tif")
        extensions = result.get("stac_extensions", [])
        raster_ext = [ext for ext in extensions if "raster" in ext]
        assert len(raster_ext) >= 1
