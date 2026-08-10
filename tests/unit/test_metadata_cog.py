"""Tests for COG metadata extraction.

Assertions here pin exact values rather than "is not None". The fixtures are
committed and their georeferencing is fixed, so every extracted field has one
correct answer:

- ``raster/valid/singleband.tif``   64x64, 1 band uint8, EPSG:4326, no nodata
- ``raster/valid/nodata.tif``       same, nodata 255
- ``raster/valid/float32.tif``      same, float32
- ``realdata/rapidai4eo-sample.tif`` 200x200, 4 bands int16, EPSG:32633, 3 m
"""

from __future__ import annotations

import re
from pathlib import Path

import pytest

from portolan_cli.metadata.cog import COGMetadata, extract_cog_metadata, extract_schema_from_cog
from portolan_cli.models.schema import BandSchema, SchemaModel


def bands_of(schema: SchemaModel) -> list[BandSchema]:
    """Return a COG schema's columns, asserting each one is a BandSchema.

    `SchemaModel.columns` is a union covering vector and raster schemas, so a
    raster assertion has to narrow it. Doing that here keeps the narrowing (and
    the check that COG extraction really produces bands) in one place.
    """
    assert all(isinstance(column, BandSchema) for column in schema.columns)
    return [column for column in schema.columns if isinstance(column, BandSchema)]


# Georeferencing shared by the three synthetic 64x64 fixtures.
SYNTHETIC_BOUNDS = (-122.5, 37.7, -122.35, 37.85)
SYNTHETIC_PIXEL = 0.15 / 64

# The RapidAI4EO sample sits in UTM 33N on an exact 3 m grid, which makes it the
# fixture to assert resolution and transform against.
RAPIDAI4EO_BOUNDS = (364800.0, 4823400.0, 365400.0, 4824000.0)
RAPIDAI4EO_GDAL_TRANSFORM = (364800.0, 3.0, 0.0, 4824000.0, 0.0, -3.0)


class TestExtractCOGMetadata:
    """Tests for extract_cog_metadata()."""

    @pytest.mark.unit
    def test_returns_cog_metadata(self, valid_rgb_cog: Path) -> None:
        """Should return COGMetadata dataclass."""
        metadata = extract_cog_metadata(valid_rgb_cog)
        assert isinstance(metadata, COGMetadata)

    @pytest.mark.unit
    def test_bbox_is_left_bottom_right_top(self, valid_rgb_cog: Path) -> None:
        """bbox follows (minx, miny, maxx, maxy), not rasterio's own ordering."""
        metadata = extract_cog_metadata(valid_rgb_cog)
        assert metadata.bbox == RAPIDAI4EO_BOUNDS

    @pytest.mark.unit
    def test_bbox_of_geographic_fixture(self, valid_singleband_cog: Path) -> None:
        """A WGS84 fixture reports its degrees bbox unchanged."""
        metadata = extract_cog_metadata(valid_singleband_cog)
        assert metadata.bbox == SYNTHETIC_BOUNDS

    @pytest.mark.unit
    def test_crs_is_epsg_code_when_available(self, valid_rgb_cog: Path) -> None:
        """A CRS with an EPSG code is reported in "EPSG:NNNN" form."""
        metadata = extract_cog_metadata(valid_rgb_cog)
        assert metadata.crs == "EPSG:32633"

    @pytest.mark.unit
    def test_crs_of_geographic_fixture(self, valid_singleband_cog: Path) -> None:
        """The synthetic fixtures are WGS84."""
        metadata = extract_cog_metadata(valid_singleband_cog)
        assert metadata.crs == "EPSG:4326"

    @pytest.mark.unit
    def test_extracts_dimensions(self, valid_rgb_cog: Path) -> None:
        """Width and height come straight from the raster."""
        metadata = extract_cog_metadata(valid_rgb_cog)
        assert (metadata.width, metadata.height) == (200, 200)

    @pytest.mark.unit
    def test_extracts_band_count(self, valid_rgb_cog: Path) -> None:
        """The RapidAI4EO sample carries four bands."""
        metadata = extract_cog_metadata(valid_rgb_cog)
        assert metadata.band_count == 4

    @pytest.mark.unit
    def test_dtype_comes_from_first_band(self, valid_rgb_cog: Path) -> None:
        """dtype is the first band's type, as a plain string."""
        metadata = extract_cog_metadata(valid_rgb_cog)
        assert metadata.dtype == "int16"

    @pytest.mark.unit
    def test_dtype_of_float_fixture(self, valid_float32_cog: Path) -> None:
        """A float raster reports float32 rather than a default."""
        metadata = extract_cog_metadata(valid_float32_cog)
        assert metadata.dtype == "float32"

    @pytest.mark.unit
    def test_extracts_nodata(self, valid_nodata_cog: Path) -> None:
        """A raster with nodata set reports that exact value."""
        metadata = extract_cog_metadata(valid_nodata_cog)
        assert metadata.nodata == 255.0

    @pytest.mark.unit
    def test_nodata_is_none_when_unset(self, valid_singleband_cog: Path) -> None:
        """A raster without nodata reports None, not 0."""
        metadata = extract_cog_metadata(valid_singleband_cog)
        assert metadata.nodata is None

    @pytest.mark.unit
    def test_nodatavals_keeps_per_band_none_slots(self, valid_singleband_cog: Path) -> None:
        """An all-None nodatavals tuple survives; only an empty tuple becomes None.

        rasterio always returns one slot per band, so a raster with no nodata
        yields ``(None,)`` rather than ``None``. Consumers must read the slots,
        not test the tuple for truthiness.
        """
        metadata = extract_cog_metadata(valid_singleband_cog)
        assert metadata.nodatavals == (None,)

    @pytest.mark.unit
    def test_nodatavals_is_per_band_tuple(self, valid_nodata_cog: Path) -> None:
        """A raster with nodata reports one entry per band."""
        metadata = extract_cog_metadata(valid_nodata_cog)
        assert metadata.nodatavals == (255.0,)

    @pytest.mark.unit
    def test_resolution_is_positive_pixel_size(self, valid_rgb_cog: Path) -> None:
        """Resolution is the absolute pixel size; the y term is negative in the transform."""
        metadata = extract_cog_metadata(valid_rgb_cog)
        assert metadata.resolution == (3.0, 3.0)

    @pytest.mark.unit
    def test_resolution_of_geographic_fixture(self, valid_singleband_cog: Path) -> None:
        """A degrees-based fixture reports its degree pixel size."""
        metadata = extract_cog_metadata(valid_singleband_cog)
        x_res, y_res = metadata.resolution
        assert x_res == pytest.approx(SYNTHETIC_PIXEL)
        assert y_res == pytest.approx(SYNTHETIC_PIXEL)

    @pytest.mark.unit
    def test_transform_is_gdal_ordered(self, valid_rgb_cog: Path) -> None:
        """transform uses GDAL GeoTransform order, not affine order."""
        metadata = extract_cog_metadata(valid_rgb_cog)
        assert metadata.transform == RAPIDAI4EO_GDAL_TRANSFORM

    @pytest.mark.unit
    def test_raises_for_nonexistent_file(self, tmp_path: Path) -> None:
        """Should raise FileNotFoundError for missing file."""
        missing = tmp_path / "missing.tif"
        with pytest.raises(FileNotFoundError, match=re.escape(str(missing))):
            extract_cog_metadata(missing)

    @pytest.mark.unit
    def test_raises_for_non_raster(self, tmp_path: Path) -> None:
        """Should raise error for non-raster file."""
        import rasterio

        fake_file = tmp_path / "fake.tif"
        fake_file.write_bytes(b"not a tiff file")

        with pytest.raises(rasterio.errors.RasterioIOError):
            extract_cog_metadata(fake_file)


class TestCOGMetadataToDict:
    """Tests for COGMetadata.to_dict()."""

    @staticmethod
    def _metadata(**overrides: object) -> COGMetadata:
        """A fully-populated COGMetadata, overridable field by field."""
        kwargs: dict[str, object] = {
            "bbox": (0.0, 1.0, 2.0, 3.0),
            "crs": "EPSG:4326",
            "width": 64,
            "height": 32,
            "band_count": 2,
            "dtype": "uint8",
            "nodata": 255.0,
            "resolution": (0.5, 0.25),
            "nodatavals": (255.0, None),
            "transform": (0.0, 0.5, 0.0, 3.0, 0.0, -0.25),
        }
        kwargs.update(overrides)
        return COGMetadata(**kwargs)  # type: ignore[arg-type]

    @pytest.mark.unit
    def test_full_metadata_round_trips_every_field(self) -> None:
        """Every field lands in the dict, with tuples widened to lists."""
        assert self._metadata().to_dict() == {
            "bbox": [0.0, 1.0, 2.0, 3.0],
            "crs": "EPSG:4326",
            "width": 64,
            "height": 32,
            "band_count": 2,
            "dtype": "uint8",
            "nodata": 255.0,
            "resolution": [0.5, 0.25],
            "nodatavals": [255.0, None],
            "transform": [0.0, 0.5, 0.0, 3.0, 0.0, -0.25],
        }

    @pytest.mark.unit
    def test_omits_nodatavals_when_absent(self) -> None:
        """nodatavals is optional and stays out of the dict when unset."""
        result = self._metadata(nodatavals=None).to_dict()
        assert "nodatavals" not in result

    @pytest.mark.unit
    def test_omits_transform_when_absent(self) -> None:
        """transform is optional and stays out of the dict when unset."""
        result = self._metadata(transform=None).to_dict()
        assert "transform" not in result

    @pytest.mark.unit
    def test_preserves_none_entries_inside_nodatavals(self) -> None:
        """A band with no nodata keeps its None slot rather than being dropped."""
        result = self._metadata(nodatavals=(None, None, 7.0)).to_dict()
        assert result["nodatavals"] == [None, None, 7.0]

    @pytest.mark.unit
    def test_extracted_metadata_serializes(self, valid_nodata_cog: Path) -> None:
        """A real extraction produces the same shape."""
        result = extract_cog_metadata(valid_nodata_cog).to_dict()
        assert result["bbox"] == list(SYNTHETIC_BOUNDS)
        assert result["nodatavals"] == [255.0]


class TestCOGMetadataToSTACProperties:
    """Tests for COGMetadata.to_stac_properties()."""

    @staticmethod
    def _metadata(**overrides: object) -> COGMetadata:
        """Three-band metadata with no nodata anywhere, overridable."""
        kwargs: dict[str, object] = {
            "bbox": (0.0, 0.0, 1.0, 1.0),
            "crs": "EPSG:4326",
            "width": 64,
            "height": 64,
            "band_count": 3,
            "dtype": "uint8",
            "nodata": None,
            "resolution": (2.0, 4.0),
            "nodatavals": None,
            "transform": None,
        }
        kwargs.update(overrides)
        return COGMetadata(**kwargs)  # type: ignore[arg-type]

    @pytest.mark.unit
    def test_bands_are_named_one_based(self) -> None:
        """Band names count from 1, matching STAC and GDAL conventions."""
        props = self._metadata().to_stac_properties()
        assert [band["name"] for band in props["bands"]] == ["band_1", "band_2", "band_3"]

    @pytest.mark.unit
    def test_every_band_carries_the_data_type(self) -> None:
        """data_type is repeated on each band entry."""
        props = self._metadata(dtype="float32").to_stac_properties()
        assert [band["data_type"] for band in props["bands"]] == ["float32"] * 3

    @pytest.mark.unit
    def test_uses_unified_bands_key_not_raster_bands(self) -> None:
        """STAC v1.1.0 uses `bands`; `raster:bands` is the superseded spelling."""
        props = self._metadata().to_stac_properties()
        assert "bands" in props
        assert "raster:bands" not in props

    @pytest.mark.unit
    def test_per_band_nodata_wins_over_uniform(self) -> None:
        """When nodatavals is present it supplies each band's nodata."""
        metadata = self._metadata(nodatavals=(1.0, 2.0, 3.0), nodata=99.0)
        props = metadata.to_stac_properties()
        assert [band["nodata"] for band in props["bands"]] == [1.0, 2.0, 3.0]

    @pytest.mark.unit
    def test_none_inside_nodatavals_omits_that_band(self) -> None:
        """A None slot means that band has no nodata, and does not fall back."""
        metadata = self._metadata(nodatavals=(None, 2.0, None), nodata=99.0)
        bands = metadata.to_stac_properties()["bands"]
        assert "nodata" not in bands[0]
        assert bands[1]["nodata"] == 2.0
        assert "nodata" not in bands[2]

    @pytest.mark.unit
    def test_short_nodatavals_falls_back_to_uniform(self) -> None:
        """Bands past the end of nodatavals use the uniform nodata."""
        metadata = self._metadata(nodatavals=(1.0,), nodata=99.0)
        assert [band["nodata"] for band in metadata.to_stac_properties()["bands"]] == [
            1.0,
            99.0,
            99.0,
        ]

    @pytest.mark.unit
    def test_uniform_nodata_applies_to_all_bands(self) -> None:
        """With no per-band values, every band gets the uniform nodata."""
        props = self._metadata(nodata=7.5).to_stac_properties()
        assert [band["nodata"] for band in props["bands"]] == [7.5, 7.5, 7.5]

    @pytest.mark.unit
    def test_omits_nodata_when_unset_everywhere(self) -> None:
        """No nodata anywhere means no nodata key on any band."""
        props = self._metadata().to_stac_properties()
        assert all("nodata" not in band for band in props["bands"])

    @pytest.mark.unit
    def test_spatial_resolution_averages_the_two_axes(self) -> None:
        """raster:spatial_resolution is the mean of x and y pixel size."""
        props = self._metadata(resolution=(2.0, 4.0)).to_stac_properties()
        assert props["raster:spatial_resolution"] == 3.0

    @pytest.mark.unit
    def test_band_count_drives_the_band_list_length(self) -> None:
        """A single-band raster produces exactly one band entry."""
        props = self._metadata(band_count=1).to_stac_properties()
        assert len(props["bands"]) == 1

    @pytest.mark.unit
    def test_extracted_metadata_produces_stac_bands(self, valid_nodata_cog: Path) -> None:
        """A real extraction yields one nodata-carrying band."""
        props = extract_cog_metadata(valid_nodata_cog).to_stac_properties()
        assert props["bands"] == [{"name": "band_1", "data_type": "uint8", "nodata": 255.0}]


class TestExtractSchemaFromCOG:
    """Tests for extract_schema_from_cog()."""

    @pytest.mark.unit
    def test_schema_header_fields(self, valid_singleband_cog: Path) -> None:
        """The schema declares version 1.0.0, format cog, and the raster CRS."""
        schema = extract_schema_from_cog(valid_singleband_cog)
        assert schema.schema_version == "1.0.0"
        assert schema.format == "cog"
        assert schema.crs == "EPSG:4326"

    @pytest.mark.unit
    def test_one_band_column_per_raster_band(self, valid_rgb_cog: Path) -> None:
        """Each raster band becomes a one-based BandSchema column."""
        schema = extract_schema_from_cog(valid_rgb_cog)
        assert [band.name for band in bands_of(schema)] == [
            "band_1",
            "band_2",
            "band_3",
            "band_4",
        ]

    @pytest.mark.unit
    def test_columns_are_band_schemas_carrying_dtype(self, valid_rgb_cog: Path) -> None:
        """Columns are BandSchema instances typed from the raster."""
        schema = extract_schema_from_cog(valid_rgb_cog)
        assert {band.data_type for band in bands_of(schema)} == {"int16"}

    @pytest.mark.unit
    def test_band_nodata_is_populated(self, valid_nodata_cog: Path) -> None:
        """A raster with nodata puts that value on its band."""
        schema = extract_schema_from_cog(valid_nodata_cog)
        assert [band.nodata for band in bands_of(schema)] == [255.0]

    @pytest.mark.unit
    def test_band_nodata_is_none_when_unset(self, valid_singleband_cog: Path) -> None:
        """A raster without nodata leaves the band's nodata unset."""
        schema = extract_schema_from_cog(valid_singleband_cog)
        assert [band.nodata for band in bands_of(schema)] == [None]

    @pytest.mark.unit
    def test_band_description_defaults_to_none(self, valid_singleband_cog: Path) -> None:
        """Undescribed bands carry no description."""
        schema = extract_schema_from_cog(valid_singleband_cog)
        assert [band.description for band in bands_of(schema)] == [None]

    @pytest.mark.unit
    def test_band_description_is_read_from_the_raster(self, tmp_path: Path) -> None:
        """A band description set on the file reaches the schema."""
        import numpy as np
        import rasterio
        from rasterio.transform import from_bounds

        path = tmp_path / "described.tif"
        with rasterio.open(
            path,
            "w",
            driver="GTiff",
            height=8,
            width=8,
            count=2,
            dtype="uint8",
            crs="EPSG:4326",
            transform=from_bounds(0, 0, 1, 1, 8, 8),
        ) as dst:
            dst.write(np.zeros((8, 8), dtype="uint8"), 1)
            dst.write(np.zeros((8, 8), dtype="uint8"), 2)
            dst.set_band_description(1, "red")
            dst.set_band_description(2, "nir")

        schema = extract_schema_from_cog(path)
        assert [band.description for band in bands_of(schema)] == ["red", "nir"]

    @pytest.mark.unit
    def test_no_warnings_when_crs_present(self, valid_singleband_cog: Path) -> None:
        """A georeferenced raster warns about nothing."""
        _, warnings = extract_schema_from_cog(valid_singleband_cog, return_warnings=True)
        assert warnings == []

    @pytest.mark.unit
    def test_warns_and_leaves_crs_unset_without_crs(
        self, invalid_not_georeferenced_tif: Path
    ) -> None:
        """A raster with no CRS warns once and reports crs=None."""
        schema, warnings = extract_schema_from_cog(
            invalid_not_georeferenced_tif, return_warnings=True
        )
        assert schema.crs is None
        assert warnings == ["Raster has no CRS defined. Consider adding CRS metadata."]

    @pytest.mark.unit
    def test_default_return_is_a_bare_schema(self, valid_singleband_cog: Path) -> None:
        """Without return_warnings the result is the SchemaModel itself."""
        result = extract_schema_from_cog(valid_singleband_cog)
        assert not isinstance(result, tuple)

    @pytest.mark.unit
    def test_raises_for_nonexistent_file(self, tmp_path: Path) -> None:
        """A missing path fails before rasterio is reached."""
        missing = tmp_path / "missing.tif"
        with pytest.raises(FileNotFoundError, match=re.escape(str(missing))):
            extract_schema_from_cog(missing)


class TestCOGMetadataEdgeCases:
    """Tests for edge cases in COG metadata extraction."""

    @pytest.mark.unit
    def test_cog_without_epsg(self, invalid_not_georeferenced_tif: Path) -> None:
        """COG without valid EPSG returns WKT or None for CRS."""
        # The not_georeferenced.tif should have no CRS
        metadata = extract_cog_metadata(invalid_not_georeferenced_tif)
        # Should be None since no CRS
        assert metadata.crs is None

    @pytest.mark.unit
    def test_extract_crs_wkt_fallback(self, tmp_path: Path) -> None:
        """Falls back to WKT when no EPSG code is available.

        The CRS below is a Mercator with a non-standard central meridian, so no
        EPSG code matches it and ``to_epsg()`` returns None.
        """
        import numpy as np
        import rasterio
        from rasterio.crs import CRS as RasterioCRS
        from rasterio.transform import from_bounds

        custom_wkt = """PROJCS["Custom_CRS",
            GEOGCS["GCS_WGS_1984",
                DATUM["D_WGS_1984",SPHEROID["WGS_1984",6378137,298.257223563]],
                PRIMEM["Greenwich",0],UNIT["Degree",0.017453292519943295]],
            PROJECTION["Mercator_1SP"],
            PARAMETER["central_meridian",17.5],
            PARAMETER["scale_factor",1],
            PARAMETER["false_easting",0],
            PARAMETER["false_northing",0],
            UNIT["Meter",1]]"""

        crs = RasterioCRS.from_wkt(custom_wkt)
        assert crs.to_epsg() is None, "fixture CRS must have no EPSG code"

        path = tmp_path / "custom_crs.tif"
        with rasterio.open(
            path,
            "w",
            driver="GTiff",
            height=64,
            width=64,
            count=1,
            dtype="uint8",
            crs=crs,
            transform=from_bounds(0, 0, 1, 1, 64, 64),
        ) as dst:
            dst.write(np.zeros((64, 64), dtype="uint8"), 1)

        metadata = extract_cog_metadata(path)
        assert metadata.crs is not None
        assert metadata.crs.startswith("PROJCS")

    @pytest.mark.unit
    def test_schema_crs_wkt_fallback(self, tmp_path: Path) -> None:
        """extract_schema_from_cog takes the same WKT fallback."""
        import numpy as np
        import rasterio
        from rasterio.crs import CRS as RasterioCRS
        from rasterio.transform import from_bounds

        custom_wkt = """PROJCS["Custom_CRS",
            GEOGCS["GCS_WGS_1984",
                DATUM["D_WGS_1984",SPHEROID["WGS_1984",6378137,298.257223563]],
                PRIMEM["Greenwich",0],UNIT["Degree",0.017453292519943295]],
            PROJECTION["Mercator_1SP"],
            PARAMETER["central_meridian",17.5],
            PARAMETER["scale_factor",1],
            PARAMETER["false_easting",0],
            PARAMETER["false_northing",0],
            UNIT["Meter",1]]"""

        crs = RasterioCRS.from_wkt(custom_wkt)
        assert crs.to_epsg() is None, "fixture CRS must have no EPSG code"

        path = tmp_path / "custom_crs_schema.tif"
        with rasterio.open(
            path,
            "w",
            driver="GTiff",
            height=8,
            width=8,
            count=1,
            dtype="uint8",
            crs=crs,
            transform=from_bounds(0, 0, 1, 1, 8, 8),
        ) as dst:
            dst.write(np.zeros((8, 8), dtype="uint8"), 1)

        schema, warnings = extract_schema_from_cog(path, return_warnings=True)
        assert schema.crs is not None
        assert schema.crs.startswith("PROJCS")
        assert warnings == []
