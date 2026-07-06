"""Tests for the single-source extension registry (issue #558).

These pin the *derived* vocabulary to explicit expected literals, so the test
fails loudly if a registry row is added/removed/mis-typed. They also assert the
structural invariants (disjointness, uniqueness) and that the four consuming
modules expose exactly the registry-derived values.
"""

from __future__ import annotations

import pytest

from portolan_cli import extension_registry as reg

pytestmark = pytest.mark.unit


class TestDerivedSets:
    """Registry derivations equal their expected post-reconciliation literals."""

    def test_cloud_native_extensions(self) -> None:
        # .raquet dropped (#487); .parquet/.tif are "inspect"; .zarr/.copc.laz
        # are cloud-native but dir/compound so excluded from the static set.
        assert reg.cloud_native_extensions() == {".fgb", ".pmtiles"}

    def test_convertible_vector_extensions(self) -> None:
        assert reg.convertible_extensions("GeoParquet") == {
            ".shp",
            ".geojson",
            ".gpkg",
            ".gdb",
            ".csv",
            ".tsv",
        }

    def test_convertible_raster_extensions(self) -> None:
        assert reg.convertible_extensions("COG") == {".jp2"}

    def test_unsupported_extensions(self) -> None:
        assert reg.unsupported_extensions() == {
            ".nc",
            ".netcdf",
            ".h5",
            ".hdf5",
            ".las",
            ".laz",
        }

    def test_vector_routing_extensions(self) -> None:
        assert reg.extensions_where(routes_as="vector") == {
            ".geojson",
            ".parquet",
            ".shp",
            ".gpkg",
            ".fgb",
            ".pmtiles",
            ".gdb",
            ".csv",
            ".tsv",
        }

    def test_raster_routing_extensions(self) -> None:
        assert reg.extensions_where(routes_as="raster") == {".tif", ".tiff", ".jp2"}

    def test_multilayer_extensions(self) -> None:
        assert reg.extensions_where(is_multilayer=True) == {".gpkg", ".gdb"}

    def test_geospatial_extensions(self) -> None:
        assert reg.extensions_where(is_geospatial=True) == {
            ".geojson",
            ".parquet",
            ".shp",
            ".gpkg",
            ".fgb",
            ".gdb",
            ".csv",
            ".tsv",
            ".tif",
            ".tiff",
            ".jp2",
            ".pmtiles",
        }

    def test_tabular_extensions(self) -> None:
        assert reg.extensions_where(is_tabular=True) == {
            ".csv",
            ".tsv",
            ".parquet",
            ".xlsx",
            ".xls",
        }

    def test_scan_geo_asset_extensions(self) -> None:
        assert reg.extensions_where(scan_category="geo_asset") == {
            ".geojson",
            ".shp",
            ".gpkg",
            ".fgb",
            ".tif",
            ".tiff",
            ".jp2",
            ".pmtiles",
        }

    def test_scan_sidecar_extensions(self) -> None:
        assert reg.extensions_where(scan_category="known_sidecar") == {
            ".dbf",
            ".shx",
            ".prj",
            ".cpg",
            ".sbn",
            ".sbx",
            ".ovr",
            ".xml",
        }

    def test_scan_image_extensions_include_svg(self) -> None:
        # .svg added so scan agrees with add._ROLE_MAP (#558).
        assert reg.extensions_where(scan_category="thumbnail") == {
            ".png",
            ".jpg",
            ".jpeg",
            ".webp",
            ".gif",
            ".svg",
        }

    def test_scan_junk_extensions(self) -> None:
        assert reg.extensions_where(scan_category="junk") == {
            ".exe",
            ".dll",
            ".so",
            ".dylib",
            ".pyc",
            ".pyo",
            ".class",
            ".o",
            ".obj",
        }


class TestDerivedMaps:
    """Display-name / media-type / role maps."""

    def test_format_display_names_drop_raquet(self) -> None:
        names = reg.field_map("display_name")
        assert ".raquet" not in names
        assert names[".parquet"] == "GeoParquet"
        assert names[".fgb"] == "FlatGeobuf"
        # Exactly the 18 human-labelled formats, nothing more.
        assert set(names) == {
            ".parquet",
            ".fgb",
            ".pmtiles",
            ".tif",
            ".tiff",
            ".shp",
            ".geojson",
            ".gpkg",
            ".csv",
            ".tsv",
            ".json",
            ".jp2",
            ".nc",
            ".netcdf",
            ".h5",
            ".hdf5",
            ".las",
            ".laz",
        }

    def test_media_types_include_webp_gif_and_unsupported(self) -> None:
        media = reg.field_map("media_type")
        assert media[".webp"] == "image/webp"
        assert media[".gif"] == "image/gif"
        assert media[".las"] == "application/vnd.laszip" or media[".las"] == "application/vnd.las"
        assert media[".nc"] == "application/x-netcdf"

    def test_roles_include_webp_gif(self) -> None:
        roles = reg.field_map("role")
        assert roles[".webp"] == "thumbnail"
        assert roles[".gif"] == "thumbnail"
        assert roles[".svg"] == "thumbnail"

    def test_unsupported_error_messages(self) -> None:
        messages = reg.field_map("unsupported_message")
        assert set(messages) == {".nc", ".netcdf", ".h5", ".hdf5", ".las", ".laz"}
        assert "COPC" in messages[".las"]


class TestInvariants:
    def test_no_duplicate_extensions(self) -> None:
        exts = [spec.ext for spec in reg.EXTENSION_REGISTRY]
        assert len(exts) == len(set(exts))

    def test_all_extensions_lowercase_dotted(self) -> None:
        for spec in reg.EXTENSION_REGISTRY:
            assert spec.ext.startswith("."), spec.ext
            assert spec.ext == spec.ext.lower(), spec.ext

    def test_cloud_native_convertible_unsupported_disjoint(self) -> None:
        cn = reg.cloud_native_extensions()
        cv = reg.convertible_extensions("GeoParquet")
        cr = reg.convertible_extensions("COG")
        un = reg.unsupported_extensions()
        assert cn.isdisjoint(cv)
        assert cn.isdisjoint(cr)
        assert cv.isdisjoint(un)
        assert cn.isdisjoint(un)

    def test_additional_cloud_native_are_dir_or_compound(self) -> None:
        extras = {
            spec.ext
            for spec in reg.EXTENSION_REGISTRY
            if spec.cloud_native == "yes" and (spec.is_dir or spec.is_compound)
        }
        assert extras == {".zarr", ".copc.laz"}


class TestModulesUseRegistry:
    """The four consuming modules expose exactly the registry-derived values."""

    def test_formats_module(self) -> None:
        from portolan_cli import formats

        assert formats.CLOUD_NATIVE_EXTENSIONS == reg.cloud_native_extensions()
        assert formats.CONVERTIBLE_VECTOR_EXTENSIONS == reg.convertible_extensions("GeoParquet")
        assert formats.CONVERTIBLE_RASTER_EXTENSIONS == reg.convertible_extensions("COG")
        assert formats.UNSUPPORTED_EXTENSIONS == reg.unsupported_extensions()
        assert formats.VECTOR_EXTENSIONS == reg.extensions_where(routes_as="vector")
        assert formats.RASTER_EXTENSIONS == reg.extensions_where(routes_as="raster")
        assert formats.MULTILAYER_EXTENSIONS == reg.extensions_where(is_multilayer=True)
        assert formats.FORMAT_DISPLAY_NAMES == reg.field_map("display_name")
        assert formats.UNSUPPORTED_ERROR_MESSAGES == reg.field_map("unsupported_message")

    def test_constants_module(self) -> None:
        from portolan_cli import constants

        assert constants.GEOSPATIAL_EXTENSIONS == reg.extensions_where(is_geospatial=True)
        assert constants.TABULAR_EXTENSIONS == reg.extensions_where(is_tabular=True)
        assert constants.SIDECAR_PATTERNS == {k: list(v) for k, v in reg.SIDECAR_OF.items()}

    def test_scan_classify_module(self) -> None:
        from portolan_cli import scan_classify

        assert scan_classify.GEO_ASSET_EXTENSIONS == reg.extensions_where(scan_category="geo_asset")
        assert scan_classify.SIDECAR_EXTENSIONS == reg.extensions_where(
            scan_category="known_sidecar"
        )
        assert scan_classify.TABULAR_EXTENSIONS == reg.extensions_where(is_tabular=True)
        assert scan_classify.IMAGE_EXTENSIONS == reg.extensions_where(scan_category="thumbnail")
        assert scan_classify.JUNK_DIRS == reg.JUNK_DIRS

    def test_add_module(self) -> None:
        from portolan_cli import add

        assert add._MEDIA_TYPE_MAP == reg.field_map("media_type")
        assert add._ROLE_MAP == reg.field_map("role")
