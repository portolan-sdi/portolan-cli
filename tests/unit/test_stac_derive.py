"""Tests for the collection fields derived from the members (issues #801, #852, #1023).

``derive_collection_fields`` is pure: the members and metadata.yaml go in, the
extent and summaries come out. The stored collection is never an input, so a
wrong stored value cannot survive a rebuild.
"""

from __future__ import annotations

from datetime import datetime, timezone

import pystac
import pytest

from portolan_cli.errors import InvalidBboxError
from portolan_cli.stac import (
    CollectionMember,
    apply_collection_fields,
    apply_collection_fields_to_dict,
    build_spatial_extent,
    create_collection,
    derive_collection_fields,
    summarize_properties,
)

pytestmark = pytest.mark.unit

# The asset transform and shape of the CARTO valencia-2024 flood collection
# (issue #1023). A north-up GeoTransform holds two zeros.
VALENCIA_TRANSFORM = [697917.4537159235, 20.0, 0.0, 4390253.961476656, 0.0, -20.0]


def _utc(year: int, month: int, day: int) -> datetime:
    return datetime(year, month, day, tzinfo=timezone.utc)


class TestBuildSpatialExtent:
    """The one constructor of a collection spatial extent applies the STAC rule."""

    def test_one_bbox_for_a_plain_union(self) -> None:
        extent = build_spatial_extent([[0.0, 0.0, 1.0, 1.0], [2.0, 2.0, 3.0, 3.0]])
        assert extent.bboxes == [[0.0, 0.0, 3.0, 3.0]]

    def test_a_fiji_item_gives_envelope_and_two_parts(self) -> None:
        """Issue #852: a crossing member writes three bboxes, never two."""
        extent = build_spatial_extent([[-74.0, 40.0, -73.0, 41.0], [177.0, -20.0, -175.0, -15.0]])
        assert extent.bboxes == [
            [-180.0, -20.0, 180.0, 41.0],
            [-180.0, -20.0, -73.0, 41.0],
            [177.0, -20.0, 180.0, -15.0],
        ]

    def test_no_valid_bbox_raises(self) -> None:
        with pytest.raises(InvalidBboxError):
            build_spatial_extent([[float("inf"), 0.0, 1.0, 1.0]])

    def test_a_projected_bbox_is_not_a_wgs84_extent(self) -> None:
        with pytest.raises(InvalidBboxError):
            build_spatial_extent([[500000.0, 4000000.0, 600000.0, 4100000.0]])

    def test_a_projected_bbox_passes_without_the_wgs84_ranges(self) -> None:
        extent = build_spatial_extent(
            [[500000.0, 4000000.0, 600000.0, 4100000.0]], wgs84_only=False
        )
        assert extent.bboxes == [[500000.0, 4000000.0, 600000.0, 4100000.0]]


class TestDeriveExtent:
    """The extent is the union of the members, never of the stored value (#801)."""

    def test_extent_is_the_union_of_the_members(self) -> None:
        fields = derive_collection_fields(
            [
                CollectionMember(bbox=[-7.1, 37.9, -6.9, 38.1]),
                CollectionMember(bbox=[19.9, 49.9, 20.1, 50.1]),
            ],
            {},
        )
        assert fields.spatial is not None
        assert fields.spatial.bboxes == [[-7.1, 37.9, 20.1, 50.1]]

    def test_applying_the_fields_shrinks_a_widened_extent(self) -> None:
        """The SITEX case of #801: an absurd stored extent is replaced."""
        collection = create_collection(
            collection_id="montes", description="d", bbox=[-20.0, 30.0, 10.0, 50.0]
        )
        sitex = [-7.334979717591043, 38.04066304591087, -4.704239370531218, 40.486611484793]
        apply_collection_fields(
            collection, derive_collection_fields([CollectionMember(bbox=sitex)], {})
        )
        assert collection.extent.spatial.bboxes == [sitex]

    def test_metadata_yaml_widens_the_extent(self) -> None:
        """A human bbox is the only override, and the result still holds the members."""
        fields = derive_collection_fields(
            [CollectionMember(bbox=[1.0, 1.0, 2.0, 2.0])],
            {"extent": {"bbox": [0.0, 0.0, 1.5, 1.5]}},
        )
        assert fields.spatial is not None
        assert fields.spatial.bboxes == [[0.0, 0.0, 2.0, 2.0]]

    def test_the_stac_form_in_metadata_yaml(self) -> None:
        """``extent.spatial.bbox`` holds a list of bboxes, and the first counts."""
        fields = derive_collection_fields(
            [CollectionMember(bbox=[1.0, 1.0, 2.0, 2.0])],
            {"extent": {"spatial": {"bbox": [[0.0, 0.0, 3.0, 3.0]]}}},
        )
        assert fields.spatial is not None
        assert fields.spatial.bboxes == [[0.0, 0.0, 3.0, 3.0]]

    def test_top_level_bbox_in_metadata_yaml(self) -> None:
        fields = derive_collection_fields([], {"bbox": [5.0, 5.0, 6.0, 6.0]})
        assert fields.spatial is not None
        assert fields.spatial.bboxes == [[5.0, 5.0, 6.0, 6.0]]

    def test_no_footprint_keeps_the_stored_extent(self) -> None:
        """A tabular collection has no footprint, so its stored AOI stays."""
        collection = create_collection(
            collection_id="table", description="d", bbox=[-75.0, 39.0, -74.0, 40.0]
        )
        apply_collection_fields(collection, derive_collection_fields([CollectionMember()], {}))
        assert collection.extent.spatial.bboxes == [[-75.0, 39.0, -74.0, 40.0]]

    def test_invalid_member_bboxes_derive_no_extent(self) -> None:
        fields = derive_collection_fields(
            [CollectionMember(bbox=[float("nan"), 0.0, 1.0, 1.0])], {}
        )
        assert fields.spatial is None


class TestDeriveTemporal:
    def test_interval_spans_the_members(self) -> None:
        fields = derive_collection_fields(
            [
                CollectionMember(start=_utc(2024, 6, 30), end=_utc(2024, 6, 30)),
                CollectionMember(start=_utc(2024, 1, 15), end=_utc(2024, 3, 1)),
            ],
            {},
        )
        assert fields.temporal is not None
        assert fields.temporal.intervals == [[_utc(2024, 1, 15), _utc(2024, 6, 30)]]

    def test_no_time_derives_no_interval(self) -> None:
        assert derive_collection_fields([CollectionMember()], {}).temporal is None


class TestSummaries:
    """Summaries keep list values whole, and skip per-item grids (#1023)."""

    def test_transform_and_square_shape_are_not_flattened(self) -> None:
        """On main the summary was the five distinct numbers of the transform."""
        summaries = summarize_properties(
            [
                {
                    "proj:transform": VALENCIA_TRANSFORM,
                    "proj:shape": [200, 200],
                    "proj:code": "EPSG:25830",
                }
            ]
        )
        assert summaries == {"proj:code": ["EPSG:25830"]}

    def test_a_list_of_numbers_is_one_value(self) -> None:
        summaries = summarize_properties(
            [{"custom:range": [0, 0]}, {"custom:range": [0, 0]}, {"custom:range": [1, 2]}]
        )
        assert summaries == {"custom:range": [[0, 0], [1, 2]]}

    def test_geometry_types_union_in_order(self) -> None:
        summaries = summarize_properties(
            [
                {"vector:geometry_types": ["Polygon"]},
                {"vector:geometry_types": ["Polygon", "MultiPolygon"]},
            ]
        )
        assert summaries == {"vector:geometry_types": ["Polygon", "MultiPolygon"]}

    def test_proj_code_distinct(self) -> None:
        summaries = summarize_properties([{"proj:code": "EPSG:4326"}, {"proj:code": "EPSG:32618"}])
        assert summaries == {"proj:code": ["EPSG:4326", "EPSG:32618"]}

    def test_gsd_is_a_range(self) -> None:
        summaries = summarize_properties([{"gsd": 10}, {"gsd": 30}, {"gsd": 20}])
        assert summaries == {"gsd": {"minimum": 10, "maximum": 30}}

    def test_a_gsd_that_is_not_a_number_is_skipped(self) -> None:
        summaries = summarize_properties([{"gsd": "ten"}, {"gsd": True}, {"gsd": 10}])
        assert summaries == {"gsd": {"minimum": 10, "maximum": 10}}

    def test_fields_after_gsd_are_summarized(self) -> None:
        summaries = summarize_properties([{"gsd": 10, "proj:code": "EPSG:4326"}])
        assert summaries == {
            "gsd": {"minimum": 10, "maximum": 10},
            "proj:code": ["EPSG:4326"],
        }

    def test_unprefixed_fields_are_not_summarized(self) -> None:
        assert summarize_properties([{"title": "A", "datetime": "2024-01-01"}]) == {}

    def test_a_stale_summary_is_replaced_and_a_foreign_one_kept(self) -> None:
        collection = create_collection(collection_id="c", description="d")
        collection.summaries = pystac.Summaries(
            {"proj:code": ["EPSG:3857"], "bands": [{"name": "b1"}]}
        )
        fields = derive_collection_fields(
            [CollectionMember(properties={"proj:code": "EPSG:4326"})], {}
        )
        apply_collection_fields(collection, fields)
        assert collection.summaries.to_dict() == {
            "bands": [{"name": "b1"}],
            "proj:code": ["EPSG:4326"],
        }

    def test_a_stored_grid_summary_is_dropped(self) -> None:
        """A catalog from an older release stores the flattened transform (#1023)."""
        data: dict[str, object] = {
            "summaries": {
                "proj:transform": [364800.0, 3.0, 0.0, 4824000.0, -3.0],
                "proj:shape": [200],
            },
            "stac_extensions": [
                "https://stac-extensions.github.io/projection/v2.0.0/schema.json"
            ],
        }
        fields = derive_collection_fields(
            [CollectionMember(properties={"proj:transform": VALENCIA_TRANSFORM})], {}
        )
        apply_collection_fields_to_dict(data, fields)
        assert "summaries" not in data
        assert data["stac_extensions"] == []


class TestApplyToDict:
    def test_dict_gets_extent_summaries_and_extension(self) -> None:
        data: dict[str, object] = {
            "extent": {
                "spatial": {"bbox": [[-20.0, 30.0, 10.0, 50.0]]},
                "temporal": {"interval": [[None, None]]},
            },
            "summaries": {"proj:code": ["EPSG:3857"]},
            "stac_extensions": [],
        }
        fields = derive_collection_fields(
            [
                CollectionMember(
                    bbox=[1.0, 2.0, 3.0, 4.0],
                    start=_utc(2020, 1, 1),
                    end=_utc(2020, 1, 1),
                    properties={"proj:code": "EPSG:4326"},
                )
            ],
            {},
        )
        apply_collection_fields_to_dict(data, fields)
        assert data["extent"] == {
            "spatial": {"bbox": [[1.0, 2.0, 3.0, 4.0]]},
            "temporal": {"interval": [["2020-01-01T00:00:00Z", "2020-01-01T00:00:00Z"]]},
        }
        assert data["summaries"] == {"proj:code": ["EPSG:4326"]}
        assert data["stac_extensions"] == [
            "https://stac-extensions.github.io/projection/v2.0.0/schema.json"
        ]

    def test_no_extension_needed_adds_no_key(self) -> None:
        data: dict[str, object] = {"id": "c"}
        apply_collection_fields_to_dict(data, derive_collection_fields([], {}))
        assert "stac_extensions" not in data

    def test_no_summaries_left_drops_the_key(self) -> None:
        data: dict[str, object] = {"summaries": {"proj:code": ["EPSG:3857"]}}
        apply_collection_fields_to_dict(data, derive_collection_fields([], {}))
        assert "summaries" not in data

    def test_an_extension_only_a_gone_summary_used_is_withdrawn(self) -> None:
        """A stale raster declaration fails the raster schema (PTL-EXT-001)."""
        raster = "https://stac-extensions.github.io/raster/v2.0.0/schema.json"
        projection = "https://stac-extensions.github.io/projection/v2.0.0/schema.json"
        data: dict[str, object] = {
            "summaries": {"raster:spatial_resolution": [0.5], "proj:code": ["EPSG:4326"]},
            "stac_extensions": [raster, projection],
            "assets": {"data": {"href": "./a.tif", "proj:code": "EPSG:4326"}},
        }
        apply_collection_fields_to_dict(data, derive_collection_fields([], {}))
        assert data["stac_extensions"] == [projection]
