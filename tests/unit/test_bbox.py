"""Unit tests for bbox validation utilities."""

from __future__ import annotations

from portolan_cli.bbox import (
    BboxValidationResult,
    compute_bbox_union,
    filter_valid_bboxes,
    is_antimeridian_crossing,
    is_valid_bbox,
    normalize_antimeridian_bbox,
    to_2d_bbox,
)


class TestTo2dBbox:
    """Tests for to_2d_bbox: reducing STAC 3D bboxes to 2D (issue #592)."""

    def test_2d_bbox_returned_unchanged(self) -> None:
        """A 4-element bbox is already 2D and returned as-is."""
        assert to_2d_bbox([-74.0, 40.0, -73.0, 41.0]) == [-74.0, 40.0, -73.0, 41.0]

    def test_3d_bbox_keeps_indices_0_1_3_4(self) -> None:
        """A 6-element bbox [w, s, min_z, e, n, max_z] reduces to [w, s, e, n]."""
        # min_z=100, max_z=500 must be dropped; a naive bbox[:4] would keep
        # min_z (index 2) as east and drop north (index 4).
        assert to_2d_bbox([-74.0, 40.0, 100.0, -73.0, 41.0, 500.0]) == [
            -74.0,
            40.0,
            -73.0,
            41.0,
        ]

    def test_3d_reduction_differs_from_naive_slice(self) -> None:
        """The correct reduction must not equal the buggy bbox[:4] slice."""
        bbox = [-74.0, 40.0, 100.0, -73.0, 41.0, 500.0]
        assert to_2d_bbox(bbox) != bbox[:4]

    def test_returns_new_list(self) -> None:
        """The result is a fresh list, not an alias of the input."""
        src = [-74.0, 40.0, -73.0, 41.0]
        assert to_2d_bbox(src) is not src


class TestIsValidBbox:
    """Tests for is_valid_bbox function."""

    def test_valid_bbox_returns_true(self) -> None:
        """Standard WGS84 bbox should be valid."""
        assert is_valid_bbox([-74.0, 40.0, -73.0, 41.0]) is True

    def test_global_bbox_valid(self) -> None:
        """Global extent bbox should be valid."""
        assert is_valid_bbox([-180.0, -90.0, 180.0, 90.0]) is True

    def test_inf_west_invalid(self) -> None:
        """Infinity in west coordinate should be invalid."""
        assert is_valid_bbox([float("inf"), 40.0, -73.0, 41.0]) is False
        assert is_valid_bbox([float("-inf"), 40.0, -73.0, 41.0]) is False

    def test_inf_south_invalid(self) -> None:
        """Infinity in south coordinate should be invalid."""
        assert is_valid_bbox([-74.0, float("inf"), -73.0, 41.0]) is False

    def test_inf_east_invalid(self) -> None:
        """Infinity in east coordinate should be invalid."""
        assert is_valid_bbox([-74.0, 40.0, float("inf"), 41.0]) is False

    def test_inf_north_invalid(self) -> None:
        """Infinity in north coordinate should be invalid."""
        assert is_valid_bbox([-74.0, 40.0, -73.0, float("inf")]) is False

    def test_nan_invalid(self) -> None:
        """NaN in any coordinate should be invalid."""
        assert is_valid_bbox([float("nan"), 40.0, -73.0, 41.0]) is False
        assert is_valid_bbox([-74.0, float("nan"), -73.0, 41.0]) is False
        assert is_valid_bbox([-74.0, 40.0, float("nan"), 41.0]) is False
        assert is_valid_bbox([-74.0, 40.0, -73.0, float("nan")]) is False

    def test_sentinel_inf_value_invalid(self) -> None:
        """The specific sentinel value from IGN Argentina should be invalid."""
        # This is the actual value that caused issue #516
        sentinel = -1.79e308
        assert is_valid_bbox([sentinel, sentinel, 180.0, 66.55]) is False

    def test_longitude_out_of_range_invalid(self) -> None:
        """Longitude outside [-180, 180] should be invalid."""
        assert is_valid_bbox([-181.0, 40.0, -73.0, 41.0]) is False
        assert is_valid_bbox([-74.0, 40.0, 181.0, 41.0]) is False

    def test_latitude_out_of_range_invalid(self) -> None:
        """Latitude outside [-90, 90] should be invalid."""
        assert is_valid_bbox([-74.0, -91.0, -73.0, 41.0]) is False
        assert is_valid_bbox([-74.0, 40.0, -73.0, 91.0]) is False

    def test_south_greater_than_north_invalid(self) -> None:
        """South > north should be invalid."""
        assert is_valid_bbox([-74.0, 50.0, -73.0, 40.0]) is False

    def test_antimeridian_crossing_valid(self) -> None:
        """Antimeridian crossing bbox (west > east) should be valid per RFC 7946."""
        # Fiji-style bbox crossing the antimeridian
        assert is_valid_bbox([177.0, -20.0, -175.0, -15.0]) is True

    def test_wrong_length_invalid(self) -> None:
        """Bbox with wrong number of elements should be invalid."""
        assert is_valid_bbox([-74.0, 40.0, -73.0]) is False  # 3 elements
        assert is_valid_bbox([-74.0, 40.0, -73.0, 41.0, 0.0]) is False  # 5 elements

    def test_6d_bbox_valid(self) -> None:
        """6-element 3D bbox [w, s, min_z, e, n, max_z] should be valid (#592)."""
        # west=-74, south=40, min_z=0, east=-73, north=41, max_z=100
        assert is_valid_bbox([-74.0, 40.0, 0.0, -73.0, 41.0, 100.0]) is True

    def test_6d_bbox_range_check_uses_correct_indices(self) -> None:
        """Range checks must read east/north from indices 3/4, not 2/3 (#592).

        Here east=190 (index 3) is out of longitude range. The pre-#592 slice
        read east from index 2 (0.0, in range) and would have wrongly passed.
        """
        # west=-74, south=40, min_z=0, east=190 (invalid), north=41, max_z=100
        assert is_valid_bbox([-74.0, 40.0, 0.0, 190.0, 41.0, 100.0]) is False

    def test_6d_bbox_south_gt_north_invalid(self) -> None:
        """A 3D bbox whose reduced south > north is invalid (#592).

        This is the exact shape of the old bug-encoding fixture
        [-74, 40, -73, 41, 0, 100]: read as STAC 3D it is
        [w=-74, s=40, min_z=-73, e=41, n=0, max_z=100], so north (0) < south (40).
        The pre-#592 slice read north from index 3 (41) and wrongly passed.
        """
        assert is_valid_bbox([-74.0, 40.0, -73.0, 41.0, 0.0, 100.0]) is False

    def test_6d_bbox_with_invalid_2d_components(self) -> None:
        """6D bbox with invalid 2D components should be invalid."""
        assert is_valid_bbox([float("inf"), 40.0, 0.0, -73.0, 41.0, 100.0]) is False


class TestIsAntimeridianCrossing:
    """Tests for antimeridian crossing detection."""

    def test_normal_bbox_not_crossing(self) -> None:
        """Standard bbox should not be detected as crossing."""
        assert is_antimeridian_crossing([-74.0, 40.0, -73.0, 41.0]) is False

    def test_fiji_bbox_crossing(self) -> None:
        """Fiji-style bbox should be detected as crossing."""
        assert is_antimeridian_crossing([177.0, -20.0, -175.0, -15.0]) is True

    def test_russia_bbox_crossing(self) -> None:
        """Russia far-east bbox crossing antimeridian."""
        assert is_antimeridian_crossing([160.0, 50.0, -170.0, 70.0]) is True

    def test_exactly_at_antimeridian_not_crossing(self) -> None:
        """Bbox ending exactly at 180 should not be crossing."""
        assert is_antimeridian_crossing([170.0, -20.0, 180.0, -15.0]) is False

    def test_exactly_at_negative_antimeridian_not_crossing(self) -> None:
        """Bbox starting exactly at -180 should not be crossing."""
        assert is_antimeridian_crossing([-180.0, -20.0, -170.0, -15.0]) is False


class TestNormalizeAntimeridianBbox:
    """Tests for splitting antimeridian-crossing bboxes."""

    def test_normal_bbox_returns_single(self) -> None:
        """Non-crossing bbox should return single bbox in list."""
        result = normalize_antimeridian_bbox([-74.0, 40.0, -73.0, 41.0])
        assert result == [[-74.0, 40.0, -73.0, 41.0]]

    def test_crossing_bbox_returns_two(self) -> None:
        """Crossing bbox should be split into two bboxes."""
        result = normalize_antimeridian_bbox([177.0, -20.0, -175.0, -15.0])
        assert len(result) == 2
        # Western part: from west to 180
        assert result[0] == [177.0, -20.0, 180.0, -15.0]
        # Eastern part: from -180 to east
        assert result[1] == [-180.0, -20.0, -175.0, -15.0]

    def test_russia_crossing_split(self) -> None:
        """Russia crossing should split correctly."""
        result = normalize_antimeridian_bbox([160.0, 50.0, -170.0, 70.0])
        assert len(result) == 2
        assert result[0] == [160.0, 50.0, 180.0, 70.0]
        assert result[1] == [-180.0, 50.0, -170.0, 70.0]


class TestFilterValidBboxes:
    """Tests for filtering bbox lists."""

    def test_all_valid_returns_all(self) -> None:
        """All valid bboxes should be returned."""
        bboxes = [
            [-74.0, 40.0, -73.0, 41.0],
            [-122.0, 37.0, -121.0, 38.0],
        ]
        result = filter_valid_bboxes(bboxes)
        assert result.valid == bboxes
        assert result.invalid == []

    def test_filters_out_inf(self) -> None:
        """Should filter out bboxes with inf."""
        bboxes = [
            [-74.0, 40.0, -73.0, 41.0],
            [float("-inf"), float("-inf"), 180.0, 66.55],  # IGN Argentina poison
        ]
        result = filter_valid_bboxes(bboxes)
        assert len(result.valid) == 1
        assert result.valid[0] == [-74.0, 40.0, -73.0, 41.0]
        assert len(result.invalid) == 1

    def test_filters_out_nan(self) -> None:
        """Should filter out bboxes with NaN."""
        bboxes = [
            [-74.0, 40.0, -73.0, 41.0],
            [float("nan"), 40.0, -73.0, 41.0],
        ]
        result = filter_valid_bboxes(bboxes)
        assert len(result.valid) == 1
        assert len(result.invalid) == 1

    def test_empty_list_returns_empty(self) -> None:
        """Empty list should return empty results."""
        result = filter_valid_bboxes([])
        assert result.valid == []
        assert result.invalid == []

    def test_all_invalid_returns_empty_valid(self) -> None:
        """All invalid bboxes should return empty valid list."""
        bboxes = [
            [float("inf"), 40.0, -73.0, 41.0],
            [float("nan"), 40.0, -73.0, 41.0],
        ]
        result = filter_valid_bboxes(bboxes)
        assert result.valid == []
        assert len(result.invalid) == 2

    def test_preserves_antimeridian_crossing(self) -> None:
        """Antimeridian crossing bboxes should be preserved as valid."""
        bboxes = [
            [177.0, -20.0, -175.0, -15.0],  # Fiji
        ]
        result = filter_valid_bboxes(bboxes)
        assert len(result.valid) == 1

    def test_invalid_reasons_captured(self) -> None:
        """Invalid bboxes should have reasons captured."""
        bboxes = [
            [float("inf"), 40.0, -73.0, 41.0],
        ]
        result = filter_valid_bboxes(bboxes)
        assert len(result.invalid) == 1
        bbox, reason = result.invalid[0]
        assert "inf" in reason.lower() or "finite" in reason.lower()


class TestComputeBboxUnion:
    """Tests for computing bbox union with validation."""

    def test_single_bbox_returns_same(self) -> None:
        """Single bbox should return itself."""
        result = compute_bbox_union([[-74.0, 40.0, -73.0, 41.0]])
        assert result.bbox == [-74.0, 40.0, -73.0, 41.0]
        assert result.is_multi_bbox is False

    def test_two_bboxes_union(self) -> None:
        """Two non-overlapping bboxes should union correctly."""
        bboxes = [
            [-74.0, 40.0, -73.0, 41.0],  # NYC area
            [-122.5, 37.5, -122.0, 38.0],  # SF area
        ]
        result = compute_bbox_union(bboxes)
        assert result.bbox == [-122.5, 37.5, -73.0, 41.0]

    def test_filters_invalid_before_union(self) -> None:
        """Invalid bboxes should be filtered before computing union."""
        bboxes = [
            [-74.0, 40.0, -73.0, 41.0],
            [float("-inf"), float("-inf"), 180.0, 66.55],  # Poison
            [-122.5, 37.5, -122.0, 38.0],
        ]
        result = compute_bbox_union(bboxes)
        # Should only union the two valid bboxes
        assert result.bbox == [-122.5, 37.5, -73.0, 41.0]
        assert len(result.skipped) == 1

    def test_all_invalid_returns_none(self) -> None:
        """All invalid bboxes should return None."""
        bboxes = [
            [float("inf"), 40.0, -73.0, 41.0],
            [float("nan"), 40.0, -73.0, 41.0],
        ]
        result = compute_bbox_union(bboxes)
        assert result.bbox is None
        assert len(result.skipped) == 2

    def test_empty_list_returns_none(self) -> None:
        """Empty list should return None."""
        result = compute_bbox_union([])
        assert result.bbox is None

    def test_3d_bboxes_union_uses_2d_extent(self) -> None:
        """6-element bboxes must union on [w, s, e, n], not the min_z slice (#592).

        With the pre-fix bbox[2]=east assumption, both bboxes' east collapses to
        their min_z (0.0) and north to their real east, producing a bogus
        [-122.5, 37.5, 0.0, -73.0] envelope instead of the real 2D union.
        """
        bboxes = [
            [-74.0, 40.0, 0.0, -73.0, 41.0, 100.0],  # NYC, elevation 0..100
            [-122.5, 37.5, 0.0, -122.0, 38.0, 100.0],  # SF, elevation 0..100
        ]
        result = compute_bbox_union(bboxes)
        assert result.bbox == [-122.5, 37.5, -73.0, 41.0]

    def test_antimeridian_crossing_produces_multi_bbox(self) -> None:
        """Union with antimeridian-crossing bbox should produce multi-bbox."""
        bboxes = [
            [-74.0, 40.0, -73.0, 41.0],  # NYC
            [177.0, -20.0, -175.0, -15.0],  # Fiji (crossing)
        ]
        result = compute_bbox_union(bboxes)
        assert result.is_multi_bbox is True
        assert result.bboxes is not None
        assert len(result.bboxes) >= 2

    def test_multiple_crossing_bboxes(self) -> None:
        """Multiple crossing bboxes should all be represented."""
        bboxes = [
            [177.0, -20.0, -175.0, -15.0],  # Fiji
            [160.0, 50.0, -170.0, 70.0],  # Russia far east
        ]
        result = compute_bbox_union(bboxes)
        assert result.is_multi_bbox is True

    def test_union_with_mixed_normal_and_crossing(self) -> None:
        """Mix of normal and crossing bboxes should handle correctly."""
        bboxes = [
            [-74.0, 40.0, -73.0, 41.0],  # NYC (normal)
            [150.0, 30.0, 160.0, 40.0],  # Japan area (normal)
            [177.0, -20.0, -175.0, -15.0],  # Fiji (crossing)
        ]
        result = compute_bbox_union(bboxes)
        # Should produce multi-bbox representation
        assert result.is_multi_bbox is True
        # The crossing bbox should be split
        assert result.bboxes is not None


class TestBboxValidationResult:
    """Tests for BboxValidationResult dataclass."""

    def test_has_valid_true_when_valid_present(self) -> None:
        """has_valid should be True when valid bboxes exist."""
        result = BboxValidationResult(
            valid=[[-74.0, 40.0, -73.0, 41.0]],
            invalid=[],
        )
        assert result.has_valid is True

    def test_has_valid_false_when_empty(self) -> None:
        """has_valid should be False when no valid bboxes."""
        result = BboxValidationResult(valid=[], invalid=[])
        assert result.has_valid is False
