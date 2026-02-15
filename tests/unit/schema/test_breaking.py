"""Unit tests for breaking change detection."""

from __future__ import annotations

import pytest

from portolan_cli.models.schema import BandSchema, ColumnSchema, SchemaModel
from portolan_cli.schema.breaking import (
    BreakingChange,
    detect_breaking_changes,
    is_breaking,
)


class TestBreakingChangeDataclass:
    """Tests for BreakingChange dataclass."""

    @pytest.mark.unit
    def test_column_removed_str(self) -> None:
        """BreakingChange str for column removal."""
        change = BreakingChange(
            change_type="column_removed",
            element="population",
            old_value=None,
            new_value=None,
        )
        assert str(change) == "Column 'population' removed"

    @pytest.mark.unit
    def test_type_changed_str(self) -> None:
        """BreakingChange str for type change."""
        change = BreakingChange(
            change_type="type_changed",
            element="count",
            old_value="int64",
            new_value="string",
        )
        assert "count" in str(change)
        assert "int64" in str(change)
        assert "string" in str(change)

    @pytest.mark.unit
    def test_band_removed_str(self) -> None:
        """BreakingChange str for band removal."""
        change = BreakingChange(
            change_type="band_removed",
            element="band_3",
            old_value=None,
            new_value=None,
        )
        assert str(change) == "Band 'band_3' removed"


class TestDetectGeoparquetBreakingChanges:
    """Tests for breaking change detection in GeoParquet schemas."""

    def _make_schema(self, columns: list[ColumnSchema], crs: str | None = None) -> SchemaModel:
        """Helper to create a schema."""
        return SchemaModel(
            schema_version="1.0.0",
            format="geoparquet",
            columns=columns,
            crs=crs,
        )

    @pytest.mark.unit
    def test_no_changes_no_breaking(self) -> None:
        """Identical schemas have no breaking changes."""
        cols = [
            ColumnSchema(name="geometry", type="binary", nullable=False),
            ColumnSchema(name="name", type="string", nullable=True),
        ]
        old = self._make_schema(cols)
        new = self._make_schema(cols)

        changes = detect_breaking_changes(old, new)
        assert len(changes) == 0

    @pytest.mark.unit
    def test_column_removed_is_breaking(self) -> None:
        """Removing a column is a breaking change."""
        old = self._make_schema(
            [
                ColumnSchema(name="geometry", type="binary", nullable=False),
                ColumnSchema(name="name", type="string", nullable=True),
            ]
        )
        new = self._make_schema([ColumnSchema(name="geometry", type="binary", nullable=False)])

        changes = detect_breaking_changes(old, new)
        assert len(changes) == 1
        assert changes[0].change_type == "column_removed"
        assert changes[0].element == "name"

    @pytest.mark.unit
    def test_column_added_not_breaking(self) -> None:
        """Adding a column is NOT a breaking change."""
        old = self._make_schema([ColumnSchema(name="geometry", type="binary", nullable=False)])
        new = self._make_schema(
            [
                ColumnSchema(name="geometry", type="binary", nullable=False),
                ColumnSchema(name="name", type="string", nullable=True),
            ]
        )

        changes = detect_breaking_changes(old, new)
        assert len(changes) == 0

    @pytest.mark.unit
    def test_type_changed_is_breaking(self) -> None:
        """Changing column type is a breaking change."""
        old = self._make_schema([ColumnSchema(name="count", type="int64", nullable=True)])
        new = self._make_schema([ColumnSchema(name="count", type="string", nullable=True)])

        changes = detect_breaking_changes(old, new)
        assert len(changes) == 1
        assert changes[0].change_type == "type_changed"
        assert changes[0].old_value == "int64"
        assert changes[0].new_value == "string"

    @pytest.mark.unit
    def test_geometry_type_changed_is_breaking(self) -> None:
        """Changing geometry type is a breaking change."""
        old = self._make_schema(
            [
                ColumnSchema(
                    name="geometry",
                    type="binary",
                    nullable=False,
                    geometry_type="Polygon",
                )
            ]
        )
        new = self._make_schema(
            [
                ColumnSchema(
                    name="geometry",
                    type="binary",
                    nullable=False,
                    geometry_type="Point",
                )
            ]
        )

        changes = detect_breaking_changes(old, new)
        assert len(changes) == 1
        assert changes[0].change_type == "geometry_type_changed"

    @pytest.mark.unit
    def test_column_crs_changed_is_breaking(self) -> None:
        """Changing column CRS is a breaking change."""
        old = self._make_schema(
            [
                ColumnSchema(
                    name="geometry",
                    type="binary",
                    nullable=False,
                    crs="EPSG:4326",
                )
            ]
        )
        new = self._make_schema(
            [
                ColumnSchema(
                    name="geometry",
                    type="binary",
                    nullable=False,
                    crs="EPSG:32610",
                )
            ]
        )

        changes = detect_breaking_changes(old, new)
        assert len(changes) == 1
        assert changes[0].change_type == "column_crs_changed"

    @pytest.mark.unit
    def test_description_changed_not_breaking(self) -> None:
        """Changing description is NOT a breaking change."""
        old = self._make_schema(
            [ColumnSchema(name="name", type="string", nullable=True, description="Old desc")]
        )
        new = self._make_schema(
            [ColumnSchema(name="name", type="string", nullable=True, description="New desc")]
        )

        changes = detect_breaking_changes(old, new)
        assert len(changes) == 0

    @pytest.mark.unit
    def test_schema_crs_changed_is_breaking(self) -> None:
        """Changing schema-level CRS is a breaking change."""
        old = self._make_schema(
            [ColumnSchema(name="geometry", type="binary", nullable=False)],
            crs="EPSG:4326",
        )
        new = self._make_schema(
            [ColumnSchema(name="geometry", type="binary", nullable=False)],
            crs="EPSG:32610",
        )

        changes = detect_breaking_changes(old, new)
        assert len(changes) == 1
        assert changes[0].change_type == "crs_changed"


class TestDetectCogBreakingChanges:
    """Tests for breaking change detection in COG schemas."""

    def _make_schema(self, bands: list[BandSchema], crs: str | None = None) -> SchemaModel:
        """Helper to create a COG schema."""
        return SchemaModel(
            schema_version="1.0.0",
            format="cog",
            columns=bands,
            crs=crs,
        )

    @pytest.mark.unit
    def test_band_removed_is_breaking(self) -> None:
        """Removing a band is a breaking change."""
        old = self._make_schema(
            [
                BandSchema(name="band_1", data_type="uint8"),
                BandSchema(name="band_2", data_type="uint8"),
            ]
        )
        new = self._make_schema([BandSchema(name="band_1", data_type="uint8")])

        changes = detect_breaking_changes(old, new)
        assert len(changes) == 1
        assert changes[0].change_type == "band_removed"
        assert changes[0].element == "band_2"

    @pytest.mark.unit
    def test_data_type_changed_is_breaking(self) -> None:
        """Changing band data type is a breaking change."""
        old = self._make_schema([BandSchema(name="band_1", data_type="uint8")])
        new = self._make_schema([BandSchema(name="band_1", data_type="float32")])

        changes = detect_breaking_changes(old, new)
        assert len(changes) == 1
        assert changes[0].change_type == "data_type_changed"

    @pytest.mark.unit
    def test_nodata_changed_is_breaking(self) -> None:
        """Changing nodata value is a breaking change."""
        old = self._make_schema([BandSchema(name="band_1", data_type="uint8", nodata=0)])
        new = self._make_schema([BandSchema(name="band_1", data_type="uint8", nodata=255)])

        changes = detect_breaking_changes(old, new)
        assert len(changes) == 1
        assert changes[0].change_type == "nodata_changed"

    @pytest.mark.unit
    def test_band_description_changed_not_breaking(self) -> None:
        """Changing band description is NOT a breaking change."""
        old = self._make_schema([BandSchema(name="band_1", data_type="uint8", description="Old")])
        new = self._make_schema([BandSchema(name="band_1", data_type="uint8", description="New")])

        changes = detect_breaking_changes(old, new)
        assert len(changes) == 0


class TestFormatMismatch:
    """Tests for format mismatch detection."""

    @pytest.mark.unit
    def test_format_changed_is_breaking(self) -> None:
        """Changing format is a breaking change."""
        old = SchemaModel(
            schema_version="1.0.0",
            format="geoparquet",
            columns=[ColumnSchema(name="geometry", type="binary", nullable=False)],
        )
        new = SchemaModel(
            schema_version="1.0.0",
            format="cog",
            columns=[BandSchema(name="band_1", data_type="uint8")],
        )

        changes = detect_breaking_changes(old, new)
        assert len(changes) == 1
        assert changes[0].change_type == "format_changed"


class TestIsBreaking:
    """Tests for is_breaking helper function."""

    @pytest.mark.unit
    def test_returns_true_when_breaking(self) -> None:
        """is_breaking returns True when changes are breaking."""
        old = SchemaModel(
            schema_version="1.0.0",
            format="geoparquet",
            columns=[
                ColumnSchema(name="a", type="int64", nullable=False),
                ColumnSchema(name="b", type="string", nullable=True),
            ],
        )
        new = SchemaModel(
            schema_version="1.0.0",
            format="geoparquet",
            columns=[ColumnSchema(name="a", type="int64", nullable=False)],
        )

        assert is_breaking(old, new) is True

    @pytest.mark.unit
    def test_returns_false_when_not_breaking(self) -> None:
        """is_breaking returns False when changes are not breaking."""
        old = SchemaModel(
            schema_version="1.0.0",
            format="geoparquet",
            columns=[ColumnSchema(name="a", type="int64", nullable=False)],
        )
        new = SchemaModel(
            schema_version="1.0.0",
            format="geoparquet",
            columns=[
                ColumnSchema(name="a", type="int64", nullable=False),
                ColumnSchema(name="b", type="string", nullable=True),
            ],
        )

        assert is_breaking(old, new) is False
