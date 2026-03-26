"""Tests for temporal extent handling.

Per ADR-0035: Default to null (not now), prompt for datetime, flag incomplete.
"""

from __future__ import annotations

from datetime import datetime, timezone

import pytest

from portolan_cli.temporal import (
    FLEXIBLE_DATETIME,
    parse_flexible_datetime,
)


class TestParseFlexibleDatetime:
    """Tests for parse_flexible_datetime function."""

    def test_parses_iso_format(self) -> None:
        """Should parse ISO 8601 datetime."""
        result = parse_flexible_datetime("2024-01-15T10:30:00Z")
        assert result == datetime(2024, 1, 15, 10, 30, 0, tzinfo=timezone.utc)

    def test_parses_iso_without_z(self) -> None:
        """Should parse ISO 8601 without Z suffix."""
        result = parse_flexible_datetime("2024-01-15T10:30:00")
        assert result.year == 2024
        assert result.month == 1
        assert result.day == 15

    def test_parses_date_only(self) -> None:
        """Should parse date-only format."""
        result = parse_flexible_datetime("2024-01-15")
        assert result.year == 2024
        assert result.month == 1
        assert result.day == 15

    def test_parses_space_separated(self) -> None:
        """Should parse space-separated datetime."""
        result = parse_flexible_datetime("2024-01-15 10:30:00")
        assert result.year == 2024
        assert result.hour == 10

    def test_returns_none_for_empty_string(self) -> None:
        """Should return None for empty string."""
        result = parse_flexible_datetime("")
        assert result is None

    def test_returns_none_for_none(self) -> None:
        """Should return None for None input."""
        result = parse_flexible_datetime(None)
        assert result is None

    def test_raises_for_invalid_format(self) -> None:
        """Should raise ValueError for invalid format."""
        with pytest.raises(ValueError, match="Invalid datetime"):
            parse_flexible_datetime("not-a-date")


class TestFlexibleDatetimeType:
    """Tests for FLEXIBLE_DATETIME Click parameter type."""

    def test_converts_valid_datetime(self) -> None:
        """Should convert valid datetime string."""
        result = FLEXIBLE_DATETIME.convert("2024-06-15", None, None)
        assert result.year == 2024
        assert result.month == 6

    def test_passes_through_datetime_objects(self) -> None:
        """Should pass through datetime objects unchanged."""
        dt = datetime(2024, 1, 1, tzinfo=timezone.utc)
        result = FLEXIBLE_DATETIME.convert(dt, None, None)
        assert result == dt

    def test_returns_none_for_empty(self) -> None:
        """Should return None for empty string."""
        result = FLEXIBLE_DATETIME.convert("", None, None)
        assert result is None


class TestCreateItemDatetime:
    """Tests for create_item datetime handling per ADR-0035."""

    def test_create_item_with_explicit_datetime(self) -> None:
        """Should use provided datetime when specified."""
        from portolan_cli.stac import create_item

        dt = datetime(2024, 6, 15, 12, 0, 0, tzinfo=timezone.utc)
        item = create_item(
            item_id="test-item",
            bbox=[-180, -90, 180, 90],
            datetime=dt,
        )
        assert item.datetime == dt
        # Explicit datetime should NOT have provisional marker
        assert "portolan:datetime_provisional" not in item.properties

    def test_create_item_with_none_datetime_marks_provisional(self) -> None:
        """Should default to now BUT mark as provisional when datetime is None (ADR-0035)."""
        from portolan_cli.stac import create_item

        item = create_item(
            item_id="test-item",
            bbox=[-180, -90, 180, 90],
            datetime=None,
        )
        # Per ADR-0035: STAC requires datetime, so we use now() as placeholder
        # BUT mark it as provisional so portolan check can flag it
        assert item.datetime is not None  # Has a datetime (STAC-valid)
        assert item.properties.get("portolan:datetime_provisional") is True

    def test_create_item_explicit_datetime_clears_provisional(self) -> None:
        """Explicit datetime should NOT have provisional marker."""
        from portolan_cli.stac import create_item

        dt = datetime(2020, 1, 1, tzinfo=timezone.utc)
        item = create_item(
            item_id="test-item",
            bbox=[-180, -90, 180, 90],
            datetime=dt,
        )
        assert item.datetime == dt
        assert "portolan:datetime_provisional" not in item.properties
