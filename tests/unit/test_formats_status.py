"""Unit tests for CloudNativeStatus enum and FormatInfo dataclass.

These tests verify the foundational data structures for cloud-native format
classification per spec 002-cloud-native-warnings.
"""

from __future__ import annotations

import pytest

from portolan_cli.formats import CloudNativeStatus, FormatInfo


class TestCloudNativeStatus:
    """Tests for CloudNativeStatus enum."""

    @pytest.mark.unit
    def test_cloud_native_status_has_three_values(self) -> None:
        """CloudNativeStatus enum has exactly three values."""
        assert len(CloudNativeStatus) == 3

    @pytest.mark.unit
    def test_cloud_native_value(self) -> None:
        """CLOUD_NATIVE status exists with correct value."""
        assert CloudNativeStatus.CLOUD_NATIVE.value == "cloud_native"

    @pytest.mark.unit
    def test_convertible_value(self) -> None:
        """CONVERTIBLE status exists with correct value."""
        assert CloudNativeStatus.CONVERTIBLE.value == "convertible"

    @pytest.mark.unit
    def test_unsupported_value(self) -> None:
        """UNSUPPORTED status exists with correct value."""
        assert CloudNativeStatus.UNSUPPORTED.value == "unsupported"


class TestFormatInfo:
    """Tests for FormatInfo dataclass."""

    @pytest.mark.unit
    def test_format_info_creation_cloud_native(self) -> None:
        """FormatInfo can be created for cloud-native formats."""
        info = FormatInfo(
            status=CloudNativeStatus.CLOUD_NATIVE,
            display_name="GeoParquet",
            target_format=None,
            error_message=None,
        )
        assert info.status == CloudNativeStatus.CLOUD_NATIVE
        assert info.display_name == "GeoParquet"
        assert info.target_format is None
        assert info.error_message is None

    @pytest.mark.unit
    def test_format_info_creation_convertible(self) -> None:
        """FormatInfo can be created for convertible formats."""
        info = FormatInfo(
            status=CloudNativeStatus.CONVERTIBLE,
            display_name="SHP",
            target_format="GeoParquet",
            error_message=None,
        )
        assert info.status == CloudNativeStatus.CONVERTIBLE
        assert info.display_name == "SHP"
        assert info.target_format == "GeoParquet"
        assert info.error_message is None

    @pytest.mark.unit
    def test_format_info_creation_unsupported(self) -> None:
        """FormatInfo can be created for unsupported formats."""
        info = FormatInfo(
            status=CloudNativeStatus.UNSUPPORTED,
            display_name="NetCDF",
            target_format=None,
            error_message="NetCDF is not yet supported. Support coming soon.",
        )
        assert info.status == CloudNativeStatus.UNSUPPORTED
        assert info.display_name == "NetCDF"
        assert info.target_format is None
        assert info.error_message == "NetCDF is not yet supported. Support coming soon."

    @pytest.mark.unit
    def test_format_info_is_frozen(self) -> None:
        """FormatInfo is immutable (frozen dataclass)."""
        info = FormatInfo(
            status=CloudNativeStatus.CLOUD_NATIVE,
            display_name="GeoParquet",
            target_format=None,
            error_message=None,
        )
        with pytest.raises(AttributeError):
            info.display_name = "Changed"  # type: ignore[misc]

    @pytest.mark.unit
    def test_format_info_equality(self) -> None:
        """FormatInfo instances with same values are equal."""
        info1 = FormatInfo(
            status=CloudNativeStatus.CLOUD_NATIVE,
            display_name="GeoParquet",
            target_format=None,
            error_message=None,
        )
        info2 = FormatInfo(
            status=CloudNativeStatus.CLOUD_NATIVE,
            display_name="GeoParquet",
            target_format=None,
            error_message=None,
        )
        assert info1 == info2
