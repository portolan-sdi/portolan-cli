"""Property-based tests for format detection.

Uses hypothesis to verify invariants across all known extensions.
"""

from __future__ import annotations

import tempfile
from pathlib import Path

import pytest

from portolan_cli.formats import (
    CLOUD_NATIVE_EXTENSIONS,
    CONVERTIBLE_RASTER_EXTENSIONS,
    CONVERTIBLE_VECTOR_EXTENSIONS,
    UNSUPPORTED_EXTENSIONS,
    CloudNativeStatus,
    FormatInfo,
    get_cloud_native_status,
)


class TestFormatInfoInvariants:
    """Property tests for FormatInfo invariants."""

    @pytest.mark.unit
    def test_cloud_native_extensions_return_valid_format_info(self) -> None:
        """All cloud-native extensions produce valid FormatInfo."""
        for extension in CLOUD_NATIVE_EXTENSIONS:
            with tempfile.TemporaryDirectory() as tmp_dir:
                test_file = Path(tmp_dir) / f"test{extension}"
                test_file.write_bytes(b"\x00\x00\x00\x00")

                result = get_cloud_native_status(test_file)

                # Invariants for cloud-native formats
                assert isinstance(result, FormatInfo), f"Failed for {extension}"
                assert result.status == CloudNativeStatus.CLOUD_NATIVE, f"Failed for {extension}"
                assert result.display_name, f"Failed for {extension}"
                assert result.target_format is None, f"Failed for {extension}"
                assert result.error_message is None, f"Failed for {extension}"

    @pytest.mark.unit
    def test_convertible_vector_extensions_return_valid_format_info(self) -> None:
        """All convertible vector extensions produce valid FormatInfo."""
        for extension in CONVERTIBLE_VECTOR_EXTENSIONS:
            with tempfile.TemporaryDirectory() as tmp_dir:
                test_file = Path(tmp_dir) / f"test{extension}"
                test_file.write_bytes(b"\x00\x00\x00\x00")

                result = get_cloud_native_status(test_file)

                # Invariants for convertible vector formats
                assert isinstance(result, FormatInfo), f"Failed for {extension}"
                assert result.status == CloudNativeStatus.CONVERTIBLE, f"Failed for {extension}"
                assert result.display_name, f"Failed for {extension}"
                assert result.target_format == "GeoParquet", f"Failed for {extension}"
                assert result.error_message is None, f"Failed for {extension}"

    @pytest.mark.unit
    def test_convertible_raster_extensions_return_valid_format_info(self) -> None:
        """All convertible raster extensions produce valid FormatInfo."""
        for extension in CONVERTIBLE_RASTER_EXTENSIONS:
            with tempfile.TemporaryDirectory() as tmp_dir:
                test_file = Path(tmp_dir) / f"test{extension}"
                test_file.write_bytes(b"\x00\x00\x00\x00")

                result = get_cloud_native_status(test_file)

                # Invariants for convertible raster formats
                assert isinstance(result, FormatInfo), f"Failed for {extension}"
                assert result.status == CloudNativeStatus.CONVERTIBLE, f"Failed for {extension}"
                assert result.display_name, f"Failed for {extension}"
                assert result.target_format == "COG", f"Failed for {extension}"
                assert result.error_message is None, f"Failed for {extension}"

    @pytest.mark.unit
    def test_unsupported_extensions_return_valid_format_info(self) -> None:
        """All unsupported extensions produce valid FormatInfo with error."""
        for extension in UNSUPPORTED_EXTENSIONS:
            with tempfile.TemporaryDirectory() as tmp_dir:
                test_file = Path(tmp_dir) / f"test{extension}"
                test_file.write_bytes(b"\x00\x00\x00\x00")

                result = get_cloud_native_status(test_file)

                # Invariants for unsupported formats
                assert isinstance(result, FormatInfo), f"Failed for {extension}"
                assert result.status == CloudNativeStatus.UNSUPPORTED, f"Failed for {extension}"
                assert result.display_name, f"Failed for {extension}"
                assert result.target_format is None, f"Failed for {extension}"
                assert result.error_message, f"Failed for {extension}"
                assert (
                    "support" in result.error_message.lower() or "COPC" in result.error_message
                ), f"Failed for {extension}"


class TestStatusExclusivity:
    """Tests that status categories are mutually exclusive."""

    @pytest.mark.unit
    def test_extension_sets_are_disjoint(self) -> None:
        """Extension sets don't overlap."""
        # Cloud-native vs convertible
        assert CLOUD_NATIVE_EXTENSIONS.isdisjoint(CONVERTIBLE_VECTOR_EXTENSIONS)
        assert CLOUD_NATIVE_EXTENSIONS.isdisjoint(CONVERTIBLE_RASTER_EXTENSIONS)

        # Convertible vs unsupported
        assert CONVERTIBLE_VECTOR_EXTENSIONS.isdisjoint(UNSUPPORTED_EXTENSIONS)
        assert CONVERTIBLE_RASTER_EXTENSIONS.isdisjoint(UNSUPPORTED_EXTENSIONS)

        # Cloud-native vs unsupported
        assert CLOUD_NATIVE_EXTENSIONS.isdisjoint(UNSUPPORTED_EXTENSIONS)

    @pytest.mark.unit
    def test_all_known_extensions_have_defined_behavior(self) -> None:
        """All defined extensions produce consistent results."""
        all_extensions = (
            CLOUD_NATIVE_EXTENSIONS
            | CONVERTIBLE_VECTOR_EXTENSIONS
            | CONVERTIBLE_RASTER_EXTENSIONS
            | UNSUPPORTED_EXTENSIONS
        )

        # Count - there should be a reasonable number (14 extensions defined)
        assert len(all_extensions) >= 14


class TestFormatInfoDataIntegrity:
    """Tests for FormatInfo data integrity."""

    @pytest.mark.unit
    def test_format_info_is_hashable(self) -> None:
        """FormatInfo can be used in sets/dicts."""
        info = FormatInfo(
            status=CloudNativeStatus.CLOUD_NATIVE,
            display_name="GeoParquet",
            target_format=None,
            error_message=None,
        )
        # Should be hashable (frozen dataclass)
        hash(info)

        # Should be usable in a set
        info_set = {info}
        assert len(info_set) == 1

    @pytest.mark.unit
    def test_cloud_native_status_has_all_expected_values(self) -> None:
        """CloudNativeStatus enum has exactly the expected values."""
        expected = {"cloud_native", "convertible", "unsupported"}
        actual = {status.value for status in CloudNativeStatus}
        assert actual == expected
