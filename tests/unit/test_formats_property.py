"""Invariant tests for format detection.

Verifies invariants across all known extensions using parameterized tests
and hypothesis property-based testing.
"""

from __future__ import annotations

import tempfile
from pathlib import Path

import pytest
from hypothesis import given, settings
from hypothesis import strategies as st

from portolan_cli.formats import (
    CLOUD_NATIVE_EXTENSIONS,
    CONVERTIBLE_RASTER_EXTENSIONS,
    CONVERTIBLE_VECTOR_EXTENSIONS,
    UNSUPPORTED_EXTENSIONS,
    VECTOR_EXTENSIONS,
    CloudNativeStatus,
    FormatInfo,
    FormatType,
    detect_format,
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


# =============================================================================
# Hypothesis Property-Based Tests
# =============================================================================


class TestHypothesisPMTilesConsistency:
    """Property-based tests for PMTiles format consistency.

    Regression tests for issue #198: PMTiles must be treated consistently
    across all format detection code paths.
    """

    @pytest.mark.unit
    @given(
        filename_base=st.text(
            alphabet=st.sampled_from("abcdefghijklmnopqrstuvwxyz0123456789_-"),
            min_size=1,
            max_size=20,
        )
    )
    @settings(max_examples=50)
    def test_pmtiles_always_detected_as_vector(self, filename_base: str) -> None:
        """PMTiles files with any valid filename are detected as VECTOR.

        Property: For any valid filename base, {base}.pmtiles -> FormatType.VECTOR
        """
        with tempfile.TemporaryDirectory() as tmp_dir:
            test_file = Path(tmp_dir) / f"{filename_base}.pmtiles"
            test_file.write_bytes(b"\x00" * 16)

            result = detect_format(test_file)

            assert result == FormatType.VECTOR, f"Failed for filename: {filename_base}.pmtiles"

    @pytest.mark.unit
    @given(
        filename_base=st.text(
            alphabet=st.sampled_from("abcdefghijklmnopqrstuvwxyz0123456789_-"),
            min_size=1,
            max_size=20,
        )
    )
    @settings(max_examples=50)
    def test_pmtiles_always_cloud_native(self, filename_base: str) -> None:
        """PMTiles files are always classified as CLOUD_NATIVE.

        Property: For any valid filename base, {base}.pmtiles -> CLOUD_NATIVE
        """
        with tempfile.TemporaryDirectory() as tmp_dir:
            test_file = Path(tmp_dir) / f"{filename_base}.pmtiles"
            test_file.write_bytes(b"\x00" * 16)

            result = get_cloud_native_status(test_file)

            assert result.status == CloudNativeStatus.CLOUD_NATIVE
            assert result.display_name == "PMTiles"
            assert result.target_format is None
            assert result.error_message is None


class TestHypothesisCloudNativeVectorConsistency:
    """Property-based tests for cloud-native vector format consistency.

    Ensures PMTiles and FlatGeobuf (cloud-native vector formats) behave
    identically across all detection code paths.
    """

    @pytest.mark.unit
    @given(ext=st.sampled_from([".pmtiles", ".fgb"]))
    @settings(max_examples=20)
    def test_cloud_native_vectors_in_both_extension_sets(self, ext: str) -> None:
        """Cloud-native vector formats are in both CLOUD_NATIVE and VECTOR extensions.

        Property: .pmtiles and .fgb are in both CLOUD_NATIVE_EXTENSIONS and VECTOR_EXTENSIONS
        This ensures detect_format() returns VECTOR, then convert_vector() skips conversion.
        """
        assert ext in CLOUD_NATIVE_EXTENSIONS, f"{ext} not in CLOUD_NATIVE_EXTENSIONS"
        assert ext in VECTOR_EXTENSIONS, f"{ext} not in VECTOR_EXTENSIONS"

    @pytest.mark.unit
    @given(ext=st.sampled_from([".pmtiles", ".fgb"]))
    @settings(max_examples=20)
    def test_cloud_native_vectors_detect_as_vector(self, ext: str) -> None:
        """Cloud-native vector formats are detected as VECTOR type.

        Property: detect_format(path.{ext}) == FormatType.VECTOR
        """
        with tempfile.TemporaryDirectory() as tmp_dir:
            test_file = Path(tmp_dir) / f"test{ext}"
            test_file.write_bytes(b"\x00" * 16)

            result = detect_format(test_file)

            assert result == FormatType.VECTOR

    @pytest.mark.unit
    @given(ext=st.sampled_from([".pmtiles", ".fgb"]))
    @settings(max_examples=20)
    def test_cloud_native_vectors_have_consistent_status(self, ext: str) -> None:
        """Cloud-native vector formats have consistent FormatInfo structure.

        Property: Both .pmtiles and .fgb produce CLOUD_NATIVE status with no conversion target.
        """
        with tempfile.TemporaryDirectory() as tmp_dir:
            test_file = Path(tmp_dir) / f"test{ext}"
            test_file.write_bytes(b"\x00" * 16)

            result = get_cloud_native_status(test_file)

            assert result.status == CloudNativeStatus.CLOUD_NATIVE
            assert result.target_format is None
            assert result.error_message is None
