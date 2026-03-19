"""Unit tests for nested collection ID inference in the scan module.

These tests follow TDD: written FIRST, verified to FAIL, then implementation added.
Tests verify that ScannedFile.inferred_collection_id correctly derives collection IDs
from the directory structure relative to the scan root.

Test fixtures used:
- flat_collection/: Single-level directory → ID "flat_collection"
- nested/: Two-level nesting (census/2020/, census/2022/, imagery/2024/)
- three_level_nested/: Three-level nesting (GAUL_L2/by_country/AFG/)
- mixed_depths/: Mixed single and two-level in same catalog
- deep_nested/: 5+ level nesting to verify deep paths work

Collection ID inference rules:
- ID is the relative directory path from scan root to the file's parent directory
- Forward slashes as separators (POSIX-style, portable)
- Files at root level have empty string or None as collection ID
"""

from __future__ import annotations

from pathlib import Path

import pytest

# Fixture path helper
FIXTURES_DIR = Path(__file__).parent.parent / "fixtures" / "scan"


@pytest.fixture
def fixtures_dir() -> Path:
    """Return the path to scan test fixtures."""
    return FIXTURES_DIR


# =============================================================================
# Nested Collection ID Inference Tests
# =============================================================================


@pytest.mark.unit
@pytest.mark.xfail(reason="TDD: inferred_collection_id field not yet implemented")
class TestNestedCollectionIdInference:
    """Tests for ScannedFile.inferred_collection_id field.

    The inferred_collection_id should be derived from the directory structure,
    allowing nested catalogs to automatically group files into collections
    based on their location in the filesystem hierarchy.
    """

    def test_single_level_collection_id(self, fixtures_dir: Path) -> None:
        """Single-level directory yields simple collection ID.

        Fixture: flat_collection/
        Expected: Files in flat_collection/ have inferred_collection_id = "flat_collection"
        """
        from portolan_cli.scan import ScanOptions, scan_directory

        result = scan_directory(fixtures_dir / "flat_collection", ScanOptions())

        # All files should have the same collection ID
        assert len(result.ready) >= 1, "Expected at least one ready file"
        for scanned_file in result.ready:
            # TDD: This field doesn't exist yet - will fail
            assert hasattr(scanned_file, "inferred_collection_id"), (
                "ScannedFile should have inferred_collection_id field"
            )
            # For single-level, the collection ID is empty string (files are at root of scan)
            # When scanning flat_collection/, files are directly in that dir
            assert scanned_file.inferred_collection_id == "", (
                f"Expected empty collection ID for root-level file, "
                f"got '{scanned_file.inferred_collection_id}'"
            )

    def test_two_level_collection_id(self, fixtures_dir: Path) -> None:
        """Two-level nesting yields slash-separated collection ID.

        Fixture: nested/
        Structure: nested/census/2020/boundaries.geojson
                   nested/census/2022/boundaries.geojson
                   nested/imagery/2024/flood_depth.tif

        When scanning nested/, files in census/2020/ should have
        inferred_collection_id = "census/2020"
        """
        from portolan_cli.scan import ScanOptions, scan_directory

        result = scan_directory(fixtures_dir / "nested", ScanOptions())

        assert len(result.ready) >= 3, f"Expected 3 ready files, got {len(result.ready)}"

        # Build a map of relative path -> inferred collection ID
        collection_ids = {str(f.relative_path): f.inferred_collection_id for f in result.ready}

        # Verify two-level collection IDs
        assert collection_ids.get("census/2020/boundaries.geojson") == "census/2020", (
            "census/2020/boundaries.geojson should have collection ID 'census/2020'"
        )
        assert collection_ids.get("census/2022/boundaries.geojson") == "census/2022", (
            "census/2022/boundaries.geojson should have collection ID 'census/2022'"
        )
        assert collection_ids.get("imagery/2024/flood_depth.tif") == "imagery/2024", (
            "imagery/2024/flood_depth.tif should have collection ID 'imagery/2024'"
        )

    def test_three_level_collection_id(self, fixtures_dir: Path) -> None:
        """Three-level nesting yields full path as collection ID.

        Fixture: three_level_nested/
        Structure: three_level_nested/GAUL_L2/by_country/AFG/AFG.parquet
                   three_level_nested/GAUL_L2/by_country/ALB/ALB.parquet

        Files should have inferred_collection_id = "GAUL_L2/by_country/AFG" etc.
        """
        from portolan_cli.scan import ScanOptions, scan_directory

        result = scan_directory(fixtures_dir / "three_level_nested", ScanOptions())

        assert len(result.ready) >= 2, f"Expected 2 ready files, got {len(result.ready)}"

        collection_ids = {str(f.relative_path): f.inferred_collection_id for f in result.ready}

        assert collection_ids.get("GAUL_L2/by_country/AFG/AFG.parquet") == "GAUL_L2/by_country/AFG"
        assert collection_ids.get("GAUL_L2/by_country/ALB/ALB.parquet") == "GAUL_L2/by_country/ALB"

    def test_mixed_depths_same_catalog(self, fixtures_dir: Path) -> None:
        """Mixed nesting depths coexist in the same scan result.

        Fixture: mixed_depths/
        Structure: mixed_depths/shallow_collection/data.parquet (1-level)
                   mixed_depths/theme/nested_collection/data.parquet (2-level)

        Different depths should each get appropriate collection IDs.
        """
        from portolan_cli.scan import ScanOptions, scan_directory

        result = scan_directory(fixtures_dir / "mixed_depths", ScanOptions())

        assert len(result.ready) >= 2, f"Expected 2 ready files, got {len(result.ready)}"

        collection_ids = {str(f.relative_path): f.inferred_collection_id for f in result.ready}

        # Single-level nesting
        assert collection_ids.get("shallow_collection/data.parquet") == "shallow_collection", (
            "shallow_collection/data.parquet should have collection ID 'shallow_collection'"
        )

        # Two-level nesting
        assert (
            collection_ids.get("theme/nested_collection/data.parquet") == "theme/nested_collection"
        ), (
            "theme/nested_collection/data.parquet should have "
            "collection ID 'theme/nested_collection'"
        )

    def test_deep_nesting_five_plus_levels(self, fixtures_dir: Path) -> None:
        """Deep nesting (5+ levels) correctly handles long paths.

        Fixture: deep_nested/
        Structure: deep_nested/level1/level2/level3/level4/level5/data.parquet
                   deep_nested/level1/level2/shallow_collection/data.parquet

        Tests that arbitrarily deep nesting doesn't break collection ID inference.
        """
        from portolan_cli.scan import ScanOptions, scan_directory

        result = scan_directory(fixtures_dir / "deep_nested", ScanOptions())

        assert len(result.ready) >= 2, f"Expected 2 ready files, got {len(result.ready)}"

        collection_ids = {str(f.relative_path): f.inferred_collection_id for f in result.ready}

        # 5-level deep path
        deep_path = "level1/level2/level3/level4/level5/data.parquet"
        expected_deep_id = "level1/level2/level3/level4/level5"
        assert collection_ids.get(deep_path) == expected_deep_id, (
            f"Deep nested file should have collection ID '{expected_deep_id}', "
            f"got '{collection_ids.get(deep_path)}'"
        )

        # Shallower path in same fixture
        shallow_path = "level1/level2/shallow_collection/data.parquet"
        expected_shallow_id = "level1/level2/shallow_collection"
        assert collection_ids.get(shallow_path) == expected_shallow_id, (
            f"Shallow nested file should have collection ID '{expected_shallow_id}', "
            f"got '{collection_ids.get(shallow_path)}'"
        )


@pytest.mark.unit
@pytest.mark.xfail(reason="TDD: inferred_collection_id field not yet implemented")
class TestCollectionIdEdgeCases:
    """Edge case tests for collection ID inference."""

    def test_collection_id_uses_forward_slashes(self, fixtures_dir: Path) -> None:
        """Collection IDs use forward slashes regardless of OS.

        This ensures portable, consistent IDs across Windows/Unix.
        """
        from portolan_cli.scan import ScanOptions, scan_directory

        result = scan_directory(fixtures_dir / "nested", ScanOptions())

        for scanned_file in result.ready:
            if scanned_file.inferred_collection_id:
                assert "\\" not in scanned_file.inferred_collection_id, (
                    f"Collection ID should not contain backslashes: "
                    f"'{scanned_file.inferred_collection_id}'"
                )

    def test_collection_id_no_trailing_slash(self, fixtures_dir: Path) -> None:
        """Collection IDs do not have trailing slashes."""
        from portolan_cli.scan import ScanOptions, scan_directory

        result = scan_directory(fixtures_dir / "nested", ScanOptions())

        for scanned_file in result.ready:
            if scanned_file.inferred_collection_id:
                assert not scanned_file.inferred_collection_id.endswith("/"), (
                    f"Collection ID should not end with slash: "
                    f"'{scanned_file.inferred_collection_id}'"
                )

    def test_collection_id_no_leading_slash(self, fixtures_dir: Path) -> None:
        """Collection IDs do not have leading slashes (they're relative)."""
        from portolan_cli.scan import ScanOptions, scan_directory

        result = scan_directory(fixtures_dir / "nested", ScanOptions())

        for scanned_file in result.ready:
            if scanned_file.inferred_collection_id:
                assert not scanned_file.inferred_collection_id.startswith("/"), (
                    f"Collection ID should not start with slash: "
                    f"'{scanned_file.inferred_collection_id}'"
                )


@pytest.mark.unit
@pytest.mark.xfail(reason="TDD: inferred_collection_id field not yet implemented")
class TestInferredCollectionIdField:
    """Tests for the ScannedFile.inferred_collection_id field itself."""

    def test_scanned_file_has_inferred_collection_id_attribute(self, tmp_path: Path) -> None:
        """ScannedFile dataclass has inferred_collection_id field."""
        from portolan_cli.scan import FormatType, ScannedFile

        # TDD: This will fail until the field is added to ScannedFile
        sf = ScannedFile(
            path=tmp_path / "collection" / "data.parquet",
            relative_path="collection/data.parquet",
            extension=".parquet",
            format_type=FormatType.VECTOR,
            size_bytes=1024,
            inferred_collection_id="collection",  # NEW field
        )
        assert sf.inferred_collection_id == "collection"

    def test_inferred_collection_id_defaults_to_empty_string(self, tmp_path: Path) -> None:
        """ScannedFile.inferred_collection_id defaults to empty string for root files."""
        from portolan_cli.scan import FormatType, ScannedFile

        # TDD: The default value should be empty string
        sf = ScannedFile(
            path=tmp_path / "data.parquet",
            relative_path="data.parquet",
            extension=".parquet",
            format_type=FormatType.VECTOR,
            size_bytes=1024,
            # inferred_collection_id not provided - should default to ""
        )
        assert sf.inferred_collection_id == ""

    def test_inferred_collection_id_is_derived_from_relative_path(self, tmp_path: Path) -> None:
        """Collection ID is the directory portion of relative_path."""
        from portolan_cli.scan import FormatType, ScannedFile

        sf = ScannedFile(
            path=tmp_path / "theme" / "subtheme" / "data.parquet",
            relative_path="theme/subtheme/data.parquet",
            extension=".parquet",
            format_type=FormatType.VECTOR,
            size_bytes=1024,
            inferred_collection_id="theme/subtheme",
        )
        # The collection ID should be the path minus the filename
        assert sf.inferred_collection_id == "theme/subtheme"
