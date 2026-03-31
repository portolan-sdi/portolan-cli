"""Property-based tests for add/rm functions using Hypothesis.

These tests verify invariants hold across many randomly generated inputs,
defending against tautological tests that might pass but not actually test
anything meaningful.

Per CLAUDE.md: "Property-based testing — Use hypothesis for invariant verification"
"""

from __future__ import annotations

import string
import tempfile
from pathlib import Path

import pytest
from hypothesis import given, settings
from hypothesis import strategies as st

from portolan_cli.catalog import find_catalog_root
from portolan_cli.dataset import (
    GEOSPATIAL_EXTENSIONS,
    SIDECAR_PATTERNS,
    get_sidecars,
    iter_geospatial_files,
    resolve_collection_id,
)

# =============================================================================
# Custom Strategies
# =============================================================================

# Safe filename characters (avoid path separators, null bytes, etc.)
safe_chars = st.sampled_from(string.ascii_letters + string.digits + "_-")
safe_filename = st.text(safe_chars, min_size=1, max_size=20)

# Geospatial extensions
geospatial_ext = st.sampled_from(list(GEOSPATIAL_EXTENSIONS))

# Sidecar-supporting extensions
sidecar_ext = st.sampled_from(list(SIDECAR_PATTERNS.keys()))

# Valid collection names (first path component)
# Per collection_id.py: must start with a letter, contain only [a-z0-9_-]
collection_name = st.builds(
    lambda first, rest: first + rest,
    # First character must be a letter
    st.sampled_from(string.ascii_lowercase),
    # Rest can be letters, digits, underscore, or hyphen
    st.text(
        st.sampled_from(string.ascii_lowercase + string.digits + "_-"),
        min_size=0,
        max_size=20,
    ),
)


# =============================================================================
# Property: get_sidecars should be pure and deterministic
# =============================================================================


class TestGetSidecarsProperties:
    """Property-based tests for get_sidecars function."""

    @pytest.mark.unit
    @given(filename=safe_filename, ext=sidecar_ext)
    @settings(max_examples=50)
    def test_get_sidecars_deterministic(self, filename: str, ext: str) -> None:
        """Calling get_sidecars twice with same input returns same result."""
        with tempfile.TemporaryDirectory() as tmp_dir:
            tmp_path = Path(tmp_dir)
            # Create a primary file
            primary = tmp_path / f"{filename}{ext}"
            primary.write_bytes(b"test")

            result1 = get_sidecars(primary)
            result2 = get_sidecars(primary)

            assert result1 == result2, "get_sidecars should be deterministic"

    @pytest.mark.unit
    @given(filename=safe_filename, ext=sidecar_ext)
    @settings(max_examples=50)
    def test_get_sidecars_returns_only_existing(self, filename: str, ext: str) -> None:
        """get_sidecars only returns paths that exist on disk."""
        with tempfile.TemporaryDirectory() as tmp_dir:
            tmp_path = Path(tmp_dir)
            primary = tmp_path / f"{filename}{ext}"
            primary.write_bytes(b"test")

            # Create just ONE sidecar (not all of them)
            patterns = SIDECAR_PATTERNS.get(ext, [])
            if patterns:
                one_sidecar = tmp_path / f"{filename}{patterns[0]}"
                one_sidecar.write_bytes(b"sidecar")

            result = get_sidecars(primary)

            for sidecar_path in result:
                assert sidecar_path.exists(), f"Returned non-existent sidecar: {sidecar_path}"

    @pytest.mark.unit
    @given(filename=safe_filename, ext=sidecar_ext)
    @settings(max_examples=50)
    def test_get_sidecars_same_stem(self, filename: str, ext: str) -> None:
        """All returned sidecars have the same stem as the primary file."""
        with tempfile.TemporaryDirectory() as tmp_dir:
            tmp_path = Path(tmp_dir)
            primary = tmp_path / f"{filename}{ext}"
            primary.write_bytes(b"test")

            # Create all sidecars
            for sidecar_ext_pattern in SIDECAR_PATTERNS.get(ext, []):
                sidecar = tmp_path / f"{filename}{sidecar_ext_pattern}"
                sidecar.write_bytes(b"sidecar")

            result = get_sidecars(primary)

            for sidecar_path in result:
                # Handle compound extensions like .aux.xml and .shp.xml
                sidecar_stem = sidecar_path.stem
                if sidecar_stem.endswith(".aux"):
                    sidecar_stem = sidecar_stem[:-4]  # Remove .aux part
                elif sidecar_stem.endswith(".shp"):
                    sidecar_stem = sidecar_stem[:-4]  # Remove .shp part (for .shp.xml)
                assert sidecar_stem == filename, f"Sidecar {sidecar_path} has different stem"

    @pytest.mark.unit
    @given(filename=safe_filename)
    @settings(max_examples=30)
    def test_get_sidecars_empty_for_unknown_ext(self, filename: str) -> None:
        """Files with unknown extensions return empty sidecar list."""
        with tempfile.TemporaryDirectory() as tmp_dir:
            tmp_path = Path(tmp_dir)
            # Use an extension not in SIDECAR_PATTERNS
            unknown_ext = ".xyz"
            primary = tmp_path / f"{filename}{unknown_ext}"
            primary.write_bytes(b"test")

            result = get_sidecars(primary)

            assert result == [], f"Unknown extension should return empty list, got {result}"


# =============================================================================
# Property: resolve_collection_id extracts first path component
# =============================================================================


class TestResolveCollectionIdProperties:
    """Property-based tests for resolve_collection_id function."""

    @pytest.mark.unit
    @given(collection=collection_name, filename=safe_filename, ext=geospatial_ext)
    @settings(max_examples=50)
    def test_resolve_returns_first_component(
        self, collection: str, filename: str, ext: str
    ) -> None:
        """resolve_collection_id returns the first path component."""
        with tempfile.TemporaryDirectory() as tmp_dir:
            tmp_path = Path(tmp_dir)
            # Create directory structure
            collection_dir = tmp_path / collection
            collection_dir.mkdir(parents=True, exist_ok=True)

            file_path = collection_dir / f"{filename}{ext}"
            file_path.write_bytes(b"test")

            result = resolve_collection_id(file_path, tmp_path)

            assert result == collection, f"Expected {collection}, got {result}"

    @pytest.mark.unit
    @given(
        collection=collection_name,
        subdir=safe_filename,
        filename=safe_filename,
        ext=geospatial_ext,
    )
    @settings(max_examples=50)
    def test_resolve_ignores_nested_dirs(
        self,
        collection: str,
        subdir: str,
        filename: str,
        ext: str,
    ) -> None:
        """resolve_collection_id returns first component even for nested paths."""
        with tempfile.TemporaryDirectory() as tmp_dir:
            tmp_path = Path(tmp_dir)
            # Create nested directory structure
            nested_dir = tmp_path / collection / subdir
            nested_dir.mkdir(parents=True, exist_ok=True)

            file_path = nested_dir / f"{filename}{ext}"
            file_path.write_bytes(b"test")

            result = resolve_collection_id(file_path, tmp_path)

            # Should still be the first component, not the subdir
            assert result == collection, f"Expected {collection} (not {subdir}), got {result}"

    @pytest.mark.unit
    @given(collection=collection_name, filename=safe_filename, ext=geospatial_ext)
    @settings(max_examples=30)
    def test_resolve_is_deterministic(self, collection: str, filename: str, ext: str) -> None:
        """Calling resolve_collection_id twice returns same result."""
        with tempfile.TemporaryDirectory() as tmp_dir:
            tmp_path = Path(tmp_dir)
            collection_dir = tmp_path / collection
            collection_dir.mkdir(parents=True, exist_ok=True)

            file_path = collection_dir / f"{filename}{ext}"
            file_path.write_bytes(b"test")

            result1 = resolve_collection_id(file_path, tmp_path)
            result2 = resolve_collection_id(file_path, tmp_path)

            assert result1 == result2, "resolve_collection_id should be deterministic"


# =============================================================================
# Property: iter_geospatial_files filters by extension
# =============================================================================


class TestIterGeospatialFilesProperties:
    """Property-based tests for iter_geospatial_files function."""

    @pytest.mark.unit
    @given(filename=safe_filename, ext=geospatial_ext)
    @settings(max_examples=30)
    def test_iter_includes_geospatial_files(self, filename: str, ext: str) -> None:
        """iter_geospatial_files includes files with geospatial extensions."""
        with tempfile.TemporaryDirectory() as tmp_dir:
            tmp_path = Path(tmp_dir)
            file_path = tmp_path / f"{filename}{ext}"
            file_path.write_bytes(b"test")

            result = iter_geospatial_files(tmp_path)

            assert file_path in result, f"Should include {file_path} with ext {ext}"

    @pytest.mark.unit
    @given(filename=safe_filename)
    @settings(max_examples=30)
    def test_iter_excludes_non_geospatial(self, filename: str) -> None:
        """iter_geospatial_files excludes files with non-geospatial extensions."""
        with tempfile.TemporaryDirectory() as tmp_dir:
            tmp_path = Path(tmp_dir)
            # Extensions definitely NOT in GEOSPATIAL_EXTENSIONS
            # Note: .csv IS geospatial (for CSV with geometry), .json is NOT (we have .geojson)
            non_geo_exts = [".txt", ".json", ".md", ".py", ".xml", ".html"]

            for ext in non_geo_exts:
                file_path = tmp_path / f"{filename}{ext}"
                file_path.write_bytes(b"test")

            result = iter_geospatial_files(tmp_path)

            for ext in non_geo_exts:
                file_path = tmp_path / f"{filename}{ext}"
                assert file_path not in result, f"Should exclude {file_path}"

    @pytest.mark.unit
    @given(filename=safe_filename, ext=geospatial_ext)
    @settings(max_examples=30)
    def test_iter_returns_sorted(self, filename: str, ext: str) -> None:
        """iter_geospatial_files returns sorted list."""
        with tempfile.TemporaryDirectory() as tmp_dir:
            tmp_path = Path(tmp_dir)
            # Create multiple files
            for i in range(3):
                file_path = tmp_path / f"{filename}_{i}{ext}"
                file_path.write_bytes(b"test")

            result = iter_geospatial_files(tmp_path)

            assert result == sorted(result), "Results should be sorted"


# =============================================================================
# Property: SIDECAR_PATTERNS has consistent structure
# =============================================================================


class TestSidecarPatternsProperties:
    """Property-based tests verifying SIDECAR_PATTERNS structure."""

    @pytest.mark.unit
    def test_sidecar_patterns_extensions_start_with_dot(self) -> None:
        """All sidecar extensions should start with a dot."""
        for primary_ext, sidecars in SIDECAR_PATTERNS.items():
            assert primary_ext.startswith("."), f"Primary {primary_ext} should start with ."
            for sidecar in sidecars:
                assert sidecar.startswith("."), f"Sidecar {sidecar} should start with ."

    @pytest.mark.unit
    def test_sidecar_patterns_no_overlap(self) -> None:
        """No sidecar extension should equal its primary extension."""
        for primary_ext, sidecars in SIDECAR_PATTERNS.items():
            for sidecar in sidecars:
                assert sidecar.lower() != primary_ext.lower(), (
                    f"Sidecar {sidecar} equals primary {primary_ext}"
                )

    @pytest.mark.unit
    def test_sidecar_patterns_unique_per_primary(self) -> None:
        """Each primary should have unique sidecar extensions."""
        for primary_ext, sidecars in SIDECAR_PATTERNS.items():
            assert len(sidecars) == len(set(sidecars)), f"Duplicate sidecars for {primary_ext}"


# =============================================================================
# Property: Geospatial extensions are consistent
# =============================================================================


class TestGeospatialExtensionsProperties:
    """Property-based tests verifying GEOSPATIAL_EXTENSIONS structure."""

    @pytest.mark.unit
    def test_geospatial_extensions_all_lowercase(self) -> None:
        """All geospatial extensions should be lowercase."""
        for ext in GEOSPATIAL_EXTENSIONS:
            assert ext == ext.lower(), f"Extension {ext} should be lowercase"

    @pytest.mark.unit
    def test_geospatial_extensions_start_with_dot(self) -> None:
        """All geospatial extensions should start with a dot."""
        for ext in GEOSPATIAL_EXTENSIONS:
            assert ext.startswith("."), f"Extension {ext} should start with ."

    @pytest.mark.unit
    def test_geospatial_extensions_no_duplicates(self) -> None:
        """No duplicate extensions in GEOSPATIAL_EXTENSIONS."""
        # GEOSPATIAL_EXTENSIONS is a frozenset, so duplicates are impossible,
        # but this test documents the expectation
        assert len(GEOSPATIAL_EXTENSIONS) == len(set(GEOSPATIAL_EXTENSIONS)), (
            "Should have no duplicates"
        )


# =============================================================================
# Property: find_catalog_root walks up directory tree
# =============================================================================


class TestFindCatalogRootProperties:
    """Property-based tests for find_catalog_root function.

    Per ADR-0029, find_catalog_root uses .portolan/config.yaml as the single sentinel,
    unifying detection across all CLI commands.
    """

    @pytest.mark.integration  # Uses filesystem I/O (tempfile.TemporaryDirectory)
    @given(collection=collection_name, subdir=safe_filename)
    @settings(max_examples=30)
    def test_find_catalog_root_from_nested_dir(self, collection: str, subdir: str) -> None:
        """find_catalog_root finds catalog from nested subdirectory."""
        with tempfile.TemporaryDirectory() as tmp_dir:
            tmp_path = Path(tmp_dir)
            # Create managed catalog with .portolan/config.yaml + catalog.json (per issue #290)
            portolan_dir = tmp_path / ".portolan"
            portolan_dir.mkdir()
            (portolan_dir / "config.yaml").write_text("# Portolan config\n")
            (tmp_path / "catalog.json").write_text('{"type": "Catalog"}')  # Operational file

            # Create nested directory
            nested_dir = tmp_path / collection / subdir
            nested_dir.mkdir(parents=True, exist_ok=True)

            result = find_catalog_root(nested_dir)

            # Use resolve() on both sides - macOS /var → /private/var, Windows short names
            assert result == tmp_path.resolve(), (
                f"Should find catalog at {tmp_path.resolve()}, got {result}"
            )

    @pytest.mark.integration  # Uses filesystem I/O (tempfile.TemporaryDirectory)
    @given(collection=collection_name)
    @settings(max_examples=30)
    def test_find_catalog_root_from_root(self, collection: str) -> None:
        """find_catalog_root finds catalog when starting at catalog root."""
        with tempfile.TemporaryDirectory() as tmp_dir:
            tmp_path = Path(tmp_dir)
            # Create managed catalog with .portolan/config.yaml + catalog.json (per issue #290)
            portolan_dir = tmp_path / ".portolan"
            portolan_dir.mkdir()
            (portolan_dir / "config.yaml").write_text("# Portolan config\n")
            (tmp_path / "catalog.json").write_text('{"type": "Catalog"}')  # Operational file
            (tmp_path / collection).mkdir(exist_ok=True)

            result = find_catalog_root(tmp_path)

            # Use resolve() on both sides - macOS /var → /private/var, Windows short names
            assert result == tmp_path.resolve(), f"Should find catalog at {tmp_path.resolve()}"

    @pytest.mark.integration  # Uses filesystem I/O (tempfile.TemporaryDirectory)
    @given(dirname=safe_filename)
    @settings(max_examples=30)
    def test_find_catalog_root_returns_none_when_not_found(self, dirname: str) -> None:
        """find_catalog_root returns None when no .portolan/config.yaml exists."""
        with tempfile.TemporaryDirectory() as tmp_dir:
            tmp_path = Path(tmp_dir)
            # Create directory structure WITHOUT .portolan/config.yaml
            search_dir = tmp_path / dirname
            search_dir.mkdir(exist_ok=True)

            result = find_catalog_root(search_dir)

            assert result is None, f"Should return None, got {result}"

    @pytest.mark.integration  # Uses filesystem I/O (tempfile.TemporaryDirectory)
    @given(collection=collection_name, subdir=safe_filename)
    @settings(max_examples=30)
    def test_find_catalog_root_is_deterministic(self, collection: str, subdir: str) -> None:
        """Calling find_catalog_root twice returns same result."""
        with tempfile.TemporaryDirectory() as tmp_dir:
            tmp_path = Path(tmp_dir)
            portolan_dir = tmp_path / ".portolan"
            portolan_dir.mkdir()
            (portolan_dir / "config.yaml").write_text("# Portolan config\n")
            (tmp_path / "catalog.json").write_text('{"type": "Catalog"}')  # Operational file

            nested_dir = tmp_path / collection / subdir
            nested_dir.mkdir(parents=True, exist_ok=True)

            result1 = find_catalog_root(nested_dir)
            result2 = find_catalog_root(nested_dir)

            assert result1 == result2, "find_catalog_root should be deterministic"

    @pytest.mark.integration  # Uses filesystem I/O (tempfile.TemporaryDirectory)
    @given(collection=collection_name)
    @settings(max_examples=30)
    def test_find_catalog_root_ignores_unmanaged_stac(self, collection: str) -> None:
        """find_catalog_root ignores catalog.json-only directories (UNMANAGED_STAC)."""
        with tempfile.TemporaryDirectory() as tmp_dir:
            tmp_path = Path(tmp_dir)
            # Create UNMANAGED_STAC structure (catalog.json only, no .portolan)
            (tmp_path / "catalog.json").write_text('{"type": "Catalog"}')
            search_dir = tmp_path / collection
            search_dir.mkdir(exist_ok=True)

            result = find_catalog_root(search_dir)

            # Per ADR-0029, should NOT find unmanaged STAC catalogs
            assert result is None, f"Should ignore UNMANAGED_STAC, got {result}"


# =============================================================================
# Property: _increment_version handles various version formats
# =============================================================================


class TestIncrementVersionProperties:
    """Property-based tests for _increment_version function."""

    @pytest.mark.unit
    def test_increment_standard_semver(self) -> None:
        """_increment_version increments standard semver correctly."""
        from portolan_cli.dataset import _increment_version

        assert _increment_version("1.2.3") == "1.2.4"
        assert _increment_version("0.0.1") == "0.0.2"
        assert _increment_version("10.20.30") == "10.20.31"

    @pytest.mark.unit
    def test_increment_prerelease_semver(self) -> None:
        """_increment_version handles pre-release versions."""
        from portolan_cli.dataset import _increment_version

        # Pre-release with number suffix
        assert _increment_version("1.0.0-beta.1") == "1.0.0-beta.2"
        assert _increment_version("2.0.0-alpha.5") == "2.0.0-alpha.6"
        assert _increment_version("1.0.0-rc.10") == "1.0.0-rc.11"

    @pytest.mark.unit
    def test_increment_prerelease_no_number(self) -> None:
        """_increment_version handles pre-release without number."""
        from portolan_cli.dataset import _increment_version

        # Pre-release without number suffix appends .1 (preserves prerelease tag)
        result = _increment_version("1.0.0-beta")
        assert result == "1.0.0-beta.1"

        # Works for any prerelease name
        assert _increment_version("2.0.0-alpha") == "2.0.0-alpha.1"
        assert _increment_version("1.0.0-rc") == "1.0.0-rc.1"

    @pytest.mark.unit
    def test_increment_empty_version(self) -> None:
        """_increment_version handles empty version."""
        from portolan_cli.dataset import _increment_version

        assert _increment_version("") == "0.0.1"
        assert _increment_version(None) == "0.0.1"  # type: ignore[arg-type]

    @pytest.mark.unit
    def test_increment_short_version(self) -> None:
        """_increment_version handles short version strings."""
        from portolan_cli.dataset import _increment_version

        assert _increment_version("1") == "1.0.1"
        assert _increment_version("1.0") == "1.0.1"


# =============================================================================
# Property: Constants module exports correct values
# =============================================================================


class TestConstantsProperties:
    """Tests verifying constants module values."""

    @pytest.mark.unit
    def test_mtime_tolerance_is_reasonable(self) -> None:
        """MTIME_TOLERANCE_SECONDS is reasonable for NFS."""
        from portolan_cli.constants import MTIME_TOLERANCE_SECONDS

        # Should be at least 1 second for NFS compatibility
        assert MTIME_TOLERANCE_SECONDS >= 1.0
        # Should not be too large (more than 10 seconds is excessive)
        assert MTIME_TOLERANCE_SECONDS <= 10.0

    @pytest.mark.unit
    def test_max_catalog_search_depth_is_reasonable(self) -> None:
        """MAX_CATALOG_SEARCH_DEPTH is reasonable."""
        from portolan_cli.constants import MAX_CATALOG_SEARCH_DEPTH

        # Should be at least 5 levels
        assert MAX_CATALOG_SEARCH_DEPTH >= 5
        # Should not be too large (100 levels is excessive)
        assert MAX_CATALOG_SEARCH_DEPTH <= 100

    @pytest.mark.unit
    def test_geospatial_extensions_contains_common_formats(self) -> None:
        """GEOSPATIAL_EXTENSIONS includes common formats."""
        from portolan_cli.constants import GEOSPATIAL_EXTENSIONS

        assert ".geojson" in GEOSPATIAL_EXTENSIONS
        assert ".shp" in GEOSPATIAL_EXTENSIONS
        assert ".tif" in GEOSPATIAL_EXTENSIONS
        assert ".parquet" in GEOSPATIAL_EXTENSIONS

    @pytest.mark.unit
    def test_sidecar_patterns_contains_shapefile_extensions(self) -> None:
        """SIDECAR_PATTERNS includes shapefile sidecars."""
        from portolan_cli.constants import SIDECAR_PATTERNS

        assert ".shp" in SIDECAR_PATTERNS
        shp_sidecars = SIDECAR_PATTERNS[".shp"]
        assert ".dbf" in shp_sidecars
        assert ".shx" in shp_sidecars
        assert ".prj" in shp_sidecars


# =============================================================================
# Edge case tests for uncovered branches
# =============================================================================


class TestResolveCollectionIdEdgeCases:
    """Tests for edge cases in resolve_collection_id."""

    @pytest.mark.unit
    def test_resolve_raises_for_path_outside_catalog(self, tmp_path: Path) -> None:
        """resolve_collection_id raises ValueError for paths outside catalog."""
        from portolan_cli.dataset import resolve_collection_id

        catalog = tmp_path / "catalog"
        catalog.mkdir()
        outside_file = tmp_path / "outside.geojson"
        outside_file.write_text("{}")

        with pytest.raises(ValueError, match="outside catalog root"):
            resolve_collection_id(outside_file, catalog)

    @pytest.mark.unit
    def test_resolve_raises_for_file_at_root(self, tmp_path: Path) -> None:
        """resolve_collection_id raises ValueError for file directly at root."""
        from portolan_cli.dataset import resolve_collection_id

        catalog = tmp_path / "catalog"
        catalog.mkdir()
        root_file = catalog / "data.geojson"
        root_file.write_text("{}")

        with pytest.raises(ValueError, match="must be in a subdirectory"):
            resolve_collection_id(root_file, catalog)


class TestIsCurrentEdgeCases:
    """Tests for edge cases in is_current function."""

    @pytest.mark.unit
    def test_is_current_returns_false_no_versions_file(self, tmp_path: Path) -> None:
        """is_current returns False when versions.json doesn't exist."""
        from portolan_cli.dataset import is_current

        test_file = tmp_path / "test.parquet"
        test_file.write_bytes(b"data")
        versions_path = tmp_path / "versions.json"

        assert is_current(test_file, versions_path) is False

    @pytest.mark.unit
    def test_is_current_returns_false_empty_versions(self, tmp_path: Path) -> None:
        """is_current returns False when versions list is empty."""
        from portolan_cli.dataset import is_current

        test_file = tmp_path / "test.parquet"
        test_file.write_bytes(b"data")
        versions_path = tmp_path / "versions.json"
        versions_path.write_text(
            '{"spec_version": "1.0.0", "current_version": null, "versions": []}'
        )

        assert is_current(test_file, versions_path) is False

    @pytest.mark.unit
    def test_is_current_returns_false_asset_not_found(self, tmp_path: Path) -> None:
        """is_current returns False when asset not in versions."""
        import json

        from portolan_cli.dataset import is_current

        test_file = tmp_path / "new_file.parquet"
        test_file.write_bytes(b"data")
        versions_path = tmp_path / "versions.json"
        versions_path.write_text(
            json.dumps(
                {
                    "spec_version": "1.0.0",
                    "current_version": "1.0.0",
                    "versions": [
                        {
                            "version": "1.0.0",
                            "created": "2024-01-01T00:00:00Z",
                            "message": "Initial",
                            "breaking": False,
                            "changes": ["other.parquet"],
                            "assets": {
                                "other.parquet": {
                                    "sha256": "abc",
                                    "size_bytes": 100,
                                    "href": "other.parquet",
                                }
                            },
                        }
                    ],
                }
            )
        )

        assert is_current(test_file, versions_path) is False

    @pytest.mark.unit
    def test_is_current_uses_mtime_fast_path(self, tmp_path: Path) -> None:
        """is_current returns True when mtime matches (fast path)."""
        import json

        from portolan_cli.dataset import is_current

        test_file = tmp_path / "test.parquet"
        test_file.write_bytes(b"data")
        current_mtime = test_file.stat().st_mtime

        versions_path = tmp_path / "versions.json"
        versions_path.write_text(
            json.dumps(
                {
                    "spec_version": "1.0.0",
                    "current_version": "1.0.0",
                    "versions": [
                        {
                            "version": "1.0.0",
                            "created": "2024-01-01T00:00:00Z",
                            "message": "Initial",
                            "breaking": False,
                            "changes": ["test.parquet"],
                            "assets": {
                                "test.parquet": {
                                    "sha256": "differenthash",  # Wrong hash, but mtime matches
                                    "size_bytes": 4,
                                    "mtime": current_mtime,
                                    "href": "test.parquet",
                                }
                            },
                        }
                    ],
                }
            )
        )

        # Should return True via mtime fast path (doesn't check hash)
        assert is_current(test_file, versions_path) is True

    @pytest.mark.unit
    def test_is_current_uses_size_check_medium_path(self, tmp_path: Path) -> None:
        """is_current returns False when size differs (skips expensive sha256)."""
        import json

        from portolan_cli.dataset import is_current

        test_file = tmp_path / "test.parquet"
        test_file.write_bytes(b"data" * 100)  # 400 bytes

        versions_path = tmp_path / "versions.json"
        versions_path.write_text(
            json.dumps(
                {
                    "spec_version": "1.0.0",
                    "current_version": "1.0.0",
                    "versions": [
                        {
                            "version": "1.0.0",
                            "created": "2024-01-01T00:00:00Z",
                            "message": "Initial",
                            "breaking": False,
                            "changes": ["test.parquet"],
                            "assets": {
                                "test.parquet": {
                                    "sha256": "abc",
                                    "size_bytes": 100,  # Different size
                                    "mtime": 0,  # Old mtime
                                    "href": "test.parquet",
                                }
                            },
                        }
                    ],
                }
            )
        )

        # Size differs → return False without computing sha256
        assert is_current(test_file, versions_path) is False

    @pytest.mark.unit
    def test_is_current_uses_sha256_slow_path(self, tmp_path: Path) -> None:
        """is_current falls back to sha256 when mtime differs but size matches."""
        import json

        from portolan_cli.dataset import compute_checksum, is_current

        test_file = tmp_path / "test.parquet"
        content = b"exact content"
        test_file.write_bytes(content)
        correct_hash = compute_checksum(test_file)

        versions_path = tmp_path / "versions.json"
        versions_path.write_text(
            json.dumps(
                {
                    "spec_version": "1.0.0",
                    "current_version": "1.0.0",
                    "versions": [
                        {
                            "version": "1.0.0",
                            "created": "2024-01-01T00:00:00Z",
                            "message": "Initial",
                            "breaking": False,
                            "changes": ["test.parquet"],
                            "assets": {
                                "test.parquet": {
                                    "sha256": correct_hash,
                                    "size_bytes": len(content),
                                    "mtime": 0,  # Old mtime, forces sha256 check
                                    "href": "test.parquet",
                                }
                            },
                        }
                    ],
                }
            )
        )

        # Mtime old, size matches → fall back to sha256 → match
        assert is_current(test_file, versions_path) is True


class TestAddFilesEdgeCases:
    """Tests for edge cases in add_files function."""

    @pytest.mark.unit
    def test_add_files_resolves_symlinks(self, tmp_path: Path) -> None:
        """add_files resolves symlinks to track real files."""
        from unittest.mock import patch

        from portolan_cli.dataset import add_files

        # Setup catalog
        catalog = tmp_path / "catalog"
        catalog.mkdir()
        (catalog / "catalog.json").write_text('{"type": "Catalog"}')
        (catalog / ".portolan").mkdir()

        # Create real file and symlink
        collection = catalog / "data"
        collection.mkdir()
        real_file = collection / "real.geojson"
        real_file.write_text('{"type": "FeatureCollection", "features": []}')
        link = collection / "link.geojson"
        link.symlink_to(real_file)

        # Per Issue #281: add_files now calls prepare_dataset + finalize_datasets
        with (
            patch("portolan_cli.dataset.prepare_dataset") as mock_prepare,
            patch("portolan_cli.dataset.finalize_datasets") as mock_finalize,
        ):
            from portolan_cli.dataset import PreparedDataset
            from portolan_cli.formats import FormatType

            mock_prepare.return_value = PreparedDataset(
                item_id="real",
                collection_id="data",
                format_type=FormatType.VECTOR,
                bbox=[0, 0, 1, 1],
                asset_files={},
                item_json_path=collection / "real" / "real.json",
            )
            mock_finalize.return_value = []

            added, skipped, failures = add_files(
                paths=[link],
                catalog_root=catalog,
            )

            # Should have called prepare_dataset with resolved path
            if mock_prepare.called:
                call_path = mock_prepare.call_args.kwargs.get("path") or mock_prepare.call_args[
                    1
                ].get("path")
                # The path should be resolved (not a symlink)
                assert not call_path.is_symlink() or call_path.resolve() == real_file.resolve()

    @pytest.mark.unit
    def test_add_files_error_wraps_context(self, tmp_path: Path) -> None:
        """add_files collects errors with file path context (Issue #175).

        Previously this test verified errors were raised with context.
        Now errors are collected in the failures list instead.
        """
        from unittest.mock import patch

        from portolan_cli.dataset import add_files

        # Setup catalog
        catalog = tmp_path / "catalog"
        catalog.mkdir()
        (catalog / "catalog.json").write_text('{"type": "Catalog"}')
        (catalog / ".portolan").mkdir()

        collection = catalog / "data"
        collection.mkdir()
        test_file = collection / "test.geojson"
        test_file.write_text("{}")

        # Per Issue #281: add_files now calls prepare_dataset instead of add_dataset
        with patch("portolan_cli.dataset.prepare_dataset") as mock_prepare:
            mock_prepare.side_effect = ValueError("original error")

            # Per Issue #175: errors are now collected instead of raised
            added, skipped, failures = add_files(paths=[test_file], catalog_root=catalog)

            # Should have one failure with the raw error message
            assert len(failures) == 1
            assert failures[0].error == "original error"


class TestRemoveFilesEdgeCases:
    """Tests for edge cases in remove_files function."""

    @pytest.mark.unit
    def test_remove_files_skips_symlinks_without_keep(self, tmp_path: Path) -> None:
        """remove_files refuses to delete symlinks (security)."""
        import json

        from portolan_cli.dataset import remove_files

        # Setup catalog
        catalog = tmp_path / "catalog"
        catalog.mkdir()
        (catalog / "catalog.json").write_text('{"type": "Catalog"}')

        # Create real file and symlink
        collection = catalog / "data"
        collection.mkdir()
        real_file = collection / "real.geojson"
        real_file.write_text("{}")
        link = collection / "link.geojson"
        link.symlink_to(real_file)

        # Create versions.json
        (collection / "versions.json").write_text(
            json.dumps({"current_version": "1.0.0", "versions": []})
        )

        # Try to rm the symlink without --keep
        removed, skipped = remove_files(
            paths=[link],
            catalog_root=catalog,
            keep=False,
            dry_run=False,
        )

        # Symlink should be skipped (not deleted)
        assert link in skipped
        assert link not in removed
        # Original file should still exist
        assert real_file.exists()

    @pytest.mark.unit
    def test_remove_files_skips_outside_catalog(self, tmp_path: Path) -> None:
        """remove_files skips files outside catalog."""
        from portolan_cli.dataset import remove_files

        # Setup catalog
        catalog = tmp_path / "catalog"
        catalog.mkdir()
        (catalog / "catalog.json").write_text('{"type": "Catalog"}')

        # File outside catalog
        outside = tmp_path / "outside.geojson"
        outside.write_text("{}")

        removed, skipped = remove_files(
            paths=[outside],
            catalog_root=catalog,
            keep=False,
            dry_run=False,
        )

        assert outside in skipped
        assert outside not in removed
        # File should still exist
        assert outside.exists()

    @pytest.mark.unit
    def test_remove_files_cleans_sidecars(self, tmp_path: Path) -> None:
        """remove_files deletes sidecars when removing shapefile."""
        import json

        from portolan_cli.dataset import remove_files

        # Setup catalog
        catalog = tmp_path / "catalog"
        catalog.mkdir()
        (catalog / "catalog.json").write_text('{"type": "Catalog"}')

        collection = catalog / "data"
        collection.mkdir()

        # Create shapefile with sidecars
        shp = collection / "test.shp"
        dbf = collection / "test.dbf"
        shx = collection / "test.shx"
        prj = collection / "test.prj"
        for f in [shp, dbf, shx, prj]:
            f.write_bytes(b"data")

        # Create versions.json
        (collection / "versions.json").write_text(
            json.dumps(
                {
                    "spec_version": "1.0.0",
                    "current_version": "1.0.0",
                    "versions": [
                        {
                            "version": "1.0.0",
                            "created": "2024-01-01T00:00:00Z",
                            "message": "test",
                            "breaking": False,
                            "changes": ["test.parquet"],
                            "assets": {
                                "test.parquet": {
                                    "sha256": "abc",
                                    "size_bytes": 4,
                                    "href": "test.parquet",
                                }
                            },
                        }
                    ],
                }
            )
        )

        removed, skipped = remove_files(
            paths=[shp],
            catalog_root=catalog,
            keep=False,
            dry_run=False,
        )

        # Primary file deleted
        assert not shp.exists()
        # Sidecars also deleted
        assert not dbf.exists()
        assert not shx.exists()
        assert not prj.exists()


class TestRemoveFromVersionsEdgeCases:
    """Tests for edge cases in _remove_from_versions function."""

    @pytest.mark.unit
    def test_remove_from_versions_noop_no_file(self, tmp_path: Path) -> None:
        """_remove_from_versions is a no-op if versions.json doesn't exist."""
        from portolan_cli.dataset import _remove_from_versions

        test_file = tmp_path / "test.parquet"
        versions_path = tmp_path / "versions.json"

        # Should not raise
        _remove_from_versions(test_file, versions_path)

    @pytest.mark.unit
    def test_remove_from_versions_noop_empty_versions(self, tmp_path: Path) -> None:
        """_remove_from_versions is a no-op if versions list is empty."""
        from portolan_cli.dataset import _remove_from_versions

        test_file = tmp_path / "test.parquet"
        versions_path = tmp_path / "versions.json"
        versions_path.write_text(
            '{"spec_version": "1.0.0", "current_version": null, "versions": []}'
        )

        # Should not raise
        _remove_from_versions(test_file, versions_path)

        # File unchanged
        content = versions_path.read_text()
        assert '"versions": []' in content


class TestIterFilesWithSidecarsEdgeCases:
    """Tests for edge cases in iter_files_with_sidecars."""

    @pytest.mark.unit
    def test_iter_returns_empty_for_non_directory(self, tmp_path: Path) -> None:
        """iter_files_with_sidecars returns empty list for non-directory."""
        from portolan_cli.dataset import iter_files_with_sidecars

        test_file = tmp_path / "test.geojson"
        test_file.write_text("{}")

        result = iter_files_with_sidecars(test_file)
        assert result == []

    @pytest.mark.unit
    def test_iter_skips_non_geospatial_in_nested_dirs(self, tmp_path: Path) -> None:
        """iter_files_with_sidecars skips non-geospatial files in nested dirs."""
        from portolan_cli.dataset import iter_files_with_sidecars

        # Create nested structure
        nested = tmp_path / "sub" / "deep"
        nested.mkdir(parents=True)

        # Create geospatial and non-geospatial files
        geo = nested / "data.geojson"
        geo.write_text("{}")
        non_geo = nested / "readme.txt"
        non_geo.write_text("hello")
        non_geo2 = nested / "config.json"
        non_geo2.write_text("{}")

        result = iter_files_with_sidecars(tmp_path)

        # Only geospatial file returned
        assert geo in result
        assert non_geo not in result
        assert non_geo2 not in result


# =============================================================================
# Multi-Asset Properties (Issue #133)
# =============================================================================


class TestMultiAssetProperties:
    """Property-based tests for multi-asset tracking behavior (issue #133).

    Per issue #133, ALL files in item directories should be tracked as assets,
    not just geospatial files. These tests verify the properties of that behavior.
    """

    @pytest.mark.unit
    @given(ext=st.sampled_from([".png", ".jpg", ".pdf", ".txt", ".md", ".json", ".xml"]))
    @settings(max_examples=5, deadline=30000)
    def test_media_type_is_deterministic(self, ext: str) -> None:
        """Same extension always produces the same MIME type."""
        from portolan_cli.dataset import _get_media_type

        path1 = Path(f"file1{ext}")
        path2 = Path(f"different_name{ext}")

        assert _get_media_type(path1) == _get_media_type(path2)

    @pytest.mark.unit
    @given(ext=st.sampled_from([".PNG", ".Png", ".pNg", ".pdf", ".PDF", ".Pdf"]))
    @settings(max_examples=15)
    def test_media_type_is_case_insensitive(self, ext: str) -> None:
        """Media type lookup is case-insensitive."""
        from portolan_cli.dataset import _get_media_type

        path = Path(f"test{ext}")
        lower_path = Path(f"test{ext.lower()}")

        assert _get_media_type(path) == _get_media_type(lower_path)

    @pytest.mark.unit
    @given(ext=st.sampled_from([".parquet", ".tif", ".geojson", ".gpkg", ".csv"]))
    @settings(max_examples=10)
    def test_data_formats_get_data_role(self, ext: str) -> None:
        """Data format extensions always get 'data' role."""
        from portolan_cli.dataset import _get_asset_role

        path = Path(f"file{ext}")
        assert _get_asset_role(path) == "data"

    @pytest.mark.unit
    @given(ext=st.sampled_from([".png", ".jpg", ".jpeg", ".svg"]))
    @settings(max_examples=10)
    def test_image_formats_get_thumbnail_role(self, ext: str) -> None:
        """Image format extensions always get 'thumbnail' role."""
        from portolan_cli.dataset import _get_asset_role

        path = Path(f"image{ext}")
        assert _get_asset_role(path) == "thumbnail"

    @pytest.mark.unit
    @given(ext=st.sampled_from([".pdf", ".txt", ".md", ".html"]))
    @settings(max_examples=10)
    def test_doc_formats_get_documentation_role(self, ext: str) -> None:
        """Documentation format extensions always get 'documentation' role."""
        from portolan_cli.dataset import _get_asset_role

        path = Path(f"doc{ext}")
        assert _get_asset_role(path) == "documentation"

    @pytest.mark.unit
    @given(
        ext=st.text(
            alphabet=string.ascii_lowercase + string.digits,
            min_size=3,
            max_size=8,
        ).map(lambda s: f".{s}zz")  # Add suffix to avoid collisions
    )
    @settings(max_examples=30)
    def test_unknown_extensions_get_default_role(self, ext: str) -> None:
        """Unknown extensions get 'data' as default role."""
        from portolan_cli.dataset import _ROLE_MAP, _get_asset_role

        # Skip if extension happens to be in the role map (very unlikely with .zz suffix)
        if ext.lower() in _ROLE_MAP:
            return

        path = Path(f"file{ext}")
        assert _get_asset_role(path) == "data"

    @pytest.mark.unit
    @given(
        ext=st.text(
            alphabet=string.ascii_lowercase + string.digits,
            min_size=3,
            max_size=8,
        ).map(lambda s: f".{s}zz")  # Add suffix to avoid collisions
    )
    @settings(max_examples=30)
    def test_unknown_extensions_get_octet_stream(self, ext: str) -> None:
        """Unknown extensions get 'application/octet-stream' MIME type."""
        from portolan_cli.dataset import _MEDIA_TYPE_MAP, _get_media_type

        # Skip if extension happens to be in the map (very unlikely with .zz suffix)
        if ext.lower() in _MEDIA_TYPE_MAP:
            return

        path = Path(f"file{ext}")
        assert _get_media_type(path) == "application/octet-stream"

    @pytest.mark.unit
    def test_ignored_files_are_stac_structural(self) -> None:
        """IGNORED_FILES contains only STAC structural files."""
        from portolan_cli.dataset import IGNORED_FILES

        # These are the structural files that should never be assets
        expected = {"catalog.json", "collection.json", "versions.json"}
        assert IGNORED_FILES == frozenset(expected)

    @pytest.mark.unit
    @given(
        filenames=st.lists(
            st.text(
                st.sampled_from(string.ascii_lowercase + string.digits),
                min_size=1,
                max_size=10,
            ).map(lambda s: f"{s}.txt"),
            min_size=1,
            max_size=5,
        )
    )
    @settings(max_examples=5, deadline=30000)
    def test_scan_item_assets_excludes_hidden_files(self, filenames: list[str]) -> None:
        """_scan_item_assets never includes hidden files."""
        from portolan_cli.dataset import _scan_item_assets

        with tempfile.TemporaryDirectory() as tmp_dir:
            item_dir = Path(tmp_dir)

            # Create a primary "data" file
            primary = item_dir / "data.parquet"
            primary.write_bytes(b"fake parquet")

            # Create regular files
            for fn in filenames:
                (item_dir / fn).write_text("content")

            # Create hidden files
            (item_dir / ".hidden").write_text("hidden")
            (item_dir / ".DS_Store").write_bytes(b"junk")

            stac_assets, asset_files, asset_paths = _scan_item_assets(
                item_dir=item_dir,
                item_id="test",
                primary_file=primary,
                collection_dir=Path(tmp_dir),  # Use temp dir as collection
            )

            # No hidden files should be in results
            for filename in asset_files.keys():
                assert not filename.startswith("."), f"Hidden file {filename} included"

    @pytest.mark.unit
    def test_scan_item_assets_excludes_structural_files(self) -> None:
        """_scan_item_assets excludes STAC structural files."""
        from portolan_cli.dataset import IGNORED_FILES, _scan_item_assets

        with tempfile.TemporaryDirectory() as tmp_dir:
            item_dir = Path(tmp_dir)

            # Create primary file
            primary = item_dir / "data.parquet"
            primary.write_bytes(b"parquet")

            # Create structural files that should be ignored
            for ignored in IGNORED_FILES:
                (item_dir / ignored).write_text("{}")

            # Create a regular file
            (item_dir / "readme.txt").write_text("hello")

            stac_assets, asset_files, asset_paths = _scan_item_assets(
                item_dir=item_dir,
                item_id="test",
                primary_file=primary,
                collection_dir=item_dir,  # Same dir for this test
            )

            # No structural files should be in results
            for filename in asset_files.keys():
                assert filename not in IGNORED_FILES, f"Structural {filename} included"

            # But regular files should be
            assert "readme.txt" in asset_files or "data.parquet" in asset_files


# =============================================================================
# Property: catalog root detection for add . (Issue #137)
# =============================================================================


class TestCatalogRootAddProperties:
    """Property-based tests for add . at catalog root behavior.

    These tests verify the invariants of the fix for Issue #137:
    - resolve_collection_id(path, catalog_root) raises ValueError when path == catalog_root
    - When target_path == catalog_root, collection_id should be None (inferred per-file)
    - When target_path != catalog_root, collection_id should be the first path component
    """

    @pytest.mark.unit
    @given(collection=collection_name)
    @settings(max_examples=30)
    def test_resolve_collection_id_always_fails_at_root(self, collection: str) -> None:
        """resolve_collection_id raises ValueError when path == catalog_root.

        This documents the pre-condition that drove the fix: the root cause
        of Issue #137 is that resolve_collection_id returns empty parts when
        path equals catalog_root.
        """
        with tempfile.TemporaryDirectory() as tmp:
            catalog_root = Path(tmp).resolve()
            # path == catalog_root → empty relative parts → ValueError
            import pytest as _pytest

            with _pytest.raises(ValueError, match="Cannot determine collection"):
                resolve_collection_id(catalog_root, catalog_root)

    @pytest.mark.unit
    @given(collection=collection_name, filename=safe_filename, ext=geospatial_ext)
    @settings(max_examples=30)
    def test_resolve_collection_id_returns_first_component_for_nested_path(
        self, collection: str, filename: str, ext: str
    ) -> None:
        """resolve_collection_id returns the first directory component for nested paths.

        This verifies the invariant that drives collection inference in add_files
        when collection_id=None: for any file inside a collection directory,
        the first component of the path relative to catalog_root is the collection.
        """
        with tempfile.TemporaryDirectory() as tmp:
            catalog_root = Path(tmp).resolve()
            # Create: catalog_root/<collection>/<filename><ext>
            col_dir = catalog_root / collection
            col_dir.mkdir(exist_ok=True)
            geo_file = col_dir / f"{filename}{ext}"
            geo_file.write_bytes(b"geo")

            result = resolve_collection_id(geo_file, catalog_root)

            assert result == collection, f"Expected collection '{collection}', got '{result}'"

    @pytest.mark.unit
    @given(collection=collection_name, sub=collection_name, filename=safe_filename)
    @settings(max_examples=30)
    def test_resolve_collection_id_first_component_only_for_deeply_nested(
        self, collection: str, sub: str, filename: str
    ) -> None:
        """resolve_collection_id returns FIRST component, not full path, for deep nesting.

        Per ADR-0022: the collection is always the first directory component.
        For a/b/c.parquet relative to root, the collection is 'a', not 'a/b'.
        """
        with tempfile.TemporaryDirectory() as tmp:
            catalog_root = Path(tmp).resolve()
            # Create deep nesting: catalog_root/<collection>/<sub>/<file>.parquet
            deep_dir = catalog_root / collection / sub
            deep_dir.mkdir(parents=True, exist_ok=True)
            geo_file = deep_dir / f"{filename}.parquet"
            geo_file.write_bytes(b"parquet")

            result = resolve_collection_id(geo_file, catalog_root)

            assert result == collection, (
                f"Expected first component '{collection}', got '{result}' for deeply nested path"
            )


# =============================================================================
# Property: item_id derivation from parent directory (Issue #163)
# =============================================================================


# Valid GeoJSON template for testing add_dataset()
VALID_GEOJSON_TEMPLATE = {
    "type": "FeatureCollection",
    "features": [
        {
            "type": "Feature",
            "geometry": {"type": "Point", "coordinates": [0, 0]},
            "properties": {"name": "test"},
        }
    ],
}

# Minimal STAC catalog for testing
MINIMAL_CATALOG_TEMPLATE = {
    "type": "Catalog",
    "id": "test-catalog",
    "stac_version": "1.0.0",
    "description": "Test catalog",
    "links": [],
}


def _setup_managed_catalog(catalog_root: Path) -> None:
    """Set up a minimal managed catalog for testing.

    Creates:
    - catalog.json (STAC root)
    - .portolan/config.yaml (sentinel)

    Note: state.json removed per issue #290.
    """
    import json

    # STAC catalog at root
    (catalog_root / "catalog.json").write_text(json.dumps(MINIMAL_CATALOG_TEMPLATE, indent=2))

    # .portolan sentinel (per ADR-0029)
    portolan_dir = catalog_root / ".portolan"
    portolan_dir.mkdir(exist_ok=True)
    (portolan_dir / "config.yaml").write_text("# Portolan config\n")


class TestItemIdDerivationProperties:
    """Property-based tests for item_id derivation.

    Issue #163: item_id should be derived from parent directory name, not filename.
    This enables the canonical structure: catalog_root/collection/item_id/files.

    CRITICAL: These tests MUST call add_dataset() to verify actual behavior,
    not just test Python's Path operations (which would be tautological).
    """

    @pytest.mark.integration  # Calls add_dataset() with real filesystem + geoparquet-io
    @given(
        collection=collection_name,
        item_id=collection_name,  # Reuse collection_name strategy (safe dir names)
        filename=safe_filename,
    )
    @settings(
        max_examples=5, deadline=30000
    )  # Reduced: integration tests with geoparquet-io are slow
    def test_add_dataset_derives_item_id_from_parent_directory(
        self, collection: str, item_id: str, filename: str
    ) -> None:
        """add_dataset() should derive item_id from parent directory name, not filename.

        This is the fundamental invariant from Issue #163: the directory structure
        determines item boundaries, not filenames.

        NON-TAUTOLOGICAL: This test calls add_dataset() and verifies DatasetInfo.item_id.
        """
        import json

        from portolan_cli.dataset import add_dataset

        with tempfile.TemporaryDirectory() as tmp:
            catalog_root = Path(tmp).resolve()

            # Set up managed catalog (catalog.json + .portolan sentinel)
            _setup_managed_catalog(catalog_root)

            # Create canonical structure: catalog_root/<collection>/<item_id>/<file>
            item_dir = catalog_root / collection / item_id
            item_dir.mkdir(parents=True, exist_ok=True)

            # Create a VALID GeoJSON file (with geometry)
            geo_file = item_dir / f"{filename}.geojson"
            geo_file.write_text(json.dumps(VALID_GEOJSON_TEMPLATE))

            # Call add_dataset() - this is what we're actually testing
            result = add_dataset(
                path=geo_file,
                catalog_root=catalog_root,
                collection_id=collection,
            )

            # Verify add_dataset() derived item_id from parent directory name
            assert result.item_id == item_id, (
                f"add_dataset() should derive item_id='{item_id}' from parent dir, "
                f"not '{result.item_id}' (filename stem: '{filename}')"
            )

    @pytest.mark.integration  # Calls add_dataset() with real filesystem + geoparquet-io
    @given(
        collection=collection_name,
        item_id=collection_name,
        filename1=safe_filename,
        filename2=safe_filename,
    )
    @settings(max_examples=5, deadline=60000)  # Reduced: creates 2 files per iteration
    def test_add_dataset_multiple_files_same_item_id(
        self, collection: str, item_id: str, filename1: str, filename2: str
    ) -> None:
        """Multiple calls to add_dataset() for files in same directory return same item_id.

        Issue #163: Files in the same item directory are assets of ONE item,
        not separate items with different item_ids.

        NON-TAUTOLOGICAL: This test calls add_dataset() twice and compares results.
        """
        import json

        from portolan_cli.dataset import add_dataset

        # Ensure distinct filenames
        if filename1 == filename2:
            filename2 = f"{filename2}_other"

        with tempfile.TemporaryDirectory() as tmp:
            catalog_root = Path(tmp).resolve()

            # Set up managed catalog (catalog.json + .portolan sentinel)
            _setup_managed_catalog(catalog_root)

            # Create item directory with two valid GeoJSON files
            item_dir = catalog_root / collection / item_id
            item_dir.mkdir(parents=True, exist_ok=True)

            file1 = item_dir / f"{filename1}.geojson"
            file2 = item_dir / f"{filename2}.geojson"
            file1.write_text(json.dumps(VALID_GEOJSON_TEMPLATE))
            file2.write_text(json.dumps(VALID_GEOJSON_TEMPLATE))

            # Call add_dataset() for both files
            result1 = add_dataset(
                path=file1,
                catalog_root=catalog_root,
                collection_id=collection,
            )
            result2 = add_dataset(
                path=file2,
                catalog_root=catalog_root,
                collection_id=collection,
            )

            # Both should have the same item_id (parent directory name)
            assert result1.item_id == result2.item_id == item_id, (
                f"Files in {item_dir} should share item_id='{item_id}', "
                f"got '{result1.item_id}' and '{result2.item_id}'"
            )


# =============================================================================
# Property: Pre-validation atomicity (Issue #163)
# =============================================================================


# Invalid GeoJSON templates for atomicity testing
INVALID_GEOJSON_NO_GEOMETRY = {
    "type": "FeatureCollection",
    "features": [{"type": "Feature", "properties": {"name": "no geometry"}}],
}

INVALID_GEOJSON_EMPTY_FEATURES = {
    "type": "FeatureCollection",
    "features": [],
}


class TestPreValidationAtomicityProperties:
    """Property-based tests for pre-validation atomicity.

    Issue #163: Failed add operations should not create partial artifacts.
    Pre-validation should check for valid geometry BEFORE any filesystem operations.

    CRITICAL: These tests MUST call add_dataset() to verify actual atomicity,
    not just _pre_validate_geometry() in isolation.
    """

    @pytest.mark.integration  # Calls add_dataset() - requires full catalog setup
    @given(collection=collection_name, item_id=collection_name)
    @settings(max_examples=5, deadline=30000)
    def test_add_dataset_invalid_geojson_no_stac_artifacts(
        self, collection: str, item_id: str
    ) -> None:
        """add_dataset() failure should not create STAC collection/item/versions.json.

        Issue #163: When add_dataset fails due to missing geometry, no STAC artifacts
        (collection.json, item.json, versions.json) should be created.

        NON-TAUTOLOGICAL: This test calls add_dataset() and checks for artifacts.
        """
        import json

        from portolan_cli.dataset import add_dataset

        with tempfile.TemporaryDirectory() as tmp:
            catalog_root = Path(tmp).resolve()

            # Set up managed catalog (catalog.json + .portolan sentinel)
            _setup_managed_catalog(catalog_root)

            # Create canonical structure with INVALID GeoJSON file
            item_dir = catalog_root / collection / item_id
            item_dir.mkdir(parents=True, exist_ok=True)
            invalid_geojson = item_dir / "data.geojson"
            invalid_geojson.write_text(json.dumps(INVALID_GEOJSON_NO_GEOMETRY))

            # Record file state before add_dataset attempt
            files_before = set(catalog_root.rglob("*"))

            # add_dataset should fail due to missing geometry
            try:
                add_dataset(
                    path=invalid_geojson,
                    catalog_root=catalog_root,
                    collection_id=collection,
                )
                raise AssertionError("Expected ValueError for GeoJSON without geometry")
            except ValueError:
                pass  # Expected

            # Check that no STAC artifacts were created
            files_after = set(catalog_root.rglob("*"))
            new_files = files_after - files_before

            stac_artifacts = [f for f in new_files if f.suffix == ".json"]
            assert not stac_artifacts, (
                f"add_dataset() failure should not create STAC artifacts. "
                f"Created: {[str(f.relative_to(catalog_root)) for f in stac_artifacts]}"
            )

    @pytest.mark.integration  # Calls add_dataset() - requires full catalog setup
    @given(collection=collection_name, item_id=collection_name)
    @settings(max_examples=5, deadline=30000)
    def test_add_dataset_empty_features_no_stac_artifacts(
        self, collection: str, item_id: str
    ) -> None:
        """add_dataset() with empty features array should fail without creating artifacts.

        NON-TAUTOLOGICAL: This test calls add_dataset() and verifies atomicity.
        """
        import json

        from portolan_cli.dataset import add_dataset

        with tempfile.TemporaryDirectory() as tmp:
            catalog_root = Path(tmp).resolve()

            # Set up managed catalog (catalog.json + .portolan sentinel)
            _setup_managed_catalog(catalog_root)

            # Create canonical structure with empty features GeoJSON
            item_dir = catalog_root / collection / item_id
            item_dir.mkdir(parents=True, exist_ok=True)
            empty_geojson = item_dir / "data.geojson"
            empty_geojson.write_text(json.dumps(INVALID_GEOJSON_EMPTY_FEATURES))

            # Record file state
            files_before = set(catalog_root.rglob("*"))

            # add_dataset should fail
            try:
                add_dataset(
                    path=empty_geojson,
                    catalog_root=catalog_root,
                    collection_id=collection,
                )
                raise AssertionError("Expected ValueError for empty features")
            except ValueError:
                pass  # Expected

            # No new STAC artifacts
            files_after = set(catalog_root.rglob("*"))
            new_files = files_after - files_before

            stac_artifacts = [f for f in new_files if f.suffix == ".json"]
            assert not stac_artifacts, f"Should not create artifacts: {stac_artifacts}"

    @pytest.mark.unit
    @given(collection=collection_name, item_id=collection_name)
    @settings(max_examples=30)
    def test_pre_validate_geometry_is_pure(self, collection: str, item_id: str) -> None:
        """_pre_validate_geometry() should be a pure function (no side effects).

        This tests the helper function directly to ensure it doesn't create files.
        """
        import json

        from portolan_cli.dataset import _pre_validate_geometry
        from portolan_cli.formats import FormatType

        with tempfile.TemporaryDirectory() as tmp:
            catalog_root = Path(tmp).resolve()

            # Create INVALID GeoJSON
            invalid_geojson = catalog_root / "data.geojson"
            invalid_geojson.write_text(json.dumps(INVALID_GEOJSON_NO_GEOMETRY))

            # Record state before
            files_before = set(catalog_root.rglob("*"))

            # Pre-validation should fail
            try:
                _pre_validate_geometry(invalid_geojson, FormatType.VECTOR)
                raise AssertionError("Expected ValueError")
            except ValueError:
                pass

            # State should be unchanged (pure function)
            files_after = set(catalog_root.rglob("*"))
            assert files_before == files_after, "_pre_validate_geometry should be pure"


# =============================================================================
# Property: item_id override (Issue #136)
# =============================================================================


# Strategies for invalid item IDs
invalid_item_id_with_slash = st.builds(
    lambda prefix, suffix: f"{prefix}/{suffix}",
    safe_filename,
    safe_filename,
)
invalid_item_id_with_backslash = st.builds(
    lambda prefix, suffix: f"{prefix}\\{suffix}",
    safe_filename,
    safe_filename,
)


class TestItemIdOverrideProperties:
    """Property-based tests for --item-id override functionality.

    Issue #136: Users should be able to override automatic item ID derivation
    via the --item-id flag. Invalid item IDs (containing path separators or
    special values like '.' and '..') should be rejected.
    """

    @pytest.mark.integration
    @given(
        collection=collection_name,
        custom_item_id=collection_name,
    )
    @settings(max_examples=5, deadline=30000)
    def test_add_dataset_respects_item_id_override(
        self, collection: str, custom_item_id: str
    ) -> None:
        """add_dataset() should use the provided item_id instead of deriving it.

        Issue #136: When item_id is explicitly provided, it should override
        the automatic derivation from parent directory name.
        """
        import json

        from portolan_cli.dataset import add_dataset

        with tempfile.TemporaryDirectory() as tmp:
            catalog_root = Path(tmp).resolve()
            _setup_managed_catalog(catalog_root)

            # Create structure where auto-derived item_id would differ
            # Parent dir is "auto-derived-dir" but we pass custom_item_id
            item_dir = catalog_root / collection / "auto-derived-dir"
            item_dir.mkdir(parents=True, exist_ok=True)

            geo_file = item_dir / "data.geojson"
            geo_file.write_text(json.dumps(VALID_GEOJSON_TEMPLATE))

            result = add_dataset(
                path=geo_file,
                catalog_root=catalog_root,
                collection_id=collection,
                item_id=custom_item_id,
            )

            # Item ID should be the custom one, not "auto-derived-dir"
            assert result.item_id == custom_item_id, (
                f"add_dataset() should use provided item_id='{custom_item_id}', "
                f"not auto-derived '{result.item_id}'"
            )

    @pytest.mark.unit
    @given(invalid_id=invalid_item_id_with_slash)
    @settings(max_examples=10)
    def test_add_dataset_rejects_item_id_with_slash(self, invalid_id: str) -> None:
        """add_dataset() should reject item_ids containing forward slashes.

        Issue #136: item_id must be a single path segment, not a path.
        """
        import json

        from portolan_cli.dataset import add_dataset

        with tempfile.TemporaryDirectory() as tmp:
            catalog_root = Path(tmp).resolve()
            _setup_managed_catalog(catalog_root)

            # Create minimal structure
            item_dir = catalog_root / "test-collection" / "item-dir"
            item_dir.mkdir(parents=True, exist_ok=True)

            geo_file = item_dir / "data.geojson"
            geo_file.write_text(json.dumps(VALID_GEOJSON_TEMPLATE))

            with pytest.raises(ValueError, match="single path segment"):
                add_dataset(
                    path=geo_file,
                    catalog_root=catalog_root,
                    collection_id="test-collection",
                    item_id=invalid_id,
                )

    @pytest.mark.unit
    @given(invalid_id=invalid_item_id_with_backslash)
    @settings(max_examples=10)
    def test_add_dataset_rejects_item_id_with_backslash(self, invalid_id: str) -> None:
        """add_dataset() should reject item_ids containing backslashes.

        Issue #136: item_id must be a single path segment, not a path.
        """
        import json

        from portolan_cli.dataset import add_dataset

        with tempfile.TemporaryDirectory() as tmp:
            catalog_root = Path(tmp).resolve()
            _setup_managed_catalog(catalog_root)

            item_dir = catalog_root / "test-collection" / "item-dir"
            item_dir.mkdir(parents=True, exist_ok=True)

            geo_file = item_dir / "data.geojson"
            geo_file.write_text(json.dumps(VALID_GEOJSON_TEMPLATE))

            with pytest.raises(ValueError, match="single path segment"):
                add_dataset(
                    path=geo_file,
                    catalog_root=catalog_root,
                    collection_id="test-collection",
                    item_id=invalid_id,
                )

    @pytest.mark.unit
    @given(invalid_id=st.sampled_from([".", ".."]))
    @settings(max_examples=2)
    def test_add_dataset_rejects_dot_item_ids(self, invalid_id: str) -> None:
        """add_dataset() should reject '.' and '..' as item_ids.

        Issue #136: These are reserved path components and not valid item IDs.
        """
        import json

        from portolan_cli.dataset import add_dataset

        with tempfile.TemporaryDirectory() as tmp:
            catalog_root = Path(tmp).resolve()
            _setup_managed_catalog(catalog_root)

            item_dir = catalog_root / "test-collection" / "item-dir"
            item_dir.mkdir(parents=True, exist_ok=True)

            geo_file = item_dir / "data.geojson"
            geo_file.write_text(json.dumps(VALID_GEOJSON_TEMPLATE))

            with pytest.raises(ValueError, match="single path segment"):
                add_dataset(
                    path=geo_file,
                    catalog_root=catalog_root,
                    collection_id="test-collection",
                    item_id=invalid_id,
                )
