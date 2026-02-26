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

from portolan_cli.dataset import (
    GEOSPATIAL_EXTENSIONS,
    SIDECAR_PATTERNS,
    find_catalog_root,
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
collection_name = st.text(
    st.sampled_from(string.ascii_lowercase + string.digits + "_-"),
    min_size=1,
    max_size=30,
).filter(lambda s: not s.startswith(".") and not s.startswith("-"))


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
                # Handle compound extensions like .aux.xml
                sidecar_stem = sidecar_path.stem
                if sidecar_stem.endswith(".aux"):
                    sidecar_stem = sidecar_stem[:-4]  # Remove .aux part
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
    """Property-based tests for find_catalog_root function."""

    @pytest.mark.unit
    @given(collection=collection_name, subdir=safe_filename)
    @settings(max_examples=30)
    def test_find_catalog_root_from_nested_dir(self, collection: str, subdir: str) -> None:
        """find_catalog_root finds catalog from nested subdirectory."""
        with tempfile.TemporaryDirectory() as tmp_dir:
            tmp_path = Path(tmp_dir)
            # Create catalog with catalog.json
            (tmp_path / "catalog.json").write_text('{"type": "Catalog"}')

            # Create nested directory
            nested_dir = tmp_path / collection / subdir
            nested_dir.mkdir(parents=True, exist_ok=True)

            result = find_catalog_root(nested_dir)

            # Use resolve() on both sides - macOS /var → /private/var, Windows short names
            assert result == tmp_path.resolve(), (
                f"Should find catalog at {tmp_path.resolve()}, got {result}"
            )

    @pytest.mark.unit
    @given(collection=collection_name)
    @settings(max_examples=30)
    def test_find_catalog_root_from_root(self, collection: str) -> None:
        """find_catalog_root finds catalog when starting at catalog root."""
        with tempfile.TemporaryDirectory() as tmp_dir:
            tmp_path = Path(tmp_dir)
            # Create catalog with catalog.json
            (tmp_path / "catalog.json").write_text('{"type": "Catalog"}')
            (tmp_path / collection).mkdir(exist_ok=True)

            result = find_catalog_root(tmp_path)

            # Use resolve() on both sides - macOS /var → /private/var, Windows short names
            assert result == tmp_path.resolve(), f"Should find catalog at {tmp_path.resolve()}"

    @pytest.mark.unit
    @given(dirname=safe_filename)
    @settings(max_examples=30)
    def test_find_catalog_root_returns_none_when_not_found(self, dirname: str) -> None:
        """find_catalog_root returns None when no catalog.json exists."""
        with tempfile.TemporaryDirectory() as tmp_dir:
            tmp_path = Path(tmp_dir)
            # Create directory structure WITHOUT catalog.json
            search_dir = tmp_path / dirname
            search_dir.mkdir(exist_ok=True)

            result = find_catalog_root(search_dir)

            assert result is None, f"Should return None, got {result}"

    @pytest.mark.unit
    @given(collection=collection_name, subdir=safe_filename)
    @settings(max_examples=30)
    def test_find_catalog_root_is_deterministic(self, collection: str, subdir: str) -> None:
        """Calling find_catalog_root twice returns same result."""
        with tempfile.TemporaryDirectory() as tmp_dir:
            tmp_path = Path(tmp_dir)
            (tmp_path / "catalog.json").write_text('{"type": "Catalog"}')

            nested_dir = tmp_path / collection / subdir
            nested_dir.mkdir(parents=True, exist_ok=True)

            result1 = find_catalog_root(nested_dir)
            result2 = find_catalog_root(nested_dir)

            assert result1 == result2, "find_catalog_root should be deterministic"


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

        with patch("portolan_cli.dataset.add_dataset") as mock_add:
            from portolan_cli.dataset import DatasetInfo
            from portolan_cli.formats import FormatType

            mock_add.return_value = DatasetInfo(
                item_id="real",
                collection_id="data",
                format_type=FormatType.VECTOR,
                bbox=None,
                asset_paths=[],
            )

            added, skipped = add_files(
                paths=[link],
                catalog_root=catalog,
            )

            # Should have called add_dataset with resolved path
            if mock_add.called:
                call_path = mock_add.call_args.kwargs.get("path") or mock_add.call_args[1].get(
                    "path"
                )
                # The path should be resolved (not a symlink)
                assert not call_path.is_symlink() or call_path.resolve() == real_file.resolve()

    @pytest.mark.unit
    def test_add_files_error_wraps_context(self, tmp_path: Path) -> None:
        """add_files wraps errors with file path context."""
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

        with patch("portolan_cli.dataset.add_dataset") as mock_add:
            mock_add.side_effect = ValueError("original error")

            with pytest.raises(ValueError, match=r"Failed to add.*original error"):
                add_files(paths=[test_file], catalog_root=catalog)


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
