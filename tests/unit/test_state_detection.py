"""Tests for catalog state detection.

The detect_state() function determines the current state of a directory:
- MANAGED: A full Portolan catalog (.portolan/config.json AND .portolan/state.json exist)
- UNMANAGED_STAC: An existing STAC catalog (catalog.json exists but not managed)
- FRESH: No catalog exists (neither .portolan nor catalog.json)
"""

from __future__ import annotations

import json
from pathlib import Path

import pytest

from portolan_cli.catalog import CatalogState, detect_state


class TestCatalogStateEnum:
    """Tests for CatalogState enum values."""

    @pytest.mark.unit
    def test_catalog_state_has_managed_value(self) -> None:
        """CatalogState should have MANAGED value."""
        assert hasattr(CatalogState, "MANAGED")
        assert CatalogState.MANAGED.value == "managed"

    @pytest.mark.unit
    def test_catalog_state_has_unmanaged_stac_value(self) -> None:
        """CatalogState should have UNMANAGED_STAC value."""
        assert hasattr(CatalogState, "UNMANAGED_STAC")
        assert CatalogState.UNMANAGED_STAC.value == "unmanaged_stac"

    @pytest.mark.unit
    def test_catalog_state_has_fresh_value(self) -> None:
        """CatalogState should have FRESH value."""
        assert hasattr(CatalogState, "FRESH")
        assert CatalogState.FRESH.value == "fresh"


class TestDetectStateFresh:
    """Tests for detect_state() returning FRESH."""

    @pytest.mark.unit
    def test_empty_directory_is_fresh(self, tmp_path: Path) -> None:
        """Empty directory should be FRESH."""
        state = detect_state(tmp_path)
        assert state == CatalogState.FRESH

    @pytest.mark.unit
    def test_directory_with_random_files_is_fresh(self, tmp_path: Path) -> None:
        """Directory with random files (not catalog.json) should be FRESH."""
        (tmp_path / "data.geojson").write_text('{"type": "FeatureCollection"}')
        (tmp_path / "readme.md").write_text("# My Data")

        state = detect_state(tmp_path)
        assert state == CatalogState.FRESH

    @pytest.mark.unit
    def test_empty_portolan_directory_is_fresh(self, tmp_path: Path) -> None:
        """Empty .portolan directory (no config/state) should be FRESH.

        This is an edge case: someone created .portolan but didn't complete init.
        """
        portolan_dir = tmp_path / ".portolan"
        portolan_dir.mkdir()

        state = detect_state(tmp_path)
        assert state == CatalogState.FRESH

    @pytest.mark.unit
    def test_portolan_with_only_config_is_fresh(self, tmp_path: Path) -> None:
        """Partial .portolan (only config.json, no state.json) is FRESH.

        Both config.json AND state.json are required for MANAGED state.
        """
        portolan_dir = tmp_path / ".portolan"
        portolan_dir.mkdir()
        (portolan_dir / "config.json").write_text("{}")

        state = detect_state(tmp_path)
        assert state == CatalogState.FRESH

    @pytest.mark.unit
    def test_portolan_with_only_state_is_fresh(self, tmp_path: Path) -> None:
        """Partial .portolan (only state.json, no config.json) is FRESH.

        Both config.json AND state.json are required for MANAGED state.
        """
        portolan_dir = tmp_path / ".portolan"
        portolan_dir.mkdir()
        (portolan_dir / "state.json").write_text("{}")

        state = detect_state(tmp_path)
        assert state == CatalogState.FRESH


class TestDetectStateManaged:
    """Tests for detect_state() returning MANAGED."""

    @pytest.mark.unit
    def test_full_portolan_structure_is_managed(self, tmp_path: Path) -> None:
        """Directory with .portolan/config.json AND .portolan/state.json is MANAGED."""
        portolan_dir = tmp_path / ".portolan"
        portolan_dir.mkdir()
        (portolan_dir / "config.json").write_text("{}")
        (portolan_dir / "state.json").write_text("{}")

        state = detect_state(tmp_path)
        assert state == CatalogState.MANAGED

    @pytest.mark.unit
    def test_managed_with_additional_files(self, tmp_path: Path) -> None:
        """MANAGED state should work even with extra files in .portolan."""
        portolan_dir = tmp_path / ".portolan"
        portolan_dir.mkdir()
        (portolan_dir / "config.json").write_text("{}")
        (portolan_dir / "state.json").write_text("{}")
        (portolan_dir / "versions.json").write_text("{}")
        (portolan_dir / "catalog.json").write_text("{}")

        state = detect_state(tmp_path)
        assert state == CatalogState.MANAGED

    @pytest.mark.unit
    def test_managed_with_root_catalog_json(self, tmp_path: Path) -> None:
        """MANAGED state with catalog.json at root (expected structure)."""
        # Root catalog.json
        (tmp_path / "catalog.json").write_text(
            json.dumps({"type": "Catalog", "stac_version": "1.0.0"})
        )
        # .portolan with both required files
        portolan_dir = tmp_path / ".portolan"
        portolan_dir.mkdir()
        (portolan_dir / "config.json").write_text("{}")
        (portolan_dir / "state.json").write_text("{}")

        state = detect_state(tmp_path)
        assert state == CatalogState.MANAGED


class TestDetectStateUnmanagedStac:
    """Tests for detect_state() returning UNMANAGED_STAC."""

    @pytest.mark.unit
    def test_catalog_json_without_portolan_is_unmanaged(self, tmp_path: Path) -> None:
        """catalog.json at root without .portolan is UNMANAGED_STAC."""
        catalog_data = {
            "type": "Catalog",
            "stac_version": "1.0.0",
            "id": "my-catalog",
            "description": "External catalog",
            "links": [],
        }
        (tmp_path / "catalog.json").write_text(json.dumps(catalog_data))

        state = detect_state(tmp_path)
        assert state == CatalogState.UNMANAGED_STAC

    @pytest.mark.unit
    def test_catalog_json_with_empty_portolan_is_unmanaged(self, tmp_path: Path) -> None:
        """catalog.json with empty .portolan dir is UNMANAGED_STAC."""
        catalog_data = {
            "type": "Catalog",
            "stac_version": "1.0.0",
            "id": "my-catalog",
            "description": "External catalog",
            "links": [],
        }
        (tmp_path / "catalog.json").write_text(json.dumps(catalog_data))
        (tmp_path / ".portolan").mkdir()

        state = detect_state(tmp_path)
        assert state == CatalogState.UNMANAGED_STAC

    @pytest.mark.unit
    def test_catalog_json_with_partial_portolan_is_unmanaged(self, tmp_path: Path) -> None:
        """catalog.json with partial .portolan (only config) is UNMANAGED_STAC.

        The presence of catalog.json without full management files
        indicates an unmanaged STAC catalog.
        """
        catalog_data = {
            "type": "Catalog",
            "stac_version": "1.0.0",
            "id": "my-catalog",
            "description": "External catalog",
            "links": [],
        }
        (tmp_path / "catalog.json").write_text(json.dumps(catalog_data))
        portolan_dir = tmp_path / ".portolan"
        portolan_dir.mkdir()
        (portolan_dir / "config.json").write_text("{}")
        # Note: state.json is missing, so not fully managed

        state = detect_state(tmp_path)
        assert state == CatalogState.UNMANAGED_STAC


def is_case_sensitive_fs(tmp_path: Path) -> bool:
    """Detect if filesystem is case-sensitive."""
    test_file = tmp_path / "CaseSensitivityTest"
    test_file.touch()
    return not (tmp_path / "casesensitivitytest").exists()


class TestDetectStateEdgeCases:
    """Edge case tests for detect_state()."""

    @pytest.mark.unit
    def test_detect_state_does_not_read_file_contents(self, tmp_path: Path) -> None:
        """detect_state() should only check file existence, not contents.

        This ensures fast detection without I/O overhead.
        """
        # Create files with invalid JSON (would error if read)
        portolan_dir = tmp_path / ".portolan"
        portolan_dir.mkdir()
        (portolan_dir / "config.json").write_text("not valid json {{{")
        (portolan_dir / "state.json").write_text("also invalid!!!")

        # Should still detect as MANAGED (existence check only)
        state = detect_state(tmp_path)
        assert state == CatalogState.MANAGED

    @pytest.mark.unit
    def test_detect_state_handles_nonexistent_path(self, tmp_path: Path) -> None:
        """detect_state() on non-existent path should raise or return FRESH.

        Note: The spec says 'no network calls, no reading file contents'
        but doesn't specify behavior for non-existent paths. We treat as FRESH.
        """
        nonexistent = tmp_path / "does-not-exist"

        # Should treat as FRESH (no files exist)
        state = detect_state(nonexistent)
        assert state == CatalogState.FRESH

    @pytest.mark.unit
    def test_detect_state_case_sensitive_filenames(self, tmp_path: Path) -> None:
        """File detection should be case-sensitive (catalog.json != Catalog.json).

        This test only runs on case-sensitive filesystems (e.g., Linux ext4).
        On case-insensitive filesystems (e.g., macOS APFS, Windows NTFS),
        Catalog.json and catalog.json refer to the same file.
        """
        if not is_case_sensitive_fs(tmp_path):
            pytest.skip("Filesystem is case-insensitive")

        # Wrong case - should not be detected on case-sensitive FS
        (tmp_path / "Catalog.json").write_text('{"type": "Catalog"}')

        state = detect_state(tmp_path)
        assert state == CatalogState.FRESH

    @pytest.mark.unit
    def test_detect_state_symlink_to_portolan(self, tmp_path: Path) -> None:
        """Symlinked .portolan directory should work."""
        # Create real portolan dir elsewhere
        real_portolan = tmp_path / "real-portolan"
        real_portolan.mkdir()
        (real_portolan / "config.json").write_text("{}")
        (real_portolan / "state.json").write_text("{}")

        # Create workspace with symlink
        workspace = tmp_path / "workspace"
        workspace.mkdir()
        (workspace / ".portolan").symlink_to(real_portolan)

        state = detect_state(workspace)
        assert state == CatalogState.MANAGED
