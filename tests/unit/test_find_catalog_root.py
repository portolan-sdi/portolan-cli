"""Unit tests for unified find_catalog_root function.

Per ADR-0029, find_catalog_root() uses .portolan/config.yaml as the single sentinel
for catalog root detection, unifying behavior across all CLI commands.
"""

from __future__ import annotations

import os
from pathlib import Path

import pytest


class TestFindCatalogRoot:
    """Unit tests for find_catalog_root function."""

    @pytest.mark.unit
    def test_finds_config_yaml_in_current_dir(self, tmp_path: Path) -> None:
        """find_catalog_root returns path when .portolan/config.yaml exists in current dir."""
        # Import here to allow tests to fail before implementation exists
        from portolan_cli.catalog import find_catalog_root

        # Setup: Create full managed catalog structure (config.yaml + state.json)
        portolan_dir = tmp_path / ".portolan"
        portolan_dir.mkdir()
        (portolan_dir / "config.yaml").write_text("# Portolan config\n")
        (portolan_dir / "state.json").write_text("{}")  # Operational file required

        # Act
        result = find_catalog_root(tmp_path)

        # Assert
        assert result == tmp_path

    @pytest.mark.unit
    def test_finds_config_yaml_in_parent(self, tmp_path: Path) -> None:
        """find_catalog_root walks up to find .portolan/config.yaml in parent."""
        from portolan_cli.catalog import find_catalog_root

        # Setup: Catalog at root, start from nested subdir
        portolan_dir = tmp_path / ".portolan"
        portolan_dir.mkdir()
        (portolan_dir / "config.yaml").write_text("# Portolan config\n")
        (portolan_dir / "state.json").write_text("{}")  # Operational file required

        nested_dir = tmp_path / "collection" / "item" / "assets"
        nested_dir.mkdir(parents=True)

        # Act
        result = find_catalog_root(nested_dir)

        # Assert
        assert result == tmp_path

    @pytest.mark.unit
    def test_returns_none_when_not_found(self, tmp_path: Path) -> None:
        """find_catalog_root returns None when no .portolan/config.yaml exists."""
        from portolan_cli.catalog import find_catalog_root

        # Setup: Empty directory
        subdir = tmp_path / "some" / "nested" / "path"
        subdir.mkdir(parents=True)

        # Act
        result = find_catalog_root(subdir)

        # Assert
        assert result is None

    @pytest.mark.unit
    def test_ignores_bare_portolan_directory(self, tmp_path: Path) -> None:
        """find_catalog_root ignores .portolan without config.yaml."""
        from portolan_cli.catalog import find_catalog_root

        # Setup: .portolan exists but no config.yaml
        portolan_dir = tmp_path / ".portolan"
        portolan_dir.mkdir()
        # Only state.json, no config.yaml
        (portolan_dir / "state.json").write_text("{}")

        # Act
        result = find_catalog_root(tmp_path)

        # Assert: Should NOT find this as a catalog
        assert result is None

    @pytest.mark.unit
    def test_ignores_catalog_json_without_config_yaml(self, tmp_path: Path) -> None:
        """find_catalog_root ignores catalog.json-only directories (UNMANAGED_STAC)."""
        from portolan_cli.catalog import find_catalog_root

        # Setup: Only catalog.json, no .portolan/config.yaml
        catalog_json = tmp_path / "catalog.json"
        catalog_json.write_text('{"type": "Catalog", "id": "test"}')

        subdir = tmp_path / "collection"
        subdir.mkdir()

        # Act
        result = find_catalog_root(subdir)

        # Assert: Should NOT find UNMANAGED_STAC as a catalog
        assert result is None

    @pytest.mark.unit
    def test_depth_limit_prevents_traversal_beyond_max(self, tmp_path: Path) -> None:
        """find_catalog_root stops after MAX_CATALOG_SEARCH_DEPTH levels."""
        from portolan_cli.catalog import find_catalog_root
        from portolan_cli.constants import MAX_CATALOG_SEARCH_DEPTH

        # Setup: Create catalog at root, then nest beyond MAX_CATALOG_SEARCH_DEPTH
        portolan_dir = tmp_path / ".portolan"
        portolan_dir.mkdir()
        (portolan_dir / "config.yaml").write_text("# Portolan config\n")

        # Create nested path deeper than MAX_CATALOG_SEARCH_DEPTH
        deep_path = tmp_path
        for i in range(MAX_CATALOG_SEARCH_DEPTH + 5):
            deep_path = deep_path / f"level{i}"
        deep_path.mkdir(parents=True)

        # Act
        result = find_catalog_root(deep_path)

        # Assert: Should NOT find catalog beyond depth limit
        assert result is None

    @pytest.mark.unit
    def test_depth_limit_finds_within_max(self, tmp_path: Path) -> None:
        """find_catalog_root finds catalog within MAX_CATALOG_SEARCH_DEPTH levels."""
        from portolan_cli.catalog import find_catalog_root
        from portolan_cli.constants import MAX_CATALOG_SEARCH_DEPTH

        # Setup: Create catalog at root
        portolan_dir = tmp_path / ".portolan"
        portolan_dir.mkdir()
        (portolan_dir / "config.yaml").write_text("# Portolan config\n")
        (portolan_dir / "state.json").write_text("{}")  # Operational file required

        # Create nested path exactly at MAX_CATALOG_SEARCH_DEPTH - 1 (should find)
        deep_path = tmp_path
        for i in range(MAX_CATALOG_SEARCH_DEPTH - 1):
            deep_path = deep_path / f"level{i}"
        deep_path.mkdir(parents=True)

        # Act
        result = find_catalog_root(deep_path)

        # Assert: Should find catalog within depth limit
        assert result == tmp_path

    @pytest.mark.unit
    def test_defaults_to_cwd_when_no_path_given(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """find_catalog_root defaults to current working directory."""
        from portolan_cli.catalog import find_catalog_root

        # Setup: Create catalog and change to it
        portolan_dir = tmp_path / ".portolan"
        portolan_dir.mkdir()
        (portolan_dir / "config.yaml").write_text("# Portolan config\n")
        (portolan_dir / "state.json").write_text("{}")  # Operational file required

        monkeypatch.chdir(tmp_path)

        # Act: Call without path argument
        result = find_catalog_root()

        # Assert
        assert result == tmp_path

    @pytest.mark.unit
    def test_resolves_to_absolute_path(self, tmp_path: Path) -> None:
        """find_catalog_root returns resolved absolute path."""
        from portolan_cli.catalog import find_catalog_root

        # Setup
        portolan_dir = tmp_path / ".portolan"
        portolan_dir.mkdir()
        (portolan_dir / "config.yaml").write_text("# Portolan config\n")
        (portolan_dir / "state.json").write_text("{}")  # Operational file required

        # Act
        result = find_catalog_root(tmp_path)

        # Assert: Result should be absolute and resolved
        assert result is not None
        assert result.is_absolute()
        assert result == result.resolve()

    @pytest.mark.unit
    @pytest.mark.skipif(os.name == "nt", reason="Symlinks require admin on Windows")
    def test_resolves_symlinks(self, tmp_path: Path) -> None:
        """find_catalog_root resolves symlinks correctly."""
        from portolan_cli.catalog import find_catalog_root

        # Setup: Create catalog
        catalog_dir = tmp_path / "real_catalog"
        catalog_dir.mkdir()
        portolan_dir = catalog_dir / ".portolan"
        portolan_dir.mkdir()
        (portolan_dir / "config.yaml").write_text("# Portolan config\n")
        (portolan_dir / "state.json").write_text("{}")  # Operational file required

        # Create symlink to catalog
        symlink_dir = tmp_path / "symlink_catalog"
        symlink_dir.symlink_to(catalog_dir)

        # Act: Search from symlink
        result = find_catalog_root(symlink_dir)

        # Assert: Should find the resolved catalog
        assert result is not None
        assert result.resolve() == catalog_dir.resolve()

    @pytest.mark.unit
    def test_handles_nonexistent_path_gracefully(self) -> None:
        """find_catalog_root handles non-existent start path."""
        from portolan_cli.catalog import find_catalog_root

        # Act & Assert: Should not raise, returns None
        result = find_catalog_root(Path("/nonexistent/path/that/doesnt/exist"))

        # The behavior depends on implementation - either None or raises
        # We accept None as valid behavior for non-existent paths
        assert result is None

    @pytest.mark.unit
    def test_finds_nearest_catalog_with_nested_catalogs(self, tmp_path: Path) -> None:
        """find_catalog_root finds the nearest catalog when nested catalogs exist."""
        from portolan_cli.catalog import find_catalog_root

        # Setup: Parent catalog
        parent_portolan = tmp_path / ".portolan"
        parent_portolan.mkdir()
        (parent_portolan / "config.yaml").write_text("# Parent catalog\n")
        (parent_portolan / "state.json").write_text("{}")  # Operational file required

        # Setup: Nested child catalog
        child_dir = tmp_path / "child"
        child_dir.mkdir()
        child_portolan = child_dir / ".portolan"
        child_portolan.mkdir()
        (child_portolan / "config.yaml").write_text("# Child catalog\n")
        (child_portolan / "state.json").write_text("{}")  # Operational file required

        # Setup: Subdir inside child
        subdir = child_dir / "collection"
        subdir.mkdir()

        # Act: Search from inside child catalog
        result = find_catalog_root(subdir)

        # Assert: Should find child (nearest), not parent
        assert result == child_dir

    @pytest.mark.unit
    def test_stops_at_filesystem_root(self, tmp_path: Path) -> None:
        """find_catalog_root stops at filesystem root without infinite loop."""
        from portolan_cli.catalog import find_catalog_root

        # Act: Search from real path with no catalog anywhere above
        # This tests that we don't infinite loop at filesystem root
        result = find_catalog_root(tmp_path)

        # Assert: Should return None, not hang
        assert result is None


class TestFindCatalogRootEdgeCases:
    """Edge case tests for find_catalog_root."""

    @pytest.mark.unit
    def test_empty_config_yaml_is_valid(self, tmp_path: Path) -> None:
        """find_catalog_root accepts empty config.yaml as valid sentinel."""
        from portolan_cli.catalog import find_catalog_root

        # Setup: Empty config.yaml with operational file
        portolan_dir = tmp_path / ".portolan"
        portolan_dir.mkdir()
        (portolan_dir / "config.yaml").write_text("")
        (portolan_dir / "state.json").write_text("{}")  # Operational file required

        # Act
        result = find_catalog_root(tmp_path)

        # Assert: Empty file is still a valid sentinel
        assert result == tmp_path

    @pytest.mark.unit
    def test_config_yaml_with_content_is_valid(self, tmp_path: Path) -> None:
        """find_catalog_root accepts config.yaml with real content."""
        from portolan_cli.catalog import find_catalog_root

        # Setup: config.yaml with content
        portolan_dir = tmp_path / ".portolan"
        portolan_dir.mkdir()
        (portolan_dir / "config.yaml").write_text("remote:\n  url: s3://my-bucket/catalog\n")
        (portolan_dir / "state.json").write_text("{}")  # Operational file required

        # Act
        result = find_catalog_root(tmp_path)

        # Assert
        assert result == tmp_path

    @pytest.mark.unit
    def test_ignores_config_yaml_outside_portolan_dir(self, tmp_path: Path) -> None:
        """find_catalog_root ignores config.yaml not inside .portolan/."""
        from portolan_cli.catalog import find_catalog_root

        # Setup: config.yaml at root (wrong location)
        (tmp_path / "config.yaml").write_text("# Wrong location\n")

        subdir = tmp_path / "subdir"
        subdir.mkdir()

        # Act
        result = find_catalog_root(subdir)

        # Assert: Should NOT find this
        assert result is None

    @pytest.mark.unit
    def test_case_sensitive_portolan_directory(self, tmp_path: Path) -> None:
        """find_catalog_root is case-sensitive for .portolan directory."""
        from portolan_cli.catalog import find_catalog_root

        # Setup: Wrong case (uppercase)
        wrong_case_dir = tmp_path / ".PORTOLAN"
        wrong_case_dir.mkdir()
        (wrong_case_dir / "config.yaml").write_text("# Wrong case\n")

        # Act
        result = find_catalog_root(tmp_path)

        # Assert: Should NOT find wrong case (on case-sensitive filesystems)
        # Note: This test may pass trivially on case-insensitive filesystems (macOS default)
        # but is important for Linux
        if not (tmp_path / ".portolan").exists():
            assert result is None

    @pytest.mark.unit
    def test_case_sensitive_config_yaml(self, tmp_path: Path) -> None:
        """find_catalog_root is case-sensitive for config.yaml file."""
        from portolan_cli.catalog import find_catalog_root

        # Setup: .portolan exists but wrong case for config.yaml
        portolan_dir = tmp_path / ".portolan"
        portolan_dir.mkdir()
        (portolan_dir / "CONFIG.YAML").write_text("# Wrong case\n")

        # Act
        result = find_catalog_root(tmp_path)

        # Assert: Should NOT find wrong case
        if not (portolan_dir / "config.yaml").exists():
            assert result is None

    @pytest.mark.unit
    def test_require_operational_false_for_init(self, tmp_path: Path) -> None:
        """find_catalog_root with require_operational=False finds config.yaml-only repos.

        This mode is used during init_catalog() when config.yaml is written
        before catalog.json/state.json are created.
        """
        from portolan_cli.catalog import find_catalog_root

        # Setup: Only config.yaml, no operational files (simulates mid-init state)
        portolan_dir = tmp_path / ".portolan"
        portolan_dir.mkdir()
        (portolan_dir / "config.yaml").write_text("# Portolan config\n")
        # No state.json or catalog.json

        # Act: With require_operational=True (default), should NOT find
        result_default = find_catalog_root(tmp_path)
        assert result_default is None, "Default should require operational files"

        # Act: With require_operational=False, SHOULD find
        result_init = find_catalog_root(tmp_path, require_operational=False)
        assert result_init == tmp_path, "require_operational=False should find config.yaml-only"

    @pytest.mark.unit
    def test_operational_file_catalog_json_at_root(self, tmp_path: Path) -> None:
        """find_catalog_root accepts catalog.json at root as operational file."""
        from portolan_cli.catalog import find_catalog_root

        # Setup: config.yaml + catalog.json (no state.json)
        portolan_dir = tmp_path / ".portolan"
        portolan_dir.mkdir()
        (portolan_dir / "config.yaml").write_text("# Portolan config\n")
        (tmp_path / "catalog.json").write_text('{"type": "Catalog"}')  # At root, not in .portolan

        # Act
        result = find_catalog_root(tmp_path)

        # Assert: Should find - catalog.json at root is valid operational file
        assert result == tmp_path
