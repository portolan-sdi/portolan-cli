"""Tests for metadata file syncing in push (Issue #426).

Portolan push should sync ALL catalog files to remote, not just versioned assets.
This includes style.json, thumbnails, collection.json updates, etc.
"""

from __future__ import annotations

import json
from pathlib import Path
from typing import TYPE_CHECKING
from unittest.mock import MagicMock, patch

import pytest

if TYPE_CHECKING:
    from typing import Any


# =============================================================================
# Fixtures
# =============================================================================


@pytest.fixture
def catalog_with_metadata(tmp_path: Path) -> Path:
    """Create a catalog with various metadata files for testing.

    Structure:
        catalog_root/
        ├── catalog.json
        ├── README.md
        ├── .portolan/
        │   └── config.yaml
        ├── .env
        ├── collection1/
        │   ├── collection.json
        │   ├── README.md
        │   ├── style.json
        │   ├── collection1.thumb.png
        │   ├── versions.json
        │   └── .portolan/
        │       └── metadata.yaml
        └── __pycache__/
            └── cache.pyc
    """
    catalog_root = tmp_path / "catalog"
    catalog_root.mkdir()

    # Root level files
    (catalog_root / "catalog.json").write_text(
        json.dumps({"type": "Catalog", "id": "test-catalog"})
    )
    (catalog_root / "README.md").write_text("# Test Catalog")
    # Root-level metadata file (should be synced)
    (catalog_root / "root-style.json").write_text(json.dumps({"version": 8, "name": "root-style"}))

    # .portolan directory (should be excluded)
    portolan_dir = catalog_root / ".portolan"
    portolan_dir.mkdir()
    (portolan_dir / "config.yaml").write_text("backend: filesystem")

    # .env file (should be excluded)
    (catalog_root / ".env").write_text("SECRET_KEY=supersecret")

    # __pycache__ (should be excluded)
    pycache = catalog_root / "__pycache__"
    pycache.mkdir()
    (pycache / "cache.pyc").write_bytes(b"\x00\x00\x00\x00")

    # Collection with metadata files
    collection_dir = catalog_root / "collection1"
    collection_dir.mkdir()
    (collection_dir / "collection.json").write_text(
        json.dumps({"type": "Collection", "id": "collection1"})
    )
    (collection_dir / "README.md").write_text("# Collection 1")
    (collection_dir / "style.json").write_text(json.dumps({"version": 8, "name": "test-style"}))
    (collection_dir / "collection1.thumb.png").write_bytes(b"\x89PNG\r\n\x1a\n" + b"\x00" * 100)
    (collection_dir / "versions.json").write_text(
        json.dumps(
            {
                "spec_version": "1.0.0",
                "current_version": "v1",
                "versions": [
                    {
                        "version": "v1",
                        "created": "2024-01-01T00:00:00Z",
                        "breaking": False,
                        "message": "Initial version",
                        "assets": {},
                        "changes": [],
                    }
                ],
            }
        )
    )

    # Collection-level .portolan (should be excluded)
    coll_portolan = collection_dir / ".portolan"
    coll_portolan.mkdir()
    (coll_portolan / "metadata.yaml").write_text("title: Test Collection")

    return catalog_root


# =============================================================================
# Tests for _discover_catalog_files
# =============================================================================


class TestDiscoverCatalogFiles:
    """Tests for discovering all catalog files for sync."""

    @pytest.mark.unit
    def test_discovers_style_json(self, catalog_with_metadata: Path) -> None:
        """style.json files should be discovered for sync."""
        from portolan_cli.push import _discover_catalog_files

        files = _discover_catalog_files(
            catalog_with_metadata,
            collection="collection1",
        )

        style_files = [f for f in files if f.name == "style.json"]
        assert len(style_files) == 1
        assert style_files[0] == catalog_with_metadata / "collection1" / "style.json"

    @pytest.mark.unit
    def test_discovers_thumbnail_files(self, catalog_with_metadata: Path) -> None:
        """Thumbnail files (*.thumb.*) should be discovered for sync."""
        from portolan_cli.push import _discover_catalog_files

        files = _discover_catalog_files(
            catalog_with_metadata,
            collection="collection1",
        )

        thumb_files = [f for f in files if ".thumb." in f.name]
        assert len(thumb_files) == 1
        assert thumb_files[0].name == "collection1.thumb.png"

    @pytest.mark.unit
    def test_excludes_portolan_directory(self, catalog_with_metadata: Path) -> None:
        """.portolan/ directories should be excluded by default."""
        from portolan_cli.push import _discover_catalog_files

        files = _discover_catalog_files(
            catalog_with_metadata,
            collection="collection1",
        )

        portolan_files = [f for f in files if ".portolan" in str(f)]
        assert len(portolan_files) == 0

    @pytest.mark.unit
    def test_excludes_env_file(self, catalog_with_metadata: Path) -> None:
        """.env files should be excluded by default."""
        from portolan_cli.push import _discover_catalog_files

        files = _discover_catalog_files(
            catalog_with_metadata,
            collection="collection1",
            include_catalog_root=True,
        )

        env_files = [f for f in files if f.name == ".env"]
        assert len(env_files) == 0

    @pytest.mark.unit
    def test_excludes_pycache(self, catalog_with_metadata: Path) -> None:
        """__pycache__/ directories should be excluded by default."""
        from portolan_cli.push import _discover_catalog_files

        files = _discover_catalog_files(
            catalog_with_metadata,
            collection="collection1",
            include_catalog_root=True,
        )

        pycache_files = [f for f in files if "__pycache__" in str(f)]
        assert len(pycache_files) == 0

    @pytest.mark.unit
    def test_excludes_versioned_assets(self, catalog_with_metadata: Path) -> None:
        """Versioned assets (in versions.json) should NOT be in metadata files.

        These are handled separately by the existing asset upload logic.
        """
        from portolan_cli.push import _discover_catalog_files

        files = _discover_catalog_files(
            catalog_with_metadata,
            collection="collection1",
        )

        # versions.json itself should not be in the list (uploaded separately)
        versions_files = [f for f in files if f.name == "versions.json"]
        assert len(versions_files) == 0

    @pytest.mark.unit
    def test_custom_exclude_patterns(self, catalog_with_metadata: Path) -> None:
        """Custom exclude patterns should be respected."""
        from portolan_cli.push import _discover_catalog_files

        # Add a file that matches a custom pattern
        (catalog_with_metadata / "collection1" / "debug.log").write_text("debug info")

        files = _discover_catalog_files(
            catalog_with_metadata,
            collection="collection1",
            exclude_patterns=["*.log"],
        )

        log_files = [f for f in files if f.suffix == ".log"]
        assert len(log_files) == 0

    @pytest.mark.unit
    def test_returns_relative_paths_structure(self, catalog_with_metadata: Path) -> None:
        """Discovered files should be absolute paths for upload."""
        from portolan_cli.push import _discover_catalog_files

        files = _discover_catalog_files(
            catalog_with_metadata,
            collection="collection1",
        )

        # All paths should be absolute
        for f in files:
            assert f.is_absolute()

    @pytest.mark.unit
    def test_include_catalog_root_files(self, catalog_with_metadata: Path) -> None:
        """When include_catalog_root=True, include root-level metadata.

        Note: catalog.json and README.md are NOT included because they're
        handled separately by _push_all_upload_root_files for atomicity.
        """
        from portolan_cli.push import _discover_catalog_files

        files = _discover_catalog_files(
            catalog_with_metadata,
            collection="collection1",
            include_catalog_root=True,
        )

        # Should include root-level metadata files (not catalog.json/README.md)
        root_style = [f for f in files if f.name == "root-style.json"]
        assert len(root_style) == 1
        assert root_style[0] == catalog_with_metadata / "root-style.json"

        # catalog.json and README.md should NOT be included (handled separately)
        catalog_json = [f for f in files if f.name == "catalog.json"]
        assert len(catalog_json) == 0
        readme = [f for f in files if f.name == "README.md"]
        assert len(readme) == 0


# =============================================================================
# Tests for config integration
# =============================================================================


class TestPushExcludeConfig:
    """Tests for push.exclude configuration."""

    @pytest.mark.unit
    def test_default_exclude_patterns(self) -> None:
        """Default exclusion patterns should include common non-syncable files."""
        from portolan_cli.config import DEFAULT_SETTINGS

        defaults = DEFAULT_SETTINGS.get("push.exclude", [])

        # These should be in defaults
        assert ".portolan/" in defaults or ".portolan/*" in defaults
        assert ".env" in defaults
        assert "__pycache__/" in defaults or "__pycache__/*" in defaults
        assert "*.py" in defaults
        assert ".git/" in defaults or ".git/*" in defaults

    @pytest.mark.unit
    def test_exclude_patterns_from_config(self, tmp_path: Path) -> None:
        """Exclusion patterns should be loadable from config.yaml."""
        from portolan_cli.config import get_setting

        # Create config with custom exclusions
        portolan_dir = tmp_path / ".portolan"
        portolan_dir.mkdir()
        (portolan_dir / "config.yaml").write_text(
            """
push:
  exclude:
    - "*.backup"
    - "temp/"
"""
        )

        patterns = get_setting("push.exclude", catalog_path=tmp_path)
        assert "*.backup" in patterns
        assert "temp/" in patterns


# =============================================================================
# Tests for metadata sync in push flow
# =============================================================================


class TestPushMetadataSync:
    """Tests for metadata sync phase in push."""

    @pytest.mark.unit
    def test_upload_metadata_files_async_uploads_style_json(
        self, catalog_with_metadata: Path
    ) -> None:
        """_upload_metadata_files_async should upload style.json files."""
        import asyncio

        from portolan_cli.push import _upload_metadata_files_async

        uploaded_keys: list[str] = []

        async def mock_put_async(store, key, content):
            uploaded_keys.append(key)

        with patch("portolan_cli.push.obs.put_async", side_effect=mock_put_async):
            mock_store = MagicMock()

            count, errors = asyncio.run(
                _upload_metadata_files_async(
                    mock_store,
                    catalog_with_metadata,
                    "prefix",
                    "collection1",
                    include_catalog_root=False,
                    concurrency=10,
                )
            )

        # Should have uploaded style.json and thumbnail
        assert count >= 2
        assert any("style.json" in k for k in uploaded_keys)
        assert any(".thumb." in k for k in uploaded_keys)
        assert len(errors) == 0

    @pytest.mark.unit
    def test_push_dry_run_shows_metadata_files(self, catalog_with_metadata: Path, capsys) -> None:
        """Dry run should list metadata files that would be synced."""
        from portolan_cli.push import push

        push(
            catalog_root=catalog_with_metadata,
            collection="collection1",
            destination="s3://test-bucket/catalog",
            dry_run=True,
        )

        captured = capsys.readouterr()
        # Should mention style.json or metadata files
        assert "style.json" in captured.out or "metadata" in captured.out.lower()

    @pytest.mark.unit
    def test_discover_catalog_files_finds_metadata(self, catalog_with_metadata: Path) -> None:
        """_discover_catalog_files should find style.json and thumbnails."""
        from portolan_cli.push import _discover_catalog_files

        files = _discover_catalog_files(
            catalog_with_metadata,
            collection="collection1",
        )

        names = [f.name for f in files]
        assert "style.json" in names
        assert "collection1.thumb.png" in names


# =============================================================================
# Tests for push_all_collections metadata sync
# =============================================================================


class TestPushAllMetadataSync:
    """Tests for metadata sync in push_all_collections."""

    @pytest.mark.unit
    def test_discover_root_metadata_files(self, catalog_with_metadata: Path) -> None:
        """_discover_root_metadata_files should find root-level metadata."""
        from portolan_cli.push import _discover_root_metadata_files

        files = _discover_root_metadata_files(catalog_with_metadata)

        # Should include root-style.json (added in fixture)
        names = [f.name for f in files]
        assert "root-style.json" in names

        # Should NOT include handled-separately files
        assert "catalog.json" not in names
        assert "README.md" not in names

    @pytest.mark.unit
    def test_push_all_root_files_dry_run_shows_metadata(
        self, catalog_with_metadata: Path, capsys
    ) -> None:
        """_push_all_upload_root_files dry run should show metadata files."""
        from portolan_cli.push import _push_all_upload_root_files

        stats: dict[str, Any] = {
            "failed": 0,
            "successful": 1,  # At least one collection succeeded
            "total_files": 0,
            "total_versions": 0,
            "errors": {},
        }

        result = _push_all_upload_root_files(
            catalog_root=catalog_with_metadata,
            destination="s3://test-bucket/catalog",
            profile=None,
            region=None,
            dry_run=True,
            stats=stats,
        )

        assert result is True
        captured = capsys.readouterr()
        # Should mention root metadata files
        assert "root-style.json" in captured.out or "metadata" in captured.out.lower()
