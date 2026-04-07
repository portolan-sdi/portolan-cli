"""Integration tests for nested catalog clone (Issue #324).

These tests verify that the full clone workflow correctly handles
nested catalog structures where collections are organized under subcatalogs.
"""

from __future__ import annotations

from pathlib import Path
from typing import Any
from unittest.mock import MagicMock, patch

import pytest


class TestNestedCatalogCloneIntegration:
    """Integration tests for cloning catalogs with nested subcatalogs.

    Issue #324: clone fails on nested catalog structures because
    list_remote_collections() only looks one level deep.

    These tests verify the end-to-end clone workflow.
    """

    @pytest.mark.integration
    def test_clone_nested_catalog_end_to_end(self, tmp_path: Path) -> None:
        """Full end-to-end test: clone discovers and pulls nested collections.

        Structure:
            root/
            ├── catalog.json
            └── climate/
                ├── catalog.json (subcatalog)
                └── hittekaart/
                    ├── collection.json
                    └── versions.json

        Expected: clone should find 'climate/hittekaart' and pull it correctly.
        """
        from portolan_cli.sync import clone

        # Root catalog pointing to subcatalog
        root_catalog = {
            "type": "Catalog",
            "id": "den-haag-example",
            "stac_version": "1.0.0",
            "description": "Example nested catalog structure",
            "links": [
                {"rel": "self", "href": "./catalog.json"},
                {"rel": "child", "href": "./climate/catalog.json"},  # subcatalog
            ],
        }

        # Climate subcatalog pointing to collection
        climate_catalog = {
            "type": "Catalog",
            "id": "climate",
            "stac_version": "1.0.0",
            "description": "Climate data collections",
            "links": [
                {"rel": "self", "href": "./catalog.json"},
                {"rel": "parent", "href": "../catalog.json"},
                {"rel": "child", "href": "./hittekaart/collection.json"},
            ],
        }

        def mock_fetch_catalog(remote_url: str, **kwargs: Any) -> dict[str, Any]:
            """Mock catalog fetching to return appropriate catalog based on URL."""
            if "climate" in remote_url and "hittekaart" not in remote_url:
                return climate_catalog
            return root_catalog

        # Track what collections are pulled
        pulled_collections: list[str] = []

        def mock_pull(
            remote_url: str, local_root: Path, collection: str, **kwargs: Any
        ) -> MagicMock:
            """Mock pull to record what collection paths are requested."""
            pulled_collections.append(collection)
            return MagicMock(
                success=True,
                files_downloaded=5,
                remote_version="1.0.0",
            )

        target_dir = tmp_path / "cloned"

        with (
            patch("portolan_cli.sync._fetch_remote_catalog_json", side_effect=mock_fetch_catalog),
            patch("portolan_cli.sync.init_catalog"),
            patch("portolan_cli.sync.pull", side_effect=mock_pull),
        ):
            result = clone(
                remote_url="s3://bucket/den-haag",
                local_path=target_dir,
                collection=None,  # Discover all
            )

        # Verify clone succeeded
        assert result.success is True

        # Verify the FULL path was used for pull (not just 'hittekaart')
        # This is the key fix for Issue #324
        assert "climate/hittekaart" in pulled_collections, (
            f"Expected 'climate/hittekaart' in pulled collections, got: {pulled_collections}. "
            "This means clone would try to pull from the wrong URL!"
        )

    @pytest.mark.integration
    def test_clone_deeply_nested_catalog(self, tmp_path: Path) -> None:
        """Test cloning a deeply nested catalog (3+ levels).

        Structure:
            root/
            └── europe/
                └── netherlands/
                    └── amsterdam/
                        └── demographics/collection.json
        """
        from portolan_cli.sync import clone

        root_catalog = {
            "type": "Catalog",
            "id": "root",
            "links": [{"rel": "child", "href": "./europe/catalog.json"}],
        }
        europe_catalog = {
            "type": "Catalog",
            "id": "europe",
            "links": [{"rel": "child", "href": "./netherlands/catalog.json"}],
        }
        netherlands_catalog = {
            "type": "Catalog",
            "id": "netherlands",
            "links": [{"rel": "child", "href": "./amsterdam/catalog.json"}],
        }
        amsterdam_catalog = {
            "type": "Catalog",
            "id": "amsterdam",
            "links": [{"rel": "child", "href": "./demographics/collection.json"}],
        }

        def mock_fetch(remote_url: str, **kwargs: Any) -> dict[str, Any]:
            if "amsterdam" in remote_url:
                return amsterdam_catalog
            elif "netherlands" in remote_url:
                return netherlands_catalog
            elif "europe" in remote_url:
                return europe_catalog
            return root_catalog

        pulled_collections: list[str] = []

        def mock_pull(
            remote_url: str, local_root: Path, collection: str, **kwargs: Any
        ) -> MagicMock:
            pulled_collections.append(collection)
            return MagicMock(success=True, files_downloaded=1, remote_version="1.0.0")

        with (
            patch("portolan_cli.sync._fetch_remote_catalog_json", side_effect=mock_fetch),
            patch("portolan_cli.sync.init_catalog"),
            patch("portolan_cli.sync.pull", side_effect=mock_pull),
        ):
            result = clone(
                remote_url="s3://bucket/catalog",
                local_path=tmp_path / "cloned",
                collection=None,
            )

        assert result.success is True
        # Full nested path must be preserved
        assert pulled_collections == ["europe/netherlands/amsterdam/demographics"]

    @pytest.mark.integration
    def test_clone_mixed_direct_and_nested_collections(self, tmp_path: Path) -> None:
        """Test catalog with both direct links and nested subcatalogs.

        Structure:
            root/
            ├── quick-data/collection.json  (direct link)
            └── organized/
                └── catalog.json (subcatalog)
                    └── nested-data/collection.json
        """
        from portolan_cli.sync import clone

        root_catalog = {
            "type": "Catalog",
            "id": "root",
            "links": [
                {"rel": "child", "href": "./quick-data/collection.json"},
                {"rel": "child", "href": "./organized/catalog.json"},
            ],
        }
        organized_catalog = {
            "type": "Catalog",
            "id": "organized",
            "links": [{"rel": "child", "href": "./nested-data/collection.json"}],
        }

        def mock_fetch(remote_url: str, **kwargs: Any) -> dict[str, Any]:
            if "organized" in remote_url:
                return organized_catalog
            return root_catalog

        pulled_collections: list[str] = []

        def mock_pull(
            remote_url: str, local_root: Path, collection: str, **kwargs: Any
        ) -> MagicMock:
            pulled_collections.append(collection)
            return MagicMock(success=True, files_downloaded=1, remote_version="1.0.0")

        with (
            patch("portolan_cli.sync._fetch_remote_catalog_json", side_effect=mock_fetch),
            patch("portolan_cli.sync.init_catalog"),
            patch("portolan_cli.sync.pull", side_effect=mock_pull),
        ):
            result = clone(
                remote_url="s3://bucket/catalog",
                local_path=tmp_path / "cloned",
                collection=None,
            )

        assert result.success is True
        # Both direct and nested paths preserved
        assert set(pulled_collections) == {"quick-data", "organized/nested-data"}
