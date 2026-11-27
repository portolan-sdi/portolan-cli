"""Integration tests for clone of a catalog with nested subcatalogs."""

from __future__ import annotations

from typing import TYPE_CHECKING, Any
from unittest.mock import MagicMock, patch

import pytest

if TYPE_CHECKING:
    from collections.abc import Callable
    from pathlib import Path

    from portolan_cli.sync.transport import CloneResult


class TestNestedCatalogCloneIntegration:
    """Integration tests for cloning catalogs with nested subcatalogs."""

    @pytest.mark.integration
    def test_clone_nested_catalog_end_to_end(self, tmp_path: Path) -> None:
        from portolan_cli.sync.transport import clone

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
            patch(
                "portolan_cli.sync.transport._fetch_remote_catalog_json",
                side_effect=mock_fetch_catalog,
            ),
            patch("portolan_cli.sync.transport.init_catalog"),
            patch("portolan_cli.sync.transport.pull", side_effect=mock_pull),
        ):
            result = clone(
                remote_url="s3://bucket/den-haag",
                local_path=target_dir,
                collection=None,  # Discover all
            )

        # Verify clone succeeded
        assert result.success is True

        # Pull uses the full path, not just 'hittekaart'.
        assert "climate/hittekaart" in pulled_collections, (
            f"Expected 'climate/hittekaart' in pulled collections, got: {pulled_collections}. "
            "This means clone would try to pull from the wrong URL!"
        )

    @pytest.mark.integration
    def test_clone_deeply_nested_catalog(self, tmp_path: Path) -> None:
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
            if "netherlands" in remote_url:
                return netherlands_catalog
            if "europe" in remote_url:
                return europe_catalog
            return root_catalog

        result, pulled_collections = _clone_all(tmp_path, mock_fetch)

        assert result.success is True
        # Full nested path must be preserved
        assert pulled_collections == ["europe/netherlands/amsterdam/demographics"]

    @pytest.mark.integration
    def test_clone_mixed_direct_and_nested_collections(self, tmp_path: Path) -> None:
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

        result, pulled_collections = _clone_all(tmp_path, mock_fetch)

        assert result.success is True
        # Both direct and nested paths preserved
        assert set(pulled_collections) == {"quick-data", "organized/nested-data"}


def _clone_all(
    tmp_path: Path, fetch: Callable[..., dict[str, Any]]
) -> tuple[CloneResult, list[str]]:
    """Clone every collection with ``fetch`` as the remote, and record each pull."""
    from portolan_cli.sync.transport import clone

    pulled: list[str] = []

    def mock_pull(remote_url: str, local_root: Path, collection: str, **kwargs: Any) -> MagicMock:
        pulled.append(collection)
        return MagicMock(success=True, files_downloaded=1, remote_version="1.0.0")

    with (
        patch("portolan_cli.sync.transport._fetch_remote_catalog_json", side_effect=fetch),
        patch("portolan_cli.sync.transport.init_catalog"),
        patch("portolan_cli.sync.transport.pull", side_effect=mock_pull),
    ):
        result = clone(remote_url="s3://bucket/catalog", local_path=tmp_path / "cloned")
    return result, pulled
