"""Tests for intermediate catalog.json / README.md upload on push (Issue #547, #552).

Per ADR-0032 (nested catalogs, flat collections), a nested collection like
``tst/latest/adm0`` has a ``catalog.json`` at each intermediate level
(``tst/`` and ``tst/latest/``) created by ``create_intermediate_catalogs``
during ``add``. These files were never uploaded by ``push`` -- neither the
single-collection path nor the catalog-wide path -- breaking STAC navigation for
any client walking ``child`` links remotely. These tests pin the fix.
"""

from __future__ import annotations

import json
from pathlib import Path
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from portolan_cli.sync.push import (
    PushResult,
    _discover_intermediate_catalog_files,
    _discover_stac_files,
    push,
    push_all_collections,
)

pytestmark = pytest.mark.unit


def _setup_valid_catalog(catalog_root: Path) -> None:
    """Create the .portolan/config.yaml sentinel required by discover_collections."""
    portolan_dir = catalog_root / ".portolan"
    portolan_dir.mkdir(parents=True, exist_ok=True)
    (portolan_dir / "config.yaml").write_text("version: '1.0'\n")


def _write_catalog_json(path: Path, catalog_id: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(
        json.dumps(
            {
                "type": "Catalog",
                "id": catalog_id,
                "stac_version": "1.1.0",
                "description": f"Catalog: {catalog_id}",
                "links": [],
            }
        )
    )


def _write_leaf_collection(catalog_root: Path, collection_id: str) -> None:
    collection_dir = catalog_root / collection_id
    collection_dir.mkdir(parents=True, exist_ok=True)
    (collection_dir / "versions.json").write_text(
        json.dumps({"versions": [{"version": "v1", "assets": {}}]})
    )
    (collection_dir / "collection.json").write_text(
        json.dumps(
            {
                "type": "Collection",
                "id": collection_id,
                "stac_version": "1.1.0",
                "description": f"Collection {collection_id}",
                "license": "proprietary",
                "extent": {
                    "spatial": {"bbox": [[-180, -90, 180, 90]]},
                    "temporal": {"interval": [[None, None]]},
                },
                "links": [],
            }
        )
    )


@pytest.fixture
def nested_catalog(tmp_path: Path) -> Path:
    """Two sibling leaf collections under a two-level nesting (structure from #552).

    Layout::

        catalog.json                    (root)
        tst/catalog.json                (intermediate, level 1)
        tst/README.md                   (intermediate authored readme)
        tst/latest/catalog.json         (intermediate, level 2)
        tst/latest/adm0/{collection,versions}.json
        tst/latest/adm1/{collection,versions}.json
    """
    _setup_valid_catalog(tmp_path)
    _write_catalog_json(tmp_path / "catalog.json", "root")
    _write_catalog_json(tmp_path / "tst" / "catalog.json", "tst")
    (tmp_path / "tst" / "README.md").write_text("# tst\n\nSub-catalog readme.\n")
    _write_catalog_json(tmp_path / "tst" / "latest" / "catalog.json", "tst/latest")
    _write_leaf_collection(tmp_path, "tst/latest/adm0")
    _write_leaf_collection(tmp_path, "tst/latest/adm1")
    return tmp_path


class TestDiscoverIntermediateCatalogFiles:
    """Unit tests for the discovery helper."""

    def test_discovers_deduped_sorted_intermediate_files(self, nested_catalog: Path) -> None:
        """Two sibling collections share ancestors; result is deduped + sorted."""
        files = _discover_intermediate_catalog_files(
            nested_catalog, ["tst/latest/adm0", "tst/latest/adm1"]
        )
        rel = [f.relative_to(nested_catalog).as_posix() for f in files]

        # tst/ and tst/latest/ each appear once despite two sibling collections;
        # the authored tst/README.md is included, and root catalog.json is NOT.
        # Sorted by ancestor dir; within a dir, catalog.json precedes README.md.
        assert rel == [
            "tst/catalog.json",
            "tst/README.md",
            "tst/latest/catalog.json",
        ]

    def test_single_level_collection_has_no_intermediates(self, tmp_path: Path) -> None:
        """A flat collection id yields no intermediate files."""
        _setup_valid_catalog(tmp_path)
        _write_catalog_json(tmp_path / "catalog.json", "root")
        _write_leaf_collection(tmp_path, "demographics")
        assert _discover_intermediate_catalog_files(tmp_path, ["demographics"]) == []


class TestCatalogWidePushIntermediateCatalogs:
    """push_all_collections must upload intermediate catalog.json (Issue #547, #552)."""

    @patch("portolan_cli.sync.push.obs.put")
    @patch("portolan_cli.sync.push.push_async", new_callable=AsyncMock)
    def test_uploads_intermediate_catalogs(
        self, mock_push: MagicMock, mock_obs_put: MagicMock, nested_catalog: Path
    ) -> None:
        mock_push.return_value = PushResult(
            success=True, files_uploaded=1, versions_pushed=1, conflicts=[], errors=[]
        )

        result = push_all_collections(
            catalog_root=nested_catalog,
            destination="s3://bucket/catalog",
            force=False,
            dry_run=False,
            profile=None,
        )

        assert result.success is True
        uploaded_keys = [c.args[1] for c in mock_obs_put.call_args_list]

        assert "catalog/tst/catalog.json" in uploaded_keys, uploaded_keys
        assert "catalog/tst/latest/catalog.json" in uploaded_keys, uploaded_keys
        assert "catalog/tst/README.md" in uploaded_keys, uploaded_keys

    @patch("portolan_cli.sync.push.obs.put")
    @patch("portolan_cli.sync.push.push_async", new_callable=AsyncMock)
    def test_versions_json_uploaded_after_intermediate_catalogs(
        self, mock_push: MagicMock, mock_obs_put: MagicMock, nested_catalog: Path
    ) -> None:
        """Manifest-last: root versions.json uploads after intermediate catalogs."""
        # Root versions.json is required for the manifest-last upload to fire.
        (nested_catalog / "versions.json").write_text(
            json.dumps({"schema_version": "1.0.0", "collections": {}})
        )
        mock_push.return_value = PushResult(
            success=True, files_uploaded=1, versions_pushed=1, conflicts=[], errors=[]
        )

        push_all_collections(
            catalog_root=nested_catalog,
            destination="s3://bucket/catalog",
            force=False,
            dry_run=False,
            profile=None,
        )

        uploaded_keys = [c.args[1] for c in mock_obs_put.call_args_list]
        versions_idx = uploaded_keys.index("catalog/versions.json")
        inter_idx = uploaded_keys.index("catalog/tst/latest/catalog.json")
        assert versions_idx > inter_idx, uploaded_keys


class TestCatalogWideDryRun:
    """Dry-run must report the intermediate catalogs it would upload."""

    @patch("portolan_cli.sync.push.push_async", new_callable=AsyncMock)
    def test_dry_run_lists_intermediate_catalogs(
        self, mock_push: MagicMock, nested_catalog: Path, capsys: pytest.CaptureFixture[str]
    ) -> None:
        mock_push.return_value = PushResult(
            success=True, files_uploaded=0, versions_pushed=0, conflicts=[], errors=[], dry_run=True
        )

        push_all_collections(
            catalog_root=nested_catalog,
            destination="s3://bucket/catalog",
            force=False,
            dry_run=True,
            profile=None,
        )

        out = capsys.readouterr().out
        assert "tst/catalog.json" in out
        assert "tst/latest/catalog.json" in out


class TestSingleCollectionIntermediateCatalogs:
    """Single-collection push (include_catalog=True) also covers its ancestors."""

    def test_discover_stac_files_includes_intermediate_catalogs(self, nested_catalog: Path) -> None:
        stac_files = _discover_stac_files(nested_catalog, "tst/latest/adm0", include_catalog=True)
        catalog_rel = {p.relative_to(nested_catalog).as_posix() for p in stac_files["catalog"]}
        readme_rel = {p.relative_to(nested_catalog).as_posix() for p in stac_files["readmes"]}

        assert "catalog.json" in catalog_rel  # root
        assert "tst/catalog.json" in catalog_rel
        assert "tst/latest/catalog.json" in catalog_rel
        assert "tst/README.md" in readme_rel

    def test_dry_run_single_collection_lists_intermediates(
        self, nested_catalog: Path, capsys: pytest.CaptureFixture[str]
    ) -> None:
        push(
            catalog_root=nested_catalog,
            collection="tst/latest/adm0",
            destination="s3://bucket/catalog",
            dry_run=True,
        )
        out = capsys.readouterr().out
        assert "tst/catalog.json" in out
        assert "tst/latest/catalog.json" in out
