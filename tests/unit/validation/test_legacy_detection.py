"""Detecting catalogs published before Portolan emitted the profile schema URI.

Pre-#654 catalogs never declared the Portolan schema URI and carried
``portolan:`` custom fields instead. They now fail PTL-CNF-001 across the board,
which reads as a broken catalog rather than an outdated one. Detecting the
generation lets `check` say what actually happened and what to run.
"""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any

import pytest

from portolan_cli.constants import PORTOLAN_SCHEMA_URI
from portolan_cli.validation.legacy import (
    detect_legacy_generation,
    detect_legacy_notes,
    detect_removed_fields,
    detect_style_manifest,
)

pytestmark = pytest.mark.unit


def _write(path: Path, doc: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(doc), encoding="utf-8")


def _catalog(**extra: Any) -> dict[str, Any]:
    return {
        "type": "Catalog",
        "stac_version": "1.1.0",
        "id": "demo",
        "description": "Demo.",
        "links": [],
        **extra,
    }


class TestDetectLegacyGeneration:
    def test_conformant_catalog_is_not_legacy(self, tmp_path: Path) -> None:
        _write(tmp_path / "catalog.json", _catalog(stac_extensions=[PORTOLAN_SCHEMA_URI]))
        assert detect_legacy_generation(tmp_path) is None

    def test_missing_catalog_is_not_legacy(self, tmp_path: Path) -> None:
        """No catalog at all is a different failure; PTL-GEN-000 reports it."""
        assert detect_legacy_generation(tmp_path) is None

    def test_unparseable_catalog_is_not_legacy(self, tmp_path: Path) -> None:
        (tmp_path / "catalog.json").write_text("{ not json", encoding="utf-8")
        assert detect_legacy_generation(tmp_path) is None

    def test_portolan_version_field_without_schema_uri_is_legacy(self, tmp_path: Path) -> None:
        _write(tmp_path / "catalog.json", _catalog(**{"portolan:version": "0.0.9"}))
        note = detect_legacy_generation(tmp_path)
        assert note is not None
        assert "portolan check --fix" in note

    def test_collection_flag_makes_the_catalog_legacy(self, tmp_path: Path) -> None:
        _write(tmp_path / "catalog.json", _catalog())
        _write(
            tmp_path / "roads" / "collection.json",
            {
                "type": "Collection",
                "stac_version": "1.1.0",
                "id": "roads",
                "description": "Roads.",
                "license": "CC-BY-4.0",
                "extent": {},
                "links": [],
                "portolan:geospatial": True,
            },
        )
        assert detect_legacy_generation(tmp_path) is not None

    def test_schema_uri_present_wins_over_a_stale_field(self, tmp_path: Path) -> None:
        """A catalog re-fixed but not yet cleaned is current, not legacy."""
        _write(
            tmp_path / "catalog.json",
            _catalog(stac_extensions=[PORTOLAN_SCHEMA_URI], **{"portolan:version": "0.0.9"}),
        )
        assert detect_legacy_generation(tmp_path) is None

    def test_no_portolan_fields_anywhere_is_not_legacy(self, tmp_path: Path) -> None:
        """A plain STAC catalog Portolan never generated is not a legacy catalog."""
        _write(tmp_path / "catalog.json", _catalog())
        assert detect_legacy_generation(tmp_path) is None


def _collection(**extra: Any) -> dict[str, Any]:
    return {
        "type": "Collection",
        "stac_version": "1.1.0",
        "id": "roads",
        "description": "Roads.",
        "license": "CC-BY-4.0",
        "extent": {},
        "links": [],
        **extra,
    }


class TestDetectStyleManifest:
    """The removed portolan:styles property (issue #739).

    No rashid rule can report this: a rule fires on a spec requirement an
    object fails, and the spec no longer names the property at all.
    """

    def test_collection_with_the_manifest_is_flagged(self, tmp_path: Path) -> None:
        _write(tmp_path / "catalog.json", _catalog(stac_extensions=[PORTOLAN_SCHEMA_URI]))
        _write(
            tmp_path / "roads" / "collection.json",
            _collection(**{"portolan:styles": ["styles/default"]}),
        )
        note = detect_style_manifest(tmp_path)
        assert note is not None
        assert "portolan:styles" in note

    def test_nested_collection_is_found(self, tmp_path: Path) -> None:
        """Collections nest under intermediate catalogs at any depth."""
        _write(tmp_path / "catalog.json", _catalog())
        _write(
            tmp_path / "transport" / "roads" / "collection.json",
            _collection(**{"portolan:styles": []}),
        )
        assert detect_style_manifest(tmp_path) is not None

    def test_current_collection_is_not_flagged(self, tmp_path: Path) -> None:
        _write(tmp_path / "catalog.json", _catalog())
        _write(
            tmp_path / "roads" / "collection.json",
            _collection(assets={"styles/default": {"roles": ["style", "default"]}}),
        )
        assert detect_style_manifest(tmp_path) is None

    def test_empty_catalog_is_not_flagged(self, tmp_path: Path) -> None:
        assert detect_style_manifest(tmp_path) is None

    def test_unparseable_collection_is_not_flagged(self, tmp_path: Path) -> None:
        (tmp_path / "roads").mkdir()
        (tmp_path / "roads" / "collection.json").write_text("{ not json", encoding="utf-8")
        assert detect_style_manifest(tmp_path) is None


def _item(**extra: Any) -> dict[str, Any]:
    return {
        "type": "Feature",
        "stac_version": "1.1.0",
        "id": "tile",
        "geometry": None,
        "bbox": [0, 0, 1, 1],
        "properties": {},
        "assets": {},
        "links": [],
        **extra,
    }


class TestDetectRemovedFields:
    """The seven fields Portolan wrote under a prefix the spec defines nothing in.

    Independent of the schema URI: a catalog generated by 0.8.0 declares the
    URI and still carries them, so gating on the URI would report nothing.
    """

    def test_collection_aggregate_is_flagged(self, tmp_path: Path) -> None:
        _write(tmp_path / "catalog.json", _catalog(stac_extensions=[PORTOLAN_SCHEMA_URI]))
        _write(
            tmp_path / "roads" / "collection.json",
            _collection(**{"portolan:asset_count": 2, "portolan:total_size_bytes": 4096}),
        )
        note = detect_removed_fields(tmp_path)
        assert note is not None
        assert "portolan check --fix" in note

    def test_catalog_aggregate_is_flagged(self, tmp_path: Path) -> None:
        _write(
            tmp_path / "catalog.json",
            _catalog(stac_extensions=[PORTOLAN_SCHEMA_URI], **{"portolan:collection_count": 3}),
        )
        assert detect_removed_fields(tmp_path) is not None

    def test_item_marker_is_flagged(self, tmp_path: Path) -> None:
        """portolan:datetime_provisional is the one that lives on an item."""
        _write(tmp_path / "catalog.json", _catalog(stac_extensions=[PORTOLAN_SCHEMA_URI]))
        _write(
            tmp_path / "roads" / "collection.json",
            _collection(links=[{"rel": "item", "href": "./tile/tile.json"}]),
        )
        _write(
            tmp_path / "roads" / "tile" / "tile.json",
            _item(properties={"portolan:datetime_provisional": True}),
        )
        assert detect_removed_fields(tmp_path) is not None

    def test_asset_field_is_flagged(self, tmp_path: Path) -> None:
        """portolan:glob and portolan:managed sit inside an asset, not at the top."""
        _write(tmp_path / "catalog.json", _catalog(stac_extensions=[PORTOLAN_SCHEMA_URI]))
        _write(
            tmp_path / "roads" / "collection.json",
            _collection(
                assets={"data": {"href": "s3://bucket/roads.parquet", "portolan:managed": False}}
            ),
        )
        assert detect_removed_fields(tmp_path) is not None

    def test_current_catalog_is_not_flagged(self, tmp_path: Path) -> None:
        _write(tmp_path / "catalog.json", _catalog(stac_extensions=[PORTOLAN_SCHEMA_URI]))
        _write(
            tmp_path / "roads" / "collection.json",
            _collection(
                assets={"data": {"href": "./roads.parquet", "file:size": 10}},
                links=[{"rel": "item", "href": "./tile/tile.json"}],
            ),
        )
        _write(tmp_path / "roads" / "tile" / "tile.json", _item())
        assert detect_removed_fields(tmp_path) is None

    def test_the_style_manifest_has_its_own_note(self, tmp_path: Path) -> None:
        """portolan:styles carries a different remedy, so this detector leaves it."""
        _write(tmp_path / "catalog.json", _catalog(stac_extensions=[PORTOLAN_SCHEMA_URI]))
        _write(
            tmp_path / "roads" / "collection.json",
            _collection(**{"portolan:styles": ["styles/default"]}),
        )
        assert detect_removed_fields(tmp_path) is None

    def test_unparseable_object_is_not_flagged(self, tmp_path: Path) -> None:
        (tmp_path / "roads").mkdir()
        (tmp_path / "roads" / "collection.json").write_text("{ not json", encoding="utf-8")
        assert detect_removed_fields(tmp_path) is None


class TestDetectLegacyNotes:
    """The detectors are independent, and `check` reports them as one note."""

    def test_current_catalog_has_no_note(self, tmp_path: Path) -> None:
        _write(tmp_path / "catalog.json", _catalog(stac_extensions=[PORTOLAN_SCHEMA_URI]))
        _write(tmp_path / "roads" / "collection.json", _collection())
        assert detect_legacy_notes(tmp_path) is None

    def test_style_manifest_alone_is_reported(self, tmp_path: Path) -> None:
        """A catalog can be current in every other respect and still carry it."""
        _write(tmp_path / "catalog.json", _catalog(stac_extensions=[PORTOLAN_SCHEMA_URI]))
        _write(
            tmp_path / "roads" / "collection.json",
            _collection(**{"portolan:styles": ["styles/default"]}),
        )
        note = detect_legacy_notes(tmp_path)
        assert note is not None
        assert "portolan:styles" in note
        assert "predates" not in note

    def test_both_markers_are_joined(self, tmp_path: Path) -> None:
        _write(tmp_path / "catalog.json", _catalog(**{"portolan:version": "0.0.9"}))
        _write(
            tmp_path / "roads" / "collection.json",
            _collection(**{"portolan:styles": ["styles/default"]}),
        )
        note = detect_legacy_notes(tmp_path)
        assert note is not None
        assert "portolan check --fix" in note
        assert "portolan:styles" in note

    def test_removed_fields_alone_are_reported(self, tmp_path: Path) -> None:
        """A 0.8.0 catalog: current schema URI, seven fields the spec never had."""
        _write(tmp_path / "catalog.json", _catalog(stac_extensions=[PORTOLAN_SCHEMA_URI]))
        _write(
            tmp_path / "roads" / "collection.json",
            _collection(**{"portolan:asset_count": 1}),
        )
        note = detect_legacy_notes(tmp_path)
        assert note is not None
        assert "predates" not in note
        assert "portolan check --fix" in note
