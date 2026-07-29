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
from portolan_cli.validation.legacy import detect_legacy_generation

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
