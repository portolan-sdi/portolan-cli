"""Unit tests for propagating metadata.yaml's license onto a collection (issue #654).

``license`` is a required metadata.yaml field, but the collection it
enriches used to keep the ``other`` default — which rashid rejects unless a
``rel="license"`` link accompanies it (PTL-LIC-002).
"""

from __future__ import annotations

from datetime import datetime, timezone

import pystac
import pytest

from portolan_cli.stac import apply_human_license

pytestmark = pytest.mark.unit


def _collection() -> pystac.Collection:
    return pystac.Collection(
        id="roads",
        description="Roads",
        extent=pystac.Extent(
            spatial=pystac.SpatialExtent([[-1.0, -1.0, 1.0, 1.0]]),
            temporal=pystac.TemporalExtent([[datetime.now(timezone.utc), None]]),
        ),
        license="other",
    )


def _license_links(collection: pystac.Collection) -> list[pystac.Link]:
    return [link for link in collection.links if link.rel == "license"]


class TestApplyHumanLicense:
    def test_applies_an_spdx_identifier(self) -> None:
        collection = _collection()

        apply_human_license(collection, {"license": "CC-BY-4.0"})

        assert collection.license == "CC-BY-4.0"
        assert _license_links(collection) == []

    def test_adds_a_license_link_for_a_non_spdx_license(self) -> None:
        collection = _collection()

        apply_human_license(
            collection,
            {"license": "other", "license_url": "https://example.org/terms"},
        )

        assert collection.license == "other"
        links = _license_links(collection)
        assert len(links) == 1
        assert links[0].href == "https://example.org/terms"

    def test_is_idempotent(self) -> None:
        collection = _collection()
        metadata = {"license": "other", "license_url": "https://example.org/terms"}

        apply_human_license(collection, metadata)
        apply_human_license(collection, metadata)

        assert len(_license_links(collection)) == 1

    def test_ignores_missing_or_blank_values(self) -> None:
        collection = _collection()

        apply_human_license(collection, {"license": "   "})
        apply_human_license(collection, {})
        apply_human_license(collection, "not-a-mapping")

        assert collection.license == "other"
        assert _license_links(collection) == []
