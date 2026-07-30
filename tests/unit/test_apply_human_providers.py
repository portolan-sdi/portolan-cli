"""Unit tests for putting metadata.yaml's providers on a collection (issue #684).

A generated collection used to declare no providers at all, which rashid rejects
under PTL-PRV-001. Two appliers close that: ``apply_human_providers`` writes the
array, and ``apply_provenance`` reads official-vs-mirror back off it to decide
whether a ``via`` link and an ``updated`` stamp belong on the collection.
"""

from __future__ import annotations

from datetime import datetime, timezone

import pystac
import pytest

from portolan_cli.stac import apply_human_providers, apply_provenance

pytestmark = pytest.mark.unit


SYNCED_AT = datetime(2026, 7, 30, 9, 15, 0, tzinfo=timezone.utc)

MIRROR = {
    "providers": [
        {"name": "INDEC", "roles": ["producer"], "url": "https://indec.gob.ar"},
        {"name": "Source Cooperative", "roles": ["host"], "url": "https://source.coop"},
    ],
    "source_url": "https://indec.gob.ar/descargas",
}

OFFICIAL = {
    "providers": [
        {"name": "City GIS", "roles": ["producer", "host"], "url": "https://gis.example"},
    ],
}


def _collection() -> pystac.Collection:
    return pystac.Collection(
        id="roads",
        description="Roads",
        extent=pystac.Extent(
            spatial=pystac.SpatialExtent([[-1.0, -1.0, 1.0, 1.0]]),
            temporal=pystac.TemporalExtent([[datetime.now(timezone.utc), None]]),
        ),
        license="CC-BY-4.0",
    )


def _links(collection: pystac.Collection, rel: str) -> list[pystac.Link]:
    return [link for link in collection.links if link.rel == rel]


def _provider_dicts(collection: pystac.Collection) -> list[dict[str, object]]:
    return [p.to_dict() for p in collection.providers or []]


class TestApplyHumanProviders:
    def test_writes_the_array_with_the_host_last(self) -> None:
        collection = _collection()

        apply_human_providers(collection, MIRROR)

        assert [p["name"] for p in _provider_dicts(collection)] == [
            "INDEC",
            "Source Cooperative",
        ]

    def test_carries_email_onto_the_host_provider(self) -> None:
        """PTL-PRV-003 accepts an email, which is not a core STAC provider field."""
        collection = _collection()

        apply_human_providers(
            collection,
            {"providers": [{"name": "INDEC", "roles": ["producer"]}], "contact": "d@example.org"},
        )

        assert _provider_dicts(collection)[-1] == {
            "name": "d@example.org",
            "roles": ["host"],
            "email": "d@example.org",
        }

    def test_leaves_providers_unset_when_nothing_is_declared(self) -> None:
        collection = _collection()

        apply_human_providers(collection, {})
        apply_human_providers(collection, "not-a-mapping")

        assert not collection.providers

    def test_does_not_clobber_existing_providers_with_nothing(self) -> None:
        """A re-add with no provider metadata must not wipe what is already there."""
        collection = _collection()
        apply_human_providers(collection, MIRROR)

        apply_human_providers(collection, {})

        assert len(_provider_dicts(collection)) == 2

    def test_is_idempotent(self) -> None:
        collection = _collection()

        apply_human_providers(collection, MIRROR)
        apply_human_providers(collection, MIRROR)

        assert len(_provider_dicts(collection)) == 2


class TestApplyProvenanceForAMirror:
    def test_adds_the_via_link_as_text_html(self) -> None:
        collection = _collection()
        apply_human_providers(collection, MIRROR)

        apply_provenance(collection, MIRROR, synced_at=SYNCED_AT)

        via = _links(collection, "via")
        assert len(via) == 1
        assert via[0].href == "https://indec.gob.ar/descargas"
        assert via[0].media_type == "text/html"

    def test_stamps_updated_as_rfc3339(self) -> None:
        collection = _collection()
        apply_human_providers(collection, MIRROR)

        apply_provenance(collection, MIRROR, synced_at=SYNCED_AT)

        assert collection.extra_fields["updated"] == "2026-07-30T09:15:00Z"

    def test_restamps_updated_on_a_later_sync(self) -> None:
        collection = _collection()
        apply_human_providers(collection, MIRROR)
        apply_provenance(collection, MIRROR, synced_at=SYNCED_AT)

        later = datetime(2026, 8, 1, 0, 0, 0, tzinfo=timezone.utc)
        apply_provenance(collection, MIRROR, synced_at=later)

        assert collection.extra_fields["updated"] == "2026-08-01T00:00:00Z"

    def test_does_not_duplicate_the_via_link(self) -> None:
        collection = _collection()
        apply_human_providers(collection, MIRROR)

        apply_provenance(collection, MIRROR, synced_at=SYNCED_AT)
        apply_provenance(collection, MIRROR, synced_at=SYNCED_AT)

        assert len(_links(collection, "via")) == 1

    def test_stamps_updated_even_without_a_source_url(self) -> None:
        """PTL-PRO-001 stays for the human to answer; PTL-PRO-003 need not."""
        metadata = {"providers": MIRROR["providers"]}
        collection = _collection()
        apply_human_providers(collection, metadata)

        apply_provenance(collection, metadata, synced_at=SYNCED_AT)

        assert _links(collection, "via") == []
        assert collection.extra_fields["updated"] == "2026-07-30T09:15:00Z"


class TestApplyProvenanceForAnOfficialCatalog:
    def test_adds_no_via_link(self) -> None:
        """PTL-PRO-004: an official collection is the source, not a mirror."""
        metadata = {**OFFICIAL, "source_url": "https://gis.example/downloads"}
        collection = _collection()
        apply_human_providers(collection, metadata)

        apply_provenance(collection, metadata, synced_at=SYNCED_AT)

        assert _links(collection, "via") == []

    def test_adds_no_updated_stamp(self) -> None:
        collection = _collection()
        apply_human_providers(collection, OFFICIAL)

        apply_provenance(collection, OFFICIAL, synced_at=SYNCED_AT)

        assert "updated" not in collection.extra_fields


class TestApplyProvenanceWhenUnderivable:
    def test_touches_nothing_without_providers(self) -> None:
        collection = _collection()

        apply_provenance(
            collection,
            {"source_url": "https://example.org/downloads"},
            synced_at=SYNCED_AT,
        )

        assert _links(collection, "via") == []
        assert "updated" not in collection.extra_fields
