"""Unit tests for the provider model and the provenance it derives (issue #684).

The spec requires every collection to name a producer and exactly one host,
listed last. ``resolve_providers`` turns the ``providers`` array a human wrote in
metadata.yaml into that shape, seeding the host from ``contact`` when the human
left it out. ``derive_provenance`` then reads official-vs-mirror back off the
result, matching rashid's ``provenance_of`` so generation and validation agree.
"""

from __future__ import annotations

import pytest

from portolan_cli.errors import InvalidProvidersError
from portolan_cli.providers import derive_provenance, resolve_providers

pytestmark = pytest.mark.unit


CONTACT = {"contact": {"name": "GIS Office", "email": "gis@example.org"}}


def _names(providers: list[dict[str, object]]) -> list[str]:
    return [str(p["name"]) for p in providers]


def _roles_of(providers: list[dict[str, object]], name: str) -> list[str]:
    for provider in providers:
        if provider["name"] == name:
            roles = provider.get("roles")
            return list(roles) if isinstance(roles, list) else []
    raise AssertionError(f"no provider named {name!r} in {_names(providers)}")


class TestResolveProviders:
    def test_returns_none_when_nothing_is_declared(self) -> None:
        """No providers and no contact leaves authorship to the human (PTL-PRV-001)."""
        assert resolve_providers({}) is None
        assert resolve_providers("not-a-mapping") is None

    def test_moves_the_host_last_regardless_of_written_order(self) -> None:
        """PTL-PRV-002 wants the host last, so the human's order cannot break it."""
        providers = resolve_providers(
            {
                "providers": [
                    {"name": "Source Cooperative", "roles": ["host"], "url": "https://source.coop"},
                    {"name": "INDEC", "roles": ["producer"], "url": "https://indec.gob.ar"},
                ]
            }
        )

        assert providers is not None
        assert _names(providers) == ["INDEC", "Source Cooperative"]

    def test_keeps_declared_order_among_non_hosts(self) -> None:
        providers = resolve_providers(
            {
                "providers": [
                    {"name": "Host Org", "roles": ["host"], "url": "https://host.example"},
                    {"name": "Producer Org", "roles": ["producer"]},
                    {"name": "Processor Org", "roles": ["processor"]},
                ]
            }
        )

        assert providers is not None
        assert _names(providers) == ["Producer Org", "Processor Org", "Host Org"]

    def test_preserves_licensor_and_processor_roles(self) -> None:
        """The spec says these SHOULD appear where they apply, so they survive."""
        providers = resolve_providers(
            {
                "providers": [
                    {
                        "name": "INDEC",
                        "roles": ["producer", "licensor"],
                        "url": "https://indec.gob.ar",
                    },
                    {"name": "Host Org", "roles": ["host"], "email": "ops@host.example"},
                ]
            }
        )

        assert providers is not None
        assert _roles_of(providers, "INDEC") == ["producer", "licensor"]

    def test_one_organization_may_hold_producer_and_host(self) -> None:
        providers = resolve_providers(
            {
                "providers": [
                    {
                        "name": "City GIS",
                        "roles": ["producer", "host"],
                        "url": "https://gis.example",
                    }
                ]
            }
        )

        assert providers is not None
        assert _names(providers) == ["City GIS"]
        assert _roles_of(providers, "City GIS") == ["producer", "host"]

    def test_seeds_the_host_from_contact_when_none_is_declared(self) -> None:
        """contact is already required, and the spec calls host the maintainer contact."""
        providers = resolve_providers(
            {"providers": [{"name": "INDEC", "roles": ["producer"]}], **CONTACT}
        )

        assert providers is not None
        assert _names(providers) == ["INDEC", "GIS Office"]
        assert _roles_of(providers, "GIS Office") == ["host"]
        assert providers[-1]["email"] == "gis@example.org"

    def test_seeds_the_host_from_a_bare_contact_email_string(self) -> None:
        providers = resolve_providers(
            {
                "providers": [{"name": "INDEC", "roles": ["producer"]}],
                "contact": "data@example.org",
            }
        )

        assert providers is not None
        assert providers[-1]["name"] == "data@example.org"
        assert providers[-1]["email"] == "data@example.org"

    def test_a_declared_host_wins_over_contact(self) -> None:
        providers = resolve_providers(
            {
                "providers": [
                    {"name": "INDEC", "roles": ["producer"]},
                    {"name": "Declared Host", "roles": ["host"], "url": "https://declared.example"},
                ],
                **CONTACT,
            }
        )

        assert providers is not None
        assert _names(providers) == ["INDEC", "Declared Host"]

    def test_contact_alone_yields_a_host_only_array(self) -> None:
        """PTL-PRV-001 still fires, but PTL-PRV-002 and -003 are satisfied."""
        providers = resolve_providers(dict(CONTACT))

        assert providers == [{"name": "GIS Office", "roles": ["host"], "email": "gis@example.org"}]

    def test_rejects_two_declared_hosts(self) -> None:
        """Nothing can pick between them, so say so instead of emitting both."""
        with pytest.raises(InvalidProvidersError, match="exactly one"):
            resolve_providers(
                {
                    "providers": [
                        {"name": "Host A", "roles": ["host"], "url": "https://a.example"},
                        {"name": "Host B", "roles": ["host"], "url": "https://b.example"},
                    ]
                }
            )

    def test_rejects_an_unknown_role(self) -> None:
        with pytest.raises(InvalidProvidersError, match="publisher"):
            resolve_providers({"providers": [{"name": "Someone", "roles": ["publisher"]}]})

    def test_rejects_a_nameless_provider(self) -> None:
        with pytest.raises(InvalidProvidersError, match="name"):
            resolve_providers({"providers": [{"name": "   ", "roles": ["producer"]}]})

    def test_ignores_a_providers_value_that_is_not_a_list(self) -> None:
        assert resolve_providers({"providers": "INDEC", **CONTACT}) == [
            {"name": "GIS Office", "roles": ["host"], "email": "gis@example.org"}
        ]

    def test_carries_url_description_and_email_through(self) -> None:
        providers = resolve_providers(
            {
                "providers": [
                    {
                        "name": "INDEC",
                        "roles": ["producer"],
                        "url": "https://indec.gob.ar",
                        "description": "National statistics institute",
                    },
                    {"name": "Host Org", "roles": ["host"], "email": "ops@host.example"},
                ]
            }
        )

        assert providers is not None
        assert providers[0]["description"] == "National statistics institute"
        assert providers[0]["url"] == "https://indec.gob.ar"
        assert providers[1]["email"] == "ops@host.example"


class TestDeriveProvenance:
    def test_official_when_producer_and_host_are_the_same_organization(self) -> None:
        providers = [
            {"name": "City GIS", "roles": ["producer", "host"], "url": "https://g.example"}
        ]

        assert derive_provenance(providers) == "official"

    def test_official_across_two_entries_with_the_same_name(self) -> None:
        providers = [
            {"name": "City GIS", "roles": ["producer"]},
            {"name": "city gis", "roles": ["host"], "email": "gis@example.org"},
        ]

        assert derive_provenance(providers) == "official"

    def test_mirror_when_they_differ(self) -> None:
        providers = [
            {"name": "INDEC", "roles": ["producer"]},
            {"name": "Source Cooperative", "roles": ["host"], "url": "https://source.coop"},
        ]

        assert derive_provenance(providers) == "mirror"

    def test_underivable_without_a_producer(self) -> None:
        providers = [{"name": "Host Org", "roles": ["host"], "email": "ops@host.example"}]

        assert derive_provenance(providers) is None

    def test_underivable_without_a_host(self) -> None:
        assert derive_provenance([{"name": "INDEC", "roles": ["producer"]}]) is None

    def test_underivable_for_an_empty_array(self) -> None:
        assert derive_provenance([]) is None
        assert derive_provenance(None) is None
