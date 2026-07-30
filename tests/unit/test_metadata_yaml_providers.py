"""Validation for the metadata.yaml providers block (issue #684).

``portolan metadata validate`` should catch a malformed providers array before
generation does, so the human sees the problem next to the file they wrote
rather than as a PTL-PRV finding from ``portolan check``.
"""

from __future__ import annotations

from typing import Any

import pytest

from portolan_cli.metadata_yaml import validate_metadata

pytestmark = pytest.mark.unit


def _metadata(**overrides: Any) -> dict[str, Any]:
    base: dict[str, Any] = {
        "contact": {"name": "GIS Office", "email": "gis@example.org"},
        "license": "CC-BY-4.0",
    }
    base.update(overrides)
    return base


def _provider_errors(providers: Any) -> list[str]:
    return [e for e in validate_metadata(_metadata(providers=providers)) if "provider" in e]


class TestValidProviders:
    def test_a_producer_and_a_host_pass(self) -> None:
        assert (
            validate_metadata(
                _metadata(
                    providers=[
                        {"name": "INDEC", "roles": ["producer"], "url": "https://indec.gob.ar"},
                        {
                            "name": "Source Cooperative",
                            "roles": ["host"],
                            "url": "https://source.coop",
                        },
                    ]
                )
            )
            == []
        )

    def test_one_organization_holding_both_roles_passes(self) -> None:
        assert (
            validate_metadata(
                _metadata(
                    providers=[
                        {
                            "name": "City GIS",
                            "roles": ["producer", "host"],
                            "email": "gis@example.org",
                        }
                    ]
                )
            )
            == []
        )

    def test_a_producer_without_a_host_passes(self) -> None:
        """The host is seeded from contact, so leaving it out is legitimate."""
        assert (
            validate_metadata(_metadata(providers=[{"name": "INDEC", "roles": ["producer"]}])) == []
        )

    def test_licensor_and_processor_roles_pass(self) -> None:
        assert (
            validate_metadata(
                _metadata(
                    providers=[
                        {"name": "INDEC", "roles": ["producer", "licensor"]},
                        {"name": "Consultancy", "roles": ["processor"]},
                        {"name": "Host Org", "roles": ["host"], "url": "https://host.example"},
                    ]
                )
            )
            == []
        )

    def test_an_absent_providers_block_passes(self) -> None:
        assert validate_metadata(_metadata()) == []


class TestInvalidProviders:
    def test_rejects_a_non_list(self) -> None:
        assert _provider_errors("INDEC") == ["Field 'providers' must be a list"]

    def test_rejects_a_non_mapping_entry(self) -> None:
        errors = _provider_errors(["INDEC"])
        assert errors == ["Field 'providers[0]' must be a mapping with a 'name'"]

    def test_rejects_a_blank_name(self) -> None:
        errors = _provider_errors([{"name": "  ", "roles": ["producer"]}])
        assert errors == ["Field 'providers[0].name' cannot be empty"]

    def test_rejects_an_unknown_role(self) -> None:
        errors = _provider_errors([{"name": "Someone", "roles": ["publisher"]}])
        assert len(errors) == 1
        assert "publisher" in errors[0]
        assert "producer" in errors[0]

    def test_rejects_roles_that_are_not_a_list(self) -> None:
        errors = _provider_errors([{"name": "Someone", "roles": "producer"}])
        assert errors == ["Field 'providers[0].roles' must be a list"]

    def test_rejects_two_hosts(self) -> None:
        errors = _provider_errors(
            [
                {"name": "Host A", "roles": ["host"], "url": "https://a.example"},
                {"name": "Host B", "roles": ["host"], "url": "https://b.example"},
            ]
        )
        assert len(errors) == 1
        assert "Exactly one provider may carry the host role" in errors[0]

    def test_rejects_a_host_with_neither_url_nor_email(self) -> None:
        errors = _provider_errors([{"name": "Host Org", "roles": ["host"]}])
        assert len(errors) == 1
        assert "url" in errors[0]
        assert "email" in errors[0]

    def test_rejects_an_invalid_host_email(self) -> None:
        errors = _provider_errors(
            [{"name": "Host Org", "roles": ["host"], "email": "not-an-email"}]
        )
        assert len(errors) == 1
        assert "not-an-email" in errors[0]

    def test_reports_the_index_of_the_offending_entry(self) -> None:
        errors = _provider_errors(
            [
                {"name": "INDEC", "roles": ["producer"]},
                {"name": "Someone", "roles": ["publisher"]},
            ]
        )
        assert len(errors) == 1
        assert "providers[1]" in errors[0]
