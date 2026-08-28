"""Unit tests for the Portolan profile schema URI declaration (issue #654).

rashid's PTL-CNF-001 makes the versioned profile URI a MUST on every catalog
and collection, and PTL-CNF-002 flags an object whose URI differs from the
root's. ``ensure_portolan_schema_uri`` is the single writer that satisfies both.
"""

from __future__ import annotations

import re

import pytest
from rashid.schema import bundled_schema_versions

from portolan_cli.constants import PORTOLAN_SCHEMA_URI, PORTOLAN_SPEC_VERSION
from portolan_cli.stac import ensure_portolan_schema_uri

pytestmark = pytest.mark.unit

# The pattern rashid matches profile URIs against (rashid.rules.conformance).
RASHID_PATTERN = re.compile(
    r"^https://schemas\.portolan-sdi\.org/portolan/v\d+\.\d+\.\d+/schema\.json$"
)


class TestSchemaUriConstant:
    """The constant is the released spec version in rashid's expected shape."""

    def test_uri_matches_rashid_pattern(self) -> None:
        assert RASHID_PATTERN.match(PORTOLAN_SCHEMA_URI)

    def test_spec_version_is_the_newest_schema_rashid_bundles(self) -> None:
        """The CLI cannot drift from the validator: it reads the version off it."""
        newest = max(bundled_schema_versions())

        assert PORTOLAN_SPEC_VERSION == newest.removeprefix("v")

    def test_spec_version_is_a_bare_semver_triple(self) -> None:
        """No leading ``v``: the URI template and the JSON payload both want digits."""
        assert re.fullmatch(r"\d+\.\d+\.\d+", PORTOLAN_SPEC_VERSION)


class TestEnsurePortolanSchemaUri:
    """The stamper is idempotent, order-preserving, and version-correcting."""

    def test_adds_uri_when_stac_extensions_absent(self) -> None:
        doc: dict[str, object] = {"type": "Catalog", "id": "c"}

        assert ensure_portolan_schema_uri(doc) is True
        assert doc["stac_extensions"] == [PORTOLAN_SCHEMA_URI]

    def test_appends_uri_preserving_existing_order(self) -> None:
        other = "https://stac-extensions.github.io/table/v1.2.0/schema.json"
        doc: dict[str, object] = {"stac_extensions": [other]}

        assert ensure_portolan_schema_uri(doc) is True
        assert doc["stac_extensions"] == [other, PORTOLAN_SCHEMA_URI]

    def test_is_idempotent(self) -> None:
        doc: dict[str, object] = {"stac_extensions": [PORTOLAN_SCHEMA_URI]}

        assert ensure_portolan_schema_uri(doc) is False
        assert doc["stac_extensions"] == [PORTOLAN_SCHEMA_URI]

    def test_replaces_a_stale_profile_version_in_place(self) -> None:
        stale = "https://schemas.portolan-sdi.org/portolan/v0.0.9/schema.json"
        other = "https://stac-extensions.github.io/table/v1.2.0/schema.json"
        doc: dict[str, object] = {"stac_extensions": [stale, other]}

        assert ensure_portolan_schema_uri(doc) is True
        assert doc["stac_extensions"] == [PORTOLAN_SCHEMA_URI, other]

    def test_collapses_duplicate_profile_uris(self) -> None:
        stale = "https://schemas.portolan-sdi.org/portolan/v0.0.9/schema.json"
        doc: dict[str, object] = {"stac_extensions": [stale, PORTOLAN_SCHEMA_URI]}

        assert ensure_portolan_schema_uri(doc) is True
        assert doc["stac_extensions"] == [PORTOLAN_SCHEMA_URI]

    def test_replaces_a_malformed_stac_extensions_value(self) -> None:
        doc: dict[str, object] = {"stac_extensions": "not-a-list"}

        assert ensure_portolan_schema_uri(doc) is True
        assert doc["stac_extensions"] == [PORTOLAN_SCHEMA_URI]

    def test_keeps_a_newer_profile_version(self) -> None:
        """A re-add must not stamp the bundled version over a newer release (issue #755)."""
        newer = _bumped_minor(PORTOLAN_SCHEMA_URI)
        doc: dict[str, object] = {"stac_extensions": [newer]}

        assert ensure_portolan_schema_uri(doc) is False
        assert doc["stac_extensions"] == [newer]

    def test_keeps_a_newer_profile_version_and_collapses_duplicates(self) -> None:
        newer = _bumped_minor(PORTOLAN_SCHEMA_URI)
        other = "https://stac-extensions.github.io/table/v1.2.0/schema.json"
        doc: dict[str, object] = {"stac_extensions": [newer, other, PORTOLAN_SCHEMA_URI]}

        assert ensure_portolan_schema_uri(doc) is True
        assert doc["stac_extensions"] == [newer, other]


def _bumped_minor(uri: str) -> str:
    """Return the profile URI one minor version above the given one."""
    match = re.search(r"/v(\d+)\.(\d+)\.(\d+)/", uri)
    assert match is not None
    major, minor, patch = (int(match.group(i)) for i in (1, 2, 3))
    return re.sub(r"/v\d+\.\d+\.\d+/", f"/v{major}.{minor + 1}.{patch}/", uri)
