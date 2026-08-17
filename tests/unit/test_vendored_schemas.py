"""The vendored STAC extension schemas stay usable and stay current (issue #746).

rashid validates against the Portolan profile and STAC core. It carries no rule
for extension schemas, so the CLI declared raster v1.1.0 for months with every
gate green (issue #654). The conformance gate closes that by validating emitted
raster items against the published raster v2.0.0 schema, which means keeping the
schema bytes on disk. These tests keep that copy honest: usable offline, tied to
the URI the CLI emits, and checked against the published document nightly.
"""

from __future__ import annotations

import json
import re
import urllib.request
from pathlib import Path
from typing import Any

import pytest

SCHEMA_DIR = Path(__file__).resolve().parents[1] / "fixtures" / "schemas"
RASTER_SCHEMA = SCHEMA_DIR / "raster-v2.0.0.schema.json"


def _load(path: Path) -> dict[str, Any]:
    loaded: dict[str, Any] = json.loads(path.read_text(encoding="utf-8"))
    return loaded


@pytest.mark.unit
class TestVendoredRasterSchema:
    def test_id_matches_the_uri_the_cli_emits(self) -> None:
        """The copy on disk is the schema Portolan actually points items at.

        Bumping ``EXTENSION_URLS["raster"]`` without vendoring the matching
        schema fails here, so the gate can never validate a new version against
        an old document.
        """
        from portolan_cli.stac import EXTENSION_URLS

        declared = EXTENSION_URLS["raster"]
        # The published schema self-identifies with a trailing '#'.
        assert _load(RASTER_SCHEMA)["$id"].rstrip("#") == declared

    def test_every_reference_resolves_inside_the_file(self) -> None:
        """No ``$ref`` leaves the document, so validation needs no network."""
        refs = set(
            re.findall(r'"\$ref"\s*:\s*"([^"]+)"', RASTER_SCHEMA.read_text(encoding="utf-8"))
        )

        assert refs, "expected the raster schema to use internal references"
        external = {ref for ref in refs if not ref.startswith("#/")}
        assert external == set()

    def test_the_schema_itself_is_valid(self) -> None:
        """A corrupt copy fails here rather than as a confusing gate error."""
        import jsonschema

        schema = _load(RASTER_SCHEMA)
        jsonschema.Draft7Validator.check_schema(schema)

    def test_an_item_with_no_raster_field_is_rejected(self) -> None:
        """Pin the v2.0.0 rule that drove the conditional declaration in #744.

        v2.0.0 added an ``anyOf`` requiring at least one ``raster:``-prefixed
        field somewhere in the item. Declaring the extension on a COG that has
        none is a schema failure, which is why ``add_raster_extension`` became
        conditional. If a later vendored version drops that requirement, this
        test turns red and the conditional can be revisited deliberately.
        """
        import jsonschema

        bare_item = {
            "type": "Feature",
            "stac_version": "1.1.0",
            "id": "no-raster-fields",
            "properties": {"datetime": "2026-01-01T00:00:00Z"},
            "geometry": None,
            "links": [],
            "assets": {
                "data": {
                    "href": "./data.tif",
                    "type": "image/tiff; application=geotiff; profile=cloud-optimized",
                    "roles": ["data"],
                }
            },
        }

        with pytest.raises(jsonschema.ValidationError):
            jsonschema.validate(bare_item, _load(RASTER_SCHEMA))


@pytest.mark.network
def test_the_vendored_raster_schema_still_matches_the_published_one() -> None:
    """Refetch the published schema and compare bytes.

    Runs in the nightly network tier. Drift opens a tracking issue there instead
    of breaking pull requests, because a schema changing under a pinned version
    is upstream news, not a regression in this repo.
    """
    from portolan_cli.stac import EXTENSION_URLS

    with urllib.request.urlopen(EXTENSION_URLS["raster"], timeout=30) as response:
        published = response.read()

    assert json.loads(published) == _load(RASTER_SCHEMA), (
        "The published raster schema changed under its pinned version. "
        "Refresh tests/fixtures/schemas/raster-v2.0.0.schema.json and re-read "
        "the diff before trusting the gate."
    )
