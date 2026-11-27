"""The vendored STAC extension schemas stay usable and stay current."""

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
        import jsonschema

        bare_item: dict[str, object] = {
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
    from portolan_cli.stac import EXTENSION_URLS

    with urllib.request.urlopen(EXTENSION_URLS["raster"], timeout=30) as response:
        published = response.read()

    assert json.loads(published) == _load(RASTER_SCHEMA), (
        "The published raster schema changed under its pinned version. "
        "Refresh tests/fixtures/schemas/raster-v2.0.0.schema.json and re-read "
        "the diff before trusting the gate."
    )
