"""A catalog carrying a logo still passes rashid (issue #654, PORTO-CORE-074..077).

The link is optional, so nothing else in the suite exercises it against the
validator. This builds the smallest catalog that has one and runs rashid's
metadata and structural passes over it: a malformed ``rel="icon"`` link — an
absolute href, a missing ``type`` — would surface here.
"""

from __future__ import annotations

import json
from pathlib import Path

import pytest
from click.testing import CliRunner
from rashid import Severity, validate

from portolan_cli.cli import cli

pytestmark = pytest.mark.integration

# A 1x1 transparent PNG, small enough to inline and real enough to copy.
PNG_BYTES = bytes.fromhex(
    "89504e470d0a1a0a0000000d4948445200000001000000010806000000"
    "1f15c4890000000a49444154789c6300010000050001"
    "0d0a2db40000000049454e44ae426082"
)


@pytest.fixture
def catalog_with_logo(tmp_path: Path) -> Path:
    source = tmp_path / "brand.png"
    source.write_bytes(PNG_BYTES)
    root = tmp_path / "demo-catalog"

    result = CliRunner().invoke(
        cli,
        [
            "init",
            str(root),
            "--auto",
            "--title",
            "Demo Catalog",
            "--license",
            "CC-BY-4.0",
            "--logo",
            str(source),
            "--logo-title",
            "Demo Agency",
        ],
        catch_exceptions=False,
    )
    assert result.exit_code == 0, result.output
    return root


def test_logo_link_has_the_canonical_shape(catalog_with_logo: Path) -> None:
    data = json.loads((catalog_with_logo / "catalog.json").read_text(encoding="utf-8"))
    icons = [link for link in data["links"] if link.get("rel") == "icon"]
    assert icons == [
        {
            "rel": "icon",
            "href": "./_assets/brand.png",
            "type": "image/png",
            "title": "Demo Agency",
        }
    ]
    assert (catalog_with_logo / "_assets" / "brand.png").read_bytes() == PNG_BYTES


def test_rashid_reports_no_errors(catalog_with_logo: Path) -> None:
    report = validate(catalog_with_logo, structural=True)
    errors = [f.to_dict() for f in report.findings if f.severity is Severity.ERROR]
    assert errors == []
