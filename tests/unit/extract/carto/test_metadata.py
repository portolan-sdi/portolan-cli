"""Unit tests for Carto metadata extraction."""

from __future__ import annotations

import pytest

from portolan_cli.extract.carto.metadata import extract_carto_metadata

pytestmark = [pytest.mark.unit]


def test_to_extracted_maps_source_type_and_attribution() -> None:
    extracted = extract_carto_metadata(
        "https://phl.carto.com/api/v2/sql", account_name="phl"
    ).to_extracted()
    assert extracted.source_type == "carto"
    assert extracted.source_url == "https://phl.carto.com/api/v2/sql"
    assert extracted.attribution == "Carto account: phl"


def test_to_extracted_without_account_has_no_attribution() -> None:
    extracted = extract_carto_metadata("https://x.carto.com/api/v2/sql").to_extracted()
    assert extracted.source_type == "carto"
    assert extracted.attribution is None
