"""Unit tests for the license model a collection must satisfy (issue #686).

A collection with no license, or with ``other`` and no ``rel="license"`` link,
fails the validator: PTL-LIC-001 and PTL-LIC-002 are both ERROR. ``license_gap``
answers whether generation is about to produce one of those, branch for branch
against ``rashid.rules.license``, so ``portolan add`` refuses the write instead
of shipping a catalog ``portolan check`` then rejects.

Spelling is deliberately out of scope here. rashid keeps its 727-identifier SPDX
list in a private module, so ``portolan check`` owns that judgment and this
module never guesses at it.
"""

from __future__ import annotations

import pytest

from portolan_cli.constants import TODO_MARKER
from portolan_cli.licensing import (
    OTHER_LICENSE,
    PROPRIETARY_LICENSE,
    license_gap,
    license_url_from_text,
    resolve_license,
)

pytestmark = pytest.mark.unit


class TestLicenseGap:
    """One branch per rashid license rule."""

    @pytest.mark.parametrize("missing", [None, "", "   ", "\t\n", 42, [], {}])
    def test_absent_or_blank_is_a_gap(self, missing: object) -> None:
        """PTL-LIC-001 fires on a collection that declares no license at all."""
        gap = license_gap(missing, has_license_link=False)  # type: ignore[arg-type]

        assert gap is not None
        assert "no license is declared" in gap

    def test_a_link_does_not_rescue_a_missing_license(self) -> None:
        """A license link is not a license. PTL-LIC-001 still fires."""
        assert license_gap(None, has_license_link=True) is not None

    @pytest.mark.parametrize(
        "placeholder",
        [TODO_MARKER, "TODO", "todo: add value", "  TODO: Add value  "],
    )
    def test_the_seeded_placeholder_is_a_gap(self, placeholder: str) -> None:
        """extract seeds a TODO marker, which reaches collection.license verbatim."""
        gap = license_gap(placeholder, has_license_link=True)

        assert gap is not None
        assert "placeholder" in gap

    def test_proprietary_is_a_gap(self) -> None:
        """PTL-LIC-003: 'proprietary' is deprecated and must not be used."""
        gap = license_gap(PROPRIETARY_LICENSE, has_license_link=True)

        assert gap is not None
        assert "proprietary" in gap

    def test_other_without_a_link_is_a_gap(self) -> None:
        """PTL-LIC-002, the defect issue #686 reported."""
        gap = license_gap(OTHER_LICENSE, has_license_link=False)

        assert gap is not None
        assert "license_url" in gap

    def test_other_with_a_link_passes(self) -> None:
        """'other' plus a license link is the second conformant shape."""
        assert license_gap(OTHER_LICENSE, has_license_link=True) is None

    @pytest.mark.parametrize(
        "license_id",
        ["CC-BY-4.0", "MIT", "CC0-1.0", "ODbL-1.0", "EUPL-1.2", "  CC-BY-4.0  "],
    )
    def test_an_identifier_passes_without_a_link(self, license_id: str) -> None:
        """An SPDX identifier needs no link."""
        assert license_gap(license_id, has_license_link=False) is None

    def test_spelling_is_judged_against_rashids_list(self) -> None:
        """rashid 0.1.4 published SPDX_LICENSE_IDS, so the gate can check it (issue #727).

        Before this, a misspelled identifier passed the gate and reappeared as
        PTL-LIC-001 under ``portolan check``, after ``add`` had already written
        the collection. Naming the official spelling here is what rashid's hint
        does, so the two commands say the same word.
        """
        gap = license_gap("cc-by-4.0", has_license_link=False)

        assert gap is not None
        assert "CC-BY-4.0" in gap

    def test_a_value_that_is_not_an_identifier_at_all_is_a_gap(self) -> None:
        """No near miss to suggest, so the reason names the two conformant shapes."""
        gap = license_gap("Apache 2.0", has_license_link=False)

        assert gap is not None
        assert "other" in gap

    def test_licenseref_is_a_gap(self) -> None:
        """LicenseRef-* is an SPDX expression construct; rashid's list holds none."""
        gap = license_gap("LicenseRef-CityOfPhiladelphia", has_license_link=False)

        assert gap is not None
        assert "other" in gap

    def test_a_link_does_not_rescue_a_misspelled_identifier(self) -> None:
        """A license link only rescues 'other'. PTL-LIC-001 still fires on a typo."""
        assert license_gap("cc-by-4.0", has_license_link=True) is not None


class TestResolveLicense:
    def test_reads_license_and_url(self) -> None:
        resolved = resolve_license({"license": "other", "license_url": "https://x.org/terms"})

        assert resolved is not None
        assert resolved.license_id == "other"
        assert resolved.license_url == "https://x.org/terms"

    def test_strips_surrounding_whitespace(self) -> None:
        resolved = resolve_license({"license": "  MIT  ", "license_url": "  https://x.org  "})

        assert resolved is not None
        assert resolved.license_id == "MIT"
        assert resolved.license_url == "https://x.org"

    def test_url_is_none_when_absent_or_blank(self) -> None:
        assert resolve_license({"license": "MIT"}).license_url is None  # type: ignore[union-attr]
        assert resolve_license({"license": "MIT", "license_url": ""}).license_url is None  # type: ignore[union-attr]

    @pytest.mark.parametrize("metadata", [{}, {"license": ""}, {"license": None}, "not-a-mapping"])
    def test_returns_none_when_no_license_is_written(self, metadata: object) -> None:
        """Nothing declared means the caller falls back to the existing collection."""
        assert resolve_license(metadata) is None

    def test_does_not_judge_the_value_it_reads(self) -> None:
        """Reading and judging are separate: license_gap owns the verdict."""
        resolved = resolve_license({"license": TODO_MARKER})

        assert resolved is not None
        assert resolved.license_id == TODO_MARKER


class TestLicenseUrlFromText:
    """Harvested license text often carries the license link. Extraction beats invention."""

    def test_pulls_a_bare_url(self) -> None:
        text = "Open Government Licence, see https://data.example.org/licence for terms"

        assert license_url_from_text(text) == "https://data.example.org/licence"

    def test_pulls_the_href_out_of_arcgis_html(self) -> None:
        """ArcGIS licenseInfo is usually an HTML fragment."""
        text = "<a href='https://data.example.org/terms' target='_blank'>Terms of use</a>"

        assert license_url_from_text(text) == "https://data.example.org/terms"

    def test_pulls_the_href_out_of_double_quoted_html(self) -> None:
        text = '<a href="https://data.example.org/terms">Terms</a>'

        assert license_url_from_text(text) == "https://data.example.org/terms"

    def test_drops_trailing_sentence_punctuation(self) -> None:
        assert license_url_from_text("See https://x.org/terms.") == "https://x.org/terms"
        assert license_url_from_text("(https://x.org/terms),") == "https://x.org/terms"

    def test_takes_the_first_url_when_several_appear(self) -> None:
        text = "Licence: https://x.org/licence. Contact: https://x.org/contact"

        assert license_url_from_text(text) == "https://x.org/licence"

    def test_accepts_plain_http(self) -> None:
        assert license_url_from_text("http://x.org/terms") == "http://x.org/terms"

    @pytest.mark.parametrize(
        "text",
        [
            None,
            "",
            "   ",
            "Public domain",
            "Contact gis@example.org for licensing",
            "See www.example.org/terms",
        ],
    )
    def test_returns_none_when_there_is_no_url(self, text: str | None) -> None:
        """No URL means no honest link, so the human has to supply one."""
        assert license_url_from_text(text) is None
