"""Tests for README metadata.yaml section rendering.

Tests that source_url, processing_notes, keywords, and attribution
fields from metadata.yaml are properly rendered in the README.
"""

from __future__ import annotations

import pytest

from portolan_cli.readme import (
    MAX_KEYWORD_BADGES,
    _add_attribution_section,
    _add_keywords_section,
    _add_processing_section,
    _add_source_section,
    _is_meaningful_keyword,
    generate_readme,
)


class TestSourceSection:
    """Tests for _add_source_section."""

    @pytest.mark.unit
    def test_adds_source_section_with_url(self) -> None:
        """Source URL should render as a linked section."""
        sections: list[str] = []
        metadata = {"source_url": "https://data.gov/census"}

        _add_source_section(sections, metadata)

        output = "\n".join(sections)
        assert "## Source" in output
        assert "[https://data.gov/census](https://data.gov/census)" in output

    @pytest.mark.unit
    def test_skips_when_no_source_url(self) -> None:
        """No section added when source_url is missing."""
        sections: list[str] = []
        metadata = {}

        _add_source_section(sections, metadata)

        assert len(sections) == 0

    @pytest.mark.unit
    def test_skips_when_source_url_empty(self) -> None:
        """No section added when source_url is empty string."""
        sections: list[str] = []
        metadata = {"source_url": ""}

        _add_source_section(sections, metadata)

        assert len(sections) == 0


class TestProcessingSection:
    """Tests for _add_processing_section."""

    @pytest.mark.unit
    def test_adds_processing_section(self) -> None:
        """Processing notes should render as prose section."""
        sections: list[str] = []
        metadata = {"processing_notes": "Reprojected to EPSG:4326. Simplified geometries."}

        _add_processing_section(sections, metadata)

        output = "\n".join(sections)
        assert "## Processing Notes" in output
        assert "Reprojected to EPSG:4326" in output

    @pytest.mark.unit
    def test_skips_when_no_processing_notes(self) -> None:
        """No section added when processing_notes is missing."""
        sections: list[str] = []
        metadata = {}

        _add_processing_section(sections, metadata)

        assert len(sections) == 0

    @pytest.mark.unit
    def test_handles_multiline_notes(self) -> None:
        """Multiline processing notes should be preserved."""
        sections: list[str] = []
        metadata = {"processing_notes": "Step 1: Downloaded\nStep 2: Cleaned\nStep 3: Converted"}

        _add_processing_section(sections, metadata)

        output = "\n".join(sections)
        assert "Step 1: Downloaded" in output
        assert "Step 2: Cleaned" in output


class TestKeywordsSection:
    """Tests for _add_keywords_section."""

    @pytest.mark.unit
    def test_adds_keywords_as_badges(self) -> None:
        """Keywords should render as shield.io badges."""
        sections: list[str] = []
        metadata = {"keywords": ["census", "demographics", "population"]}

        _add_keywords_section(sections, metadata)

        output = "\n".join(sections)
        # Should have badge-style images
        assert "![census]" in output
        assert "shields.io" in output

    @pytest.mark.unit
    def test_skips_when_no_keywords(self) -> None:
        """No section added when keywords is missing."""
        sections: list[str] = []
        metadata = {}

        _add_keywords_section(sections, metadata)

        assert len(sections) == 0

    @pytest.mark.unit
    def test_skips_when_keywords_empty_list(self) -> None:
        """No section added when keywords is empty list."""
        sections: list[str] = []
        metadata = {"keywords": []}

        _add_keywords_section(sections, metadata)

        assert len(sections) == 0

    @pytest.mark.unit
    def test_handles_keywords_with_spaces(self) -> None:
        """Keywords with spaces should be URL-encoded in badges."""
        sections: list[str] = []
        metadata = {"keywords": ["land use"]}

        _add_keywords_section(sections, metadata)

        # Spaces should be encoded as %20 or replaced with underscores
        assert "land" in sections[0].lower()

    @pytest.mark.unit
    def test_truncates_long_curated_list(self) -> None:
        """A long but clean list is truncated, not omitted (#515).

        Regression guard: the dump signal is the technical ratio, not the
        length, so a genuinely curated long list still renders (capped).
        """
        sections: list[str] = []
        # 20 clean, meaningful keywords (ratio 0.0).
        metadata = {"keywords": [f"Theme{chr(65 + i)}" for i in range(20)]}

        _add_keywords_section(sections, metadata)

        output = "\n".join(sections)
        assert output.count("shields.io") == MAX_KEYWORD_BADGES

    @pytest.mark.unit
    def test_omits_when_mostly_technical(self) -> None:
        """A junk-dominated list (>60% technical) is omitted entirely (#515)."""
        sections: list[str] = []
        # 4 of 5 are technical (ratio 0.8).
        metadata = {"keywords": ["census", "AP010", "orden:30", "vial_nacional", "DB120"]}

        _add_keywords_section(sections, metadata)

        assert len(sections) == 0

    @pytest.mark.unit
    def test_drops_junk_keeps_meaningful(self) -> None:
        """In a clean-majority list, technical keywords are dropped (#515)."""
        sections: list[str] = []
        # ratio 0.33 technical - not a dump, so render the meaningful ones.
        metadata = {"keywords": ["census", "demographics", "AP010"]}

        _add_keywords_section(sections, metadata)

        output = "\n".join(sections)
        assert "![census]" in output
        assert "![demographics]" in output
        assert "AP010" not in output

    @pytest.mark.unit
    def test_caps_badge_count(self) -> None:
        """No more than MAX_KEYWORD_BADGES badges are rendered (#515)."""
        sections: list[str] = []
        # 14 distinct meaningful keywords: under the dump threshold, over the cap.
        metadata = {"keywords": [f"Theme{chr(65 + i)}" for i in range(14)]}

        _add_keywords_section(sections, metadata)

        output = "\n".join(sections)
        assert output.count("shields.io") == MAX_KEYWORD_BADGES

    @pytest.mark.unit
    def test_omits_when_all_junk(self) -> None:
        """A list of only technical slugs renders nothing (the IGN case) (#515)."""
        sections: list[str] = []
        metadata = {"keywords": ["AP010", "orden:30", "vial_nacional"]}

        _add_keywords_section(sections, metadata)

        assert len(sections) == 0


class TestIsMeaningfulKeyword:
    """Tests for _is_meaningful_keyword (the keyword-quality predicate, #515)."""

    @pytest.mark.unit
    @pytest.mark.parametrize(
        "keyword",
        ["census", "demographics", "Provincia", "land use", "COVID19"],
    )
    def test_keeps_meaningful_keywords(self, keyword: str) -> None:
        """Human-readable discovery terms are kept."""
        assert _is_meaningful_keyword(keyword) is True

    @pytest.mark.unit
    @pytest.mark.parametrize(
        "keyword",
        [
            "AP010",  # FACC code (is_technical_name gap)
            "DB120",
            "BH020",
            "CA010",
            "orden:30",  # STAC summary value
            "ns:Name",  # namespace prefix
            "Foo:Bar",  # colon-bearing, uppercase (is_technical_name gap)
            "vial_nacional",  # snake_case slug
            "vial_AP050",
            "lineas_de_geomorfologia_CA010",  # WFS layer id
            "",  # empty
        ],
    )
    def test_drops_technical_keywords(self, keyword: str) -> None:
        """Technical slugs, codes, and summary values are dropped."""
        assert _is_meaningful_keyword(keyword) is False


class TestAttributionSection:
    """Tests for _add_attribution_section."""

    @pytest.mark.unit
    def test_adds_attribution_section(self) -> None:
        """Attribution should render in footer area."""
        sections: list[str] = []
        metadata = {"attribution": "Data provided by US Census Bureau"}

        _add_attribution_section(sections, metadata)

        output = "\n".join(sections)
        assert "## Attribution" in output
        assert "US Census Bureau" in output

    @pytest.mark.unit
    def test_skips_when_no_attribution(self) -> None:
        """No section added when attribution is missing."""
        sections: list[str] = []
        metadata = {}

        _add_attribution_section(sections, metadata)

        assert len(sections) == 0


class TestGenerateReadmeWithMetadataFields:
    """Integration tests for generate_readme with new metadata fields."""

    @pytest.mark.unit
    def test_includes_all_metadata_sections(self) -> None:
        """generate_readme should include all metadata.yaml fields."""
        stac = {
            "type": "Collection",
            "id": "test-collection",
            "title": "Test Collection",
            "description": "A test",
        }
        metadata = {
            "source_url": "https://example.com/data",
            "processing_notes": "Cleaned and validated",
            "keywords": ["test", "sample"],
            "attribution": "Example Corp",
            "license": "CC-BY-4.0",
        }

        readme = generate_readme(stac=stac, metadata=metadata)

        assert "## Source" in readme
        assert "https://example.com/data" in readme
        assert "## Processing Notes" in readme
        assert "Cleaned and validated" in readme
        assert "![test]" in readme  # keyword badge
        assert "## Attribution" in readme
        assert "Example Corp" in readme

    @pytest.mark.unit
    def test_metadata_sections_order(self) -> None:
        """Metadata sections should appear in consistent order."""
        stac = {"type": "Collection", "id": "test", "title": "Test"}
        metadata = {
            "source_url": "https://example.com",
            "processing_notes": "Notes here",
            "attribution": "Someone",
            "license": "MIT",
        }

        readme = generate_readme(stac=stac, metadata=metadata)

        # Source should come early (after description)
        # Attribution should come late (before footer)
        source_pos = readme.find("## Source")
        attribution_pos = readme.find("## Attribution")
        license_pos = readme.find("## License")

        assert source_pos < attribution_pos
        assert attribution_pos < license_pos or license_pos == -1
