"""Tests for metadata seeding from extracted metadata.

Tests the seed_metadata_yaml function that generates .portolan/metadata.yaml
from ExtractedMetadata, providing a pre-filled starting point for human
enrichment.

Addresses:
- #312: Unified metadata extraction framework
- #316: Author and citation support
"""

from __future__ import annotations

from datetime import date
from pathlib import Path

import pytest
import yaml


class TestSeedMetadataYaml:
    """Tests for the seed_metadata_yaml function."""

    @pytest.mark.unit
    def test_generates_valid_yaml(self, tmp_path: Path) -> None:
        """seed_metadata_yaml generates valid, parseable YAML."""
        from portolan_cli.metadata_extraction import ExtractedMetadata
        from portolan_cli.metadata_seeding import seed_metadata_yaml

        extracted = ExtractedMetadata(
            source_url="https://example.com/api/data",
            source_type="arcgis",
            extraction_date=date(2026, 4, 7),
        )

        output_path = tmp_path / ".portolan" / "metadata.yaml"
        output_path.parent.mkdir(parents=True)

        result = seed_metadata_yaml(extracted, output_path)

        assert result is True
        assert output_path.exists()

        # Should be valid YAML
        content = output_path.read_text()
        parsed = yaml.safe_load(content)
        assert isinstance(parsed, dict)

    @pytest.mark.unit
    def test_includes_provenance_comment(self, tmp_path: Path) -> None:
        """Generated YAML includes provenance header comment."""
        from portolan_cli.metadata_extraction import ExtractedMetadata
        from portolan_cli.metadata_seeding import seed_metadata_yaml

        extracted = ExtractedMetadata(
            source_url="https://example.com/api/data",
            source_type="arcgis",
            extraction_date=date(2026, 4, 7),
        )

        output_path = tmp_path / "metadata.yaml"
        seed_metadata_yaml(extracted, output_path)

        content = output_path.read_text()

        # Should have provenance info in comments
        assert "arcgis" in content.lower()
        assert "2026-04-07" in content
        assert "https://example.com/api/data" in content

    @pytest.mark.unit
    def test_required_section_includes_todo_markers(self, tmp_path: Path) -> None:
        """Required section includes TODO markers for human fields."""
        from portolan_cli.metadata_extraction import ExtractedMetadata
        from portolan_cli.metadata_seeding import seed_metadata_yaml

        extracted = ExtractedMetadata(
            source_url="https://example.com/api/data",
            source_type="arcgis",
            extraction_date=date(2026, 4, 7),
        )

        output_path = tmp_path / "metadata.yaml"
        seed_metadata_yaml(extracted, output_path)

        content = output_path.read_text()

        # Should have TODO markers for required fields
        assert "TODO" in content
        assert "license" in content.lower()

    @pytest.mark.unit
    def test_populates_contact_name_when_available(self, tmp_path: Path) -> None:
        """contact.name is populated from extracted contact_name."""
        from portolan_cli.metadata_extraction import ExtractedMetadata
        from portolan_cli.metadata_seeding import seed_metadata_yaml

        extracted = ExtractedMetadata(
            source_url="https://example.com/api/data",
            source_type="arcgis",
            extraction_date=date(2026, 4, 7),
            contact_name="Data Team",
        )

        output_path = tmp_path / "metadata.yaml"
        seed_metadata_yaml(extracted, output_path)

        parsed = yaml.safe_load(output_path.read_text())

        assert parsed["contact"]["name"] == "Data Team"

    @pytest.mark.unit
    def test_populates_attribution(self, tmp_path: Path) -> None:
        """attribution field is populated from extracted metadata."""
        from portolan_cli.metadata_extraction import ExtractedMetadata
        from portolan_cli.metadata_seeding import seed_metadata_yaml

        extracted = ExtractedMetadata(
            source_url="https://example.com/api/data",
            source_type="arcgis",
            extraction_date=date(2026, 4, 7),
            attribution="City of Philadelphia",
        )

        output_path = tmp_path / "metadata.yaml"
        seed_metadata_yaml(extracted, output_path)

        parsed = yaml.safe_load(output_path.read_text())

        assert parsed["attribution"] == "City of Philadelphia"

    @pytest.mark.unit
    def test_populates_keywords(self, tmp_path: Path) -> None:
        """keywords field is populated as a list."""
        from portolan_cli.metadata_extraction import ExtractedMetadata
        from portolan_cli.metadata_seeding import seed_metadata_yaml

        extracted = ExtractedMetadata(
            source_url="https://example.com/api/data",
            source_type="arcgis",
            extraction_date=date(2026, 4, 7),
            keywords=["census", "demographics", "population"],
        )

        output_path = tmp_path / "metadata.yaml"
        seed_metadata_yaml(extracted, output_path)

        parsed = yaml.safe_load(output_path.read_text())

        assert parsed["keywords"] == ["census", "demographics", "population"]

    @pytest.mark.unit
    def test_populates_source_url(self, tmp_path: Path) -> None:
        """source_url field is populated in lifecycle section."""
        from portolan_cli.metadata_extraction import ExtractedMetadata
        from portolan_cli.metadata_seeding import seed_metadata_yaml

        extracted = ExtractedMetadata(
            source_url="https://services.arcgis.com/fLeGjb7u4uXqeF9q/arcgis/rest/services",
            source_type="arcgis",
            extraction_date=date(2026, 4, 7),
        )

        output_path = tmp_path / "metadata.yaml"
        seed_metadata_yaml(extracted, output_path)

        parsed = yaml.safe_load(output_path.read_text())

        assert (
            parsed["source_url"]
            == "https://services.arcgis.com/fLeGjb7u4uXqeF9q/arcgis/rest/services"
        )

    @pytest.mark.unit
    def test_populates_processing_notes(self, tmp_path: Path) -> None:
        """processing_notes field is populated."""
        from portolan_cli.metadata_extraction import ExtractedMetadata
        from portolan_cli.metadata_seeding import seed_metadata_yaml

        extracted = ExtractedMetadata(
            source_url="https://example.com/api/data",
            source_type="arcgis",
            extraction_date=date(2026, 4, 7),
            processing_notes="Extracted via portolan extract arcgis",
        )

        output_path = tmp_path / "metadata.yaml"
        seed_metadata_yaml(extracted, output_path)

        parsed = yaml.safe_load(output_path.read_text())

        assert parsed["processing_notes"] == "Extracted via portolan extract arcgis"

    @pytest.mark.unit
    def test_populates_known_issues(self, tmp_path: Path) -> None:
        """known_issues field is populated."""
        from portolan_cli.metadata_extraction import ExtractedMetadata
        from portolan_cli.metadata_seeding import seed_metadata_yaml

        extracted = ExtractedMetadata(
            source_url="https://example.com/api/data",
            source_type="arcgis",
            extraction_date=date(2026, 4, 7),
            known_issues="Data restricted to authenticated users",
        )

        output_path = tmp_path / "metadata.yaml"
        seed_metadata_yaml(extracted, output_path)

        parsed = yaml.safe_load(output_path.read_text())

        assert parsed["known_issues"] == "Data restricted to authenticated users"

    @pytest.mark.unit
    def test_includes_license_hint_as_comment(self, tmp_path: Path) -> None:
        """license_hint is included as a comment/suggestion."""
        from portolan_cli.metadata_extraction import ExtractedMetadata
        from portolan_cli.metadata_seeding import seed_metadata_yaml

        extracted = ExtractedMetadata(
            source_url="https://example.com/api/data",
            source_type="socrata",
            extraction_date=date(2026, 4, 7),
            license_hint="Creative Commons Attribution",
        )

        output_path = tmp_path / "metadata.yaml"
        seed_metadata_yaml(extracted, output_path)

        content = output_path.read_text()

        # License hint should appear as a comment suggesting the SPDX identifier
        assert "Creative Commons Attribution" in content

    @pytest.mark.unit
    def test_does_not_overwrite_by_default(self, tmp_path: Path) -> None:
        """seed_metadata_yaml returns False and does not overwrite existing file."""
        from portolan_cli.metadata_extraction import ExtractedMetadata
        from portolan_cli.metadata_seeding import seed_metadata_yaml

        output_path = tmp_path / "metadata.yaml"
        output_path.write_text("# existing content\nlicense: MIT\n")

        extracted = ExtractedMetadata(
            source_url="https://example.com/api/data",
            source_type="arcgis",
            extraction_date=date(2026, 4, 7),
        )

        result = seed_metadata_yaml(extracted, output_path, overwrite=False)

        assert result is False
        assert "existing content" in output_path.read_text()

    @pytest.mark.unit
    def test_overwrites_when_flag_set(self, tmp_path: Path) -> None:
        """seed_metadata_yaml overwrites when overwrite=True."""
        from portolan_cli.metadata_extraction import ExtractedMetadata
        from portolan_cli.metadata_seeding import seed_metadata_yaml

        output_path = tmp_path / "metadata.yaml"
        output_path.write_text("# existing content\nlicense: MIT\n")

        extracted = ExtractedMetadata(
            source_url="https://example.com/api/data",
            source_type="arcgis",
            extraction_date=date(2026, 4, 7),
        )

        result = seed_metadata_yaml(extracted, output_path, overwrite=True)

        assert result is True
        assert "existing content" not in output_path.read_text()
        assert "arcgis" in output_path.read_text().lower()


class TestAuthorSection:
    """Tests for #316 author support in seeding."""

    @pytest.mark.unit
    def test_populates_authors_list(self, tmp_path: Path) -> None:
        """authors field is populated as a list of dicts."""
        from portolan_cli.metadata_extraction import Author, ExtractedMetadata
        from portolan_cli.metadata_seeding import seed_metadata_yaml

        extracted = ExtractedMetadata(
            source_url="https://zenodo.org/record/1234567",
            source_type="zenodo",
            extraction_date=date(2026, 4, 7),
            authors=[
                Author(name="Alice Smith", email="alice@example.org", orcid="0000-0001-2345-6789"),
                Author(name="Bob Jones"),
            ],
        )

        output_path = tmp_path / "metadata.yaml"
        seed_metadata_yaml(extracted, output_path)

        parsed = yaml.safe_load(output_path.read_text())

        assert "authors" in parsed
        assert len(parsed["authors"]) == 2
        assert parsed["authors"][0]["name"] == "Alice Smith"
        assert parsed["authors"][0]["email"] == "alice@example.org"
        assert parsed["authors"][0]["orcid"] == "0000-0001-2345-6789"
        assert parsed["authors"][1]["name"] == "Bob Jones"

    @pytest.mark.unit
    def test_omits_none_author_fields(self, tmp_path: Path) -> None:
        """Author entries omit None fields (email, orcid) when not present."""
        from portolan_cli.metadata_extraction import Author, ExtractedMetadata
        from portolan_cli.metadata_seeding import seed_metadata_yaml

        extracted = ExtractedMetadata(
            source_url="https://zenodo.org/record/1234567",
            source_type="zenodo",
            extraction_date=date(2026, 4, 7),
            authors=[Author(name="Alice Smith")],  # No email or orcid
        )

        output_path = tmp_path / "metadata.yaml"
        seed_metadata_yaml(extracted, output_path)

        parsed = yaml.safe_load(output_path.read_text())

        # Should only have name, not email/orcid keys
        assert parsed["authors"][0] == {"name": "Alice Smith"}

    @pytest.mark.unit
    def test_populates_citations_list(self, tmp_path: Path) -> None:
        """citations field is populated as a list of strings."""
        from portolan_cli.metadata_extraction import ExtractedMetadata
        from portolan_cli.metadata_seeding import seed_metadata_yaml

        extracted = ExtractedMetadata(
            source_url="https://zenodo.org/record/1234567",
            source_type="zenodo",
            extraction_date=date(2026, 4, 7),
            citations=[
                "Smith et al. (2023) Nature 123:456-789",
                "Jones (2022) Science 100:234",
            ],
        )

        output_path = tmp_path / "metadata.yaml"
        seed_metadata_yaml(extracted, output_path)

        parsed = yaml.safe_load(output_path.read_text())

        assert "citations" in parsed
        assert len(parsed["citations"]) == 2
        assert "Smith et al." in parsed["citations"][0]

    @pytest.mark.unit
    def test_populates_doi(self, tmp_path: Path) -> None:
        """doi field is populated."""
        from portolan_cli.metadata_extraction import ExtractedMetadata
        from portolan_cli.metadata_seeding import seed_metadata_yaml

        extracted = ExtractedMetadata(
            source_url="https://zenodo.org/record/1234567",
            source_type="zenodo",
            extraction_date=date(2026, 4, 7),
            doi="10.5281/zenodo.1234567",
        )

        output_path = tmp_path / "metadata.yaml"
        seed_metadata_yaml(extracted, output_path)

        parsed = yaml.safe_load(output_path.read_text())

        assert parsed["doi"] == "10.5281/zenodo.1234567"

    @pytest.mark.unit
    def test_populates_related_dois(self, tmp_path: Path) -> None:
        """related_dois field is populated as a list."""
        from portolan_cli.metadata_extraction import ExtractedMetadata
        from portolan_cli.metadata_seeding import seed_metadata_yaml

        extracted = ExtractedMetadata(
            source_url="https://zenodo.org/record/1234567",
            source_type="zenodo",
            extraction_date=date(2026, 4, 7),
            related_dois=["10.1234/related.1", "10.1234/related.2"],
        )

        output_path = tmp_path / "metadata.yaml"
        seed_metadata_yaml(extracted, output_path)

        parsed = yaml.safe_load(output_path.read_text())

        assert "related_dois" in parsed
        assert parsed["related_dois"] == ["10.1234/related.1", "10.1234/related.2"]


class TestVersionSection:
    """Tests for #316 upstream version support in seeding."""

    @pytest.mark.unit
    def test_populates_upstream_version(self, tmp_path: Path) -> None:
        """upstream_version field is populated."""
        from portolan_cli.metadata_extraction import ExtractedMetadata
        from portolan_cli.metadata_seeding import seed_metadata_yaml

        extracted = ExtractedMetadata(
            source_url="https://zenodo.org/record/1234567",
            source_type="zenodo",
            extraction_date=date(2026, 4, 7),
            upstream_version="2.1.0",
        )

        output_path = tmp_path / "metadata.yaml"
        seed_metadata_yaml(extracted, output_path)

        parsed = yaml.safe_load(output_path.read_text())

        assert parsed["upstream_version"] == "2.1.0"

    @pytest.mark.unit
    def test_populates_upstream_version_url(self, tmp_path: Path) -> None:
        """upstream_version_url field is populated."""
        from portolan_cli.metadata_extraction import ExtractedMetadata
        from portolan_cli.metadata_seeding import seed_metadata_yaml

        extracted = ExtractedMetadata(
            source_url="https://zenodo.org/record/1234567",
            source_type="zenodo",
            extraction_date=date(2026, 4, 7),
            upstream_version="2.1.0",
            upstream_version_url="https://zenodo.org/record/1234567/versions",
        )

        output_path = tmp_path / "metadata.yaml"
        seed_metadata_yaml(extracted, output_path)

        parsed = yaml.safe_load(output_path.read_text())

        assert parsed["upstream_version_url"] == "https://zenodo.org/record/1234567/versions"


class TestSectionGenerators:
    """Tests for individual section generator functions."""

    @pytest.mark.unit
    def test_add_header_includes_source_info(self) -> None:
        """_add_header includes source type, URL, and date."""
        from portolan_cli.metadata_extraction import ExtractedMetadata
        from portolan_cli.metadata_seeding import _add_header

        extracted = ExtractedMetadata(
            source_url="https://example.com/api",
            source_type="arcgis",
            extraction_date=date(2026, 4, 7),
        )
        lines: list[str] = []

        _add_header(lines, extracted)

        content = "\n".join(lines)
        assert "arcgis" in content.lower()
        assert "https://example.com/api" in content
        assert "2026-04-07" in content

    @pytest.mark.unit
    def test_add_required_section_creates_contact_structure(self) -> None:
        """_add_required_section creates contact with name and email TODO."""
        from portolan_cli.metadata_extraction import ExtractedMetadata
        from portolan_cli.metadata_seeding import _add_required_section

        extracted = ExtractedMetadata(
            source_url="https://example.com/api",
            source_type="arcgis",
            extraction_date=date(2026, 4, 7),
        )
        lines: list[str] = []

        _add_required_section(lines, extracted)

        content = "\n".join(lines)
        assert "contact:" in content
        assert "name:" in content
        assert "email:" in content
        assert "license:" in content

    @pytest.mark.unit
    def test_add_discovery_section_includes_doi_when_present(self) -> None:
        """_add_discovery_section includes DOI when extracted."""
        from portolan_cli.metadata_extraction import ExtractedMetadata
        from portolan_cli.metadata_seeding import _add_discovery_section

        extracted = ExtractedMetadata(
            source_url="https://example.com/api",
            source_type="zenodo",
            extraction_date=date(2026, 4, 7),
            doi="10.5281/zenodo.1234567",
        )
        lines: list[str] = []

        _add_discovery_section(lines, extracted)

        content = "\n".join(lines)
        assert "10.5281/zenodo.1234567" in content

    @pytest.mark.unit
    def test_add_lifecycle_section_includes_source_url(self) -> None:
        """_add_lifecycle_section includes source_url."""
        from portolan_cli.metadata_extraction import ExtractedMetadata
        from portolan_cli.metadata_seeding import _add_lifecycle_section

        extracted = ExtractedMetadata(
            source_url="https://services.arcgis.com/test",
            source_type="arcgis",
            extraction_date=date(2026, 4, 7),
            processing_notes="Extracted from ArcGIS",
        )
        lines: list[str] = []

        _add_lifecycle_section(lines, extracted)

        content = "\n".join(lines)
        assert "https://services.arcgis.com/test" in content
        assert "Extracted from ArcGIS" in content

    @pytest.mark.unit
    def test_add_authors_section_formats_orcid(self) -> None:
        """_add_authors_section properly formats ORCID identifiers."""
        from portolan_cli.metadata_extraction import Author, ExtractedMetadata
        from portolan_cli.metadata_seeding import _add_authors_section

        extracted = ExtractedMetadata(
            source_url="https://zenodo.org/record/1234567",
            source_type="zenodo",
            extraction_date=date(2026, 4, 7),
            authors=[Author(name="Alice", orcid="0000-0001-2345-6789")],
        )
        lines: list[str] = []

        _add_authors_section(lines, extracted)

        content = "\n".join(lines)
        assert "0000-0001-2345-6789" in content

    @pytest.mark.unit
    def test_add_version_section_includes_version_info(self) -> None:
        """_add_version_section includes upstream version details."""
        from portolan_cli.metadata_extraction import ExtractedMetadata
        from portolan_cli.metadata_seeding import _add_version_section

        extracted = ExtractedMetadata(
            source_url="https://zenodo.org/record/1234567",
            source_type="zenodo",
            extraction_date=date(2026, 4, 7),
            upstream_version="2.1.0",
            upstream_version_url="https://zenodo.org/versions",
        )
        lines: list[str] = []

        _add_version_section(lines, extracted)

        content = "\n".join(lines)
        assert "2.1.0" in content
        assert "https://zenodo.org/versions" in content
