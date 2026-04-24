"""Tests for ISO 19139 XML parser."""

from __future__ import annotations

from pathlib import Path

import pytest

pytestmark = pytest.mark.unit

FIXTURES_DIR = Path(__file__).parent.parent.parent.parent / "fixtures" / "csw"


class TestParseISO19139:
    """Tests for parse_iso19139 function."""

    @pytest.fixture
    def belgium_buildings_xml(self) -> str:
        """Load Belgium buildings ISO 19139 fixture."""
        fixture_path = FIXTURES_DIR / "belgium_buildings_iso19139.xml"
        return fixture_path.read_text(encoding="utf-8")

    def test_parses_file_identifier(self, belgium_buildings_xml: str) -> None:
        """Extracts file identifier from ISO metadata."""
        from portolan_cli.extract.csw.iso_parser import parse_iso19139

        metadata = parse_iso19139(belgium_buildings_xml)

        assert metadata.file_identifier == "9a8322bd-f53a-4f99-ad9e-753b45bdee85"

    def test_parses_title(self, belgium_buildings_xml: str) -> None:
        """Extracts title from ISO metadata."""
        from portolan_cli.extract.csw.iso_parser import parse_iso19139

        metadata = parse_iso19139(belgium_buildings_xml)

        assert metadata.title == "INSPIRE - Bâtiments en Wallonie (BE)"

    def test_parses_abstract(self, belgium_buildings_xml: str) -> None:
        """Extracts abstract from ISO metadata."""
        from portolan_cli.extract.csw.iso_parser import parse_iso19139

        metadata = parse_iso19139(belgium_buildings_xml)

        assert metadata.abstract is not None
        assert "Cette série de couches de données" in metadata.abstract
        assert "Directive INSPIRE" in metadata.abstract
        assert "PICC" in metadata.abstract

    def test_parses_keywords(self, belgium_buildings_xml: str) -> None:
        """Extracts keywords from multiple thesauri."""
        from portolan_cli.extract.csw.iso_parser import parse_iso19139

        metadata = parse_iso19139(belgium_buildings_xml)

        assert metadata.keywords is not None
        # From various thesauri in the document
        assert "Bâtiments" in metadata.keywords
        assert "Building" in metadata.keywords
        assert "emprise" in metadata.keywords

    def test_parses_contact_organization(self, belgium_buildings_xml: str) -> None:
        """Extracts contact organization from first pointOfContact."""
        from portolan_cli.extract.csw.iso_parser import parse_iso19139

        metadata = parse_iso19139(belgium_buildings_xml)

        assert metadata.contact_organization is not None
        # First pointOfContact is Helpdesk carto du SPW
        assert "Helpdesk carto du SPW" in metadata.contact_organization

    def test_parses_contact_email(self, belgium_buildings_xml: str) -> None:
        """Extracts contact email address."""
        from portolan_cli.extract.csw.iso_parser import parse_iso19139

        metadata = parse_iso19139(belgium_buildings_xml)

        assert metadata.contact_email == "helpdesk.carto@spw.wallonie.be"

    def test_parses_license_url(self, belgium_buildings_xml: str) -> None:
        """Extracts license URL (CC-BY-4.0)."""
        from portolan_cli.extract.csw.iso_parser import parse_iso19139

        metadata = parse_iso19139(belgium_buildings_xml)

        assert metadata.license_url == "https://creativecommons.org/licenses/by/4.0/"

    def test_parses_license_text(self, belgium_buildings_xml: str) -> None:
        """Extracts license text description."""
        from portolan_cli.extract.csw.iso_parser import parse_iso19139

        metadata = parse_iso19139(belgium_buildings_xml)

        assert metadata.license_text is not None
        assert "CC-BY 4.0" in metadata.license_text

    def test_parses_access_constraints(self, belgium_buildings_xml: str) -> None:
        """Extracts access constraints."""
        from portolan_cli.extract.csw.iso_parser import parse_iso19139

        metadata = parse_iso19139(belgium_buildings_xml)

        assert metadata.access_constraints is not None
        assert "limitation" in metadata.access_constraints.lower()

    def test_parses_lineage(self, belgium_buildings_xml: str) -> None:
        """Extracts lineage/provenance information."""
        from portolan_cli.extract.csw.iso_parser import parse_iso19139

        metadata = parse_iso19139(belgium_buildings_xml)

        assert metadata.lineage is not None
        assert "PICC" in metadata.lineage
        assert "photogramm" in metadata.lineage.lower()

    def test_parses_thumbnail_url(self, belgium_buildings_xml: str) -> None:
        """Extracts thumbnail/graphic overview URL."""
        from portolan_cli.extract.csw.iso_parser import parse_iso19139

        metadata = parse_iso19139(belgium_buildings_xml)

        assert metadata.thumbnail_url is not None
        assert "picc_vdiff_2.png" in metadata.thumbnail_url

    def test_parses_scale_denominator(self, belgium_buildings_xml: str) -> None:
        """Extracts scale denominator."""
        from portolan_cli.extract.csw.iso_parser import parse_iso19139

        metadata = parse_iso19139(belgium_buildings_xml)

        assert metadata.scale_denominator == 10000

    def test_parses_topic_category(self, belgium_buildings_xml: str) -> None:
        """Extracts topic category code."""
        from portolan_cli.extract.csw.iso_parser import parse_iso19139

        metadata = parse_iso19139(belgium_buildings_xml)

        assert metadata.topic_category == "imageryBaseMapsEarthCover"

    def test_parses_maintenance_frequency(self, belgium_buildings_xml: str) -> None:
        """Extracts maintenance frequency code."""
        from portolan_cli.extract.csw.iso_parser import parse_iso19139

        metadata = parse_iso19139(belgium_buildings_xml)

        assert metadata.maintenance_frequency == "asNeeded"

    def test_parses_dates(self, belgium_buildings_xml: str) -> None:
        """Extracts creation, revision, and publication dates."""
        from portolan_cli.extract.csw.iso_parser import parse_iso19139

        metadata = parse_iso19139(belgium_buildings_xml)

        assert metadata.date_created == "2020-10-29"
        assert metadata.date_revised == "2020-10-29"
        assert metadata.date_published == "2020-10-29"

    def test_handles_missing_optional_fields(self) -> None:
        """Gracefully handles missing optional fields."""
        from portolan_cli.extract.csw.iso_parser import parse_iso19139

        minimal_xml = """<?xml version="1.0" encoding="UTF-8"?>
        <csw:GetRecordByIdResponse xmlns:csw="http://www.opengis.net/cat/csw/2.0.2">
          <gmd:MD_Metadata xmlns:gmd="http://www.isotc211.org/2005/gmd"
                           xmlns:gco="http://www.isotc211.org/2005/gco">
            <gmd:fileIdentifier>
              <gco:CharacterString>test-id</gco:CharacterString>
            </gmd:fileIdentifier>
            <gmd:identificationInfo>
              <gmd:MD_DataIdentification>
                <gmd:citation>
                  <gmd:CI_Citation>
                    <gmd:title>
                      <gco:CharacterString>Test Title</gco:CharacterString>
                    </gmd:title>
                  </gmd:CI_Citation>
                </gmd:citation>
              </gmd:MD_DataIdentification>
            </gmd:identificationInfo>
          </gmd:MD_Metadata>
        </csw:GetRecordByIdResponse>
        """

        metadata = parse_iso19139(minimal_xml)

        assert metadata.file_identifier == "test-id"
        assert metadata.title == "Test Title"
        assert metadata.abstract is None
        assert metadata.keywords is None
        assert metadata.license_url is None

    def test_handles_direct_md_metadata_root(self) -> None:
        """Handles ISO XML with MD_Metadata as root (no CSW wrapper)."""
        from portolan_cli.extract.csw.iso_parser import parse_iso19139

        direct_xml = """<?xml version="1.0" encoding="UTF-8"?>
        <gmd:MD_Metadata xmlns:gmd="http://www.isotc211.org/2005/gmd"
                         xmlns:gco="http://www.isotc211.org/2005/gco">
          <gmd:fileIdentifier>
            <gco:CharacterString>direct-id</gco:CharacterString>
          </gmd:fileIdentifier>
          <gmd:identificationInfo>
            <gmd:MD_DataIdentification>
              <gmd:citation>
                <gmd:CI_Citation>
                  <gmd:title>
                    <gco:CharacterString>Direct Title</gco:CharacterString>
                  </gmd:title>
                </gmd:CI_Citation>
              </gmd:citation>
              <gmd:abstract>
                <gco:CharacterString>Direct abstract</gco:CharacterString>
              </gmd:abstract>
            </gmd:MD_DataIdentification>
          </gmd:identificationInfo>
        </gmd:MD_Metadata>
        """

        metadata = parse_iso19139(direct_xml)

        assert metadata.file_identifier == "direct-id"
        assert metadata.title == "Direct Title"
        assert metadata.abstract == "Direct abstract"

    def test_raises_on_invalid_xml(self) -> None:
        """Raises ParseError on invalid XML."""
        from portolan_cli.extract.csw.iso_parser import ISOParseError, parse_iso19139

        with pytest.raises(ISOParseError, match="Failed to parse"):
            parse_iso19139("not valid xml <<<<")

    def test_raises_on_missing_required_fields(self) -> None:
        """Raises ParseError when required fields are missing."""
        from portolan_cli.extract.csw.iso_parser import ISOParseError, parse_iso19139

        no_identifier_xml = """<?xml version="1.0" encoding="UTF-8"?>
        <gmd:MD_Metadata xmlns:gmd="http://www.isotc211.org/2005/gmd"
                         xmlns:gco="http://www.isotc211.org/2005/gco">
          <gmd:identificationInfo>
            <gmd:MD_DataIdentification>
              <gmd:citation>
                <gmd:CI_Citation>
                  <gmd:title>
                    <gco:CharacterString>Title Only</gco:CharacterString>
                  </gmd:title>
                </gmd:CI_Citation>
              </gmd:citation>
            </gmd:MD_DataIdentification>
          </gmd:identificationInfo>
        </gmd:MD_Metadata>
        """

        with pytest.raises(ISOParseError, match="file_identifier"):
            parse_iso19139(no_identifier_xml)


class TestExtractKeywordsFromThesauri:
    """Tests for keyword extraction from multiple thesauri."""

    def test_extracts_from_character_string(self) -> None:
        """Extracts keywords from gco:CharacterString elements."""
        from portolan_cli.extract.csw.iso_parser import parse_iso19139

        xml = """<?xml version="1.0" encoding="UTF-8"?>
        <gmd:MD_Metadata xmlns:gmd="http://www.isotc211.org/2005/gmd"
                         xmlns:gco="http://www.isotc211.org/2005/gco">
          <gmd:fileIdentifier>
            <gco:CharacterString>test-id</gco:CharacterString>
          </gmd:fileIdentifier>
          <gmd:identificationInfo>
            <gmd:MD_DataIdentification>
              <gmd:citation>
                <gmd:CI_Citation>
                  <gmd:title>
                    <gco:CharacterString>Test</gco:CharacterString>
                  </gmd:title>
                </gmd:CI_Citation>
              </gmd:citation>
              <gmd:descriptiveKeywords>
                <gmd:MD_Keywords>
                  <gmd:keyword>
                    <gco:CharacterString>Keyword One</gco:CharacterString>
                  </gmd:keyword>
                  <gmd:keyword>
                    <gco:CharacterString>Keyword Two</gco:CharacterString>
                  </gmd:keyword>
                </gmd:MD_Keywords>
              </gmd:descriptiveKeywords>
            </gmd:MD_DataIdentification>
          </gmd:identificationInfo>
        </gmd:MD_Metadata>
        """

        metadata = parse_iso19139(xml)

        assert metadata.keywords is not None
        assert "Keyword One" in metadata.keywords
        assert "Keyword Two" in metadata.keywords

    def test_extracts_from_anchor_elements(self) -> None:
        """Extracts keywords from gmx:Anchor elements."""
        from portolan_cli.extract.csw.iso_parser import parse_iso19139

        xml = """<?xml version="1.0" encoding="UTF-8"?>
        <gmd:MD_Metadata xmlns:gmd="http://www.isotc211.org/2005/gmd"
                         xmlns:gco="http://www.isotc211.org/2005/gco"
                         xmlns:gmx="http://www.isotc211.org/2005/gmx"
                         xmlns:xlink="http://www.w3.org/1999/xlink">
          <gmd:fileIdentifier>
            <gco:CharacterString>test-id</gco:CharacterString>
          </gmd:fileIdentifier>
          <gmd:identificationInfo>
            <gmd:MD_DataIdentification>
              <gmd:citation>
                <gmd:CI_Citation>
                  <gmd:title>
                    <gco:CharacterString>Test</gco:CharacterString>
                  </gmd:title>
                </gmd:CI_Citation>
              </gmd:citation>
              <gmd:descriptiveKeywords>
                <gmd:MD_Keywords>
                  <gmd:keyword>
                    <gmx:Anchor xlink:href="http://example.com/keyword1">Anchor Keyword</gmx:Anchor>
                  </gmd:keyword>
                </gmd:MD_Keywords>
              </gmd:descriptiveKeywords>
            </gmd:MD_DataIdentification>
          </gmd:identificationInfo>
        </gmd:MD_Metadata>
        """

        metadata = parse_iso19139(xml)

        assert metadata.keywords is not None
        assert "Anchor Keyword" in metadata.keywords

    def test_deduplicates_keywords(self) -> None:
        """Removes duplicate keywords from different thesauri."""
        from portolan_cli.extract.csw.iso_parser import parse_iso19139

        xml = """<?xml version="1.0" encoding="UTF-8"?>
        <gmd:MD_Metadata xmlns:gmd="http://www.isotc211.org/2005/gmd"
                         xmlns:gco="http://www.isotc211.org/2005/gco">
          <gmd:fileIdentifier>
            <gco:CharacterString>test-id</gco:CharacterString>
          </gmd:fileIdentifier>
          <gmd:identificationInfo>
            <gmd:MD_DataIdentification>
              <gmd:citation>
                <gmd:CI_Citation>
                  <gmd:title>
                    <gco:CharacterString>Test</gco:CharacterString>
                  </gmd:title>
                </gmd:CI_Citation>
              </gmd:citation>
              <gmd:descriptiveKeywords>
                <gmd:MD_Keywords>
                  <gmd:keyword>
                    <gco:CharacterString>Buildings</gco:CharacterString>
                  </gmd:keyword>
                </gmd:MD_Keywords>
              </gmd:descriptiveKeywords>
              <gmd:descriptiveKeywords>
                <gmd:MD_Keywords>
                  <gmd:keyword>
                    <gco:CharacterString>Buildings</gco:CharacterString>
                  </gmd:keyword>
                  <gmd:keyword>
                    <gco:CharacterString>Structures</gco:CharacterString>
                  </gmd:keyword>
                </gmd:MD_Keywords>
              </gmd:descriptiveKeywords>
            </gmd:MD_DataIdentification>
          </gmd:identificationInfo>
        </gmd:MD_Metadata>
        """

        metadata = parse_iso19139(xml)

        assert metadata.keywords is not None
        assert metadata.keywords.count("Buildings") == 1
        assert "Structures" in metadata.keywords

    def test_invalid_fixture_raises_error(self) -> None:
        """Invalid fixture file (missing required fields) raises ISOParseError."""
        from portolan_cli.extract.csw.iso_parser import ISOParseError, parse_iso19139

        fixture_path = FIXTURES_DIR / "invalid_iso19139.xml"
        xml_content = fixture_path.read_text(encoding="utf-8")

        with pytest.raises(ISOParseError, match="Missing required field"):
            parse_iso19139(xml_content)

    def test_minimal_fixture_parses(self) -> None:
        """Minimal fixture parses successfully with only required fields."""
        from portolan_cli.extract.csw.iso_parser import parse_iso19139

        fixture_path = FIXTURES_DIR / "minimal_iso19139.xml"
        xml_content = fixture_path.read_text(encoding="utf-8")

        metadata = parse_iso19139(xml_content)

        assert metadata.file_identifier == "minimal-test-id"
        assert metadata.title == "Minimal Test Dataset"
        assert metadata.abstract == "A minimal test abstract for unit testing."

    def test_csw_wrapped_fixture_parses(self) -> None:
        """CSW-wrapped fixture parses correctly, extracting MD_Metadata from wrapper."""
        from portolan_cli.extract.csw.iso_parser import parse_iso19139

        fixture_path = FIXTURES_DIR / "csw_wrapped_iso19139.xml"
        xml_content = fixture_path.read_text(encoding="utf-8")

        metadata = parse_iso19139(xml_content)

        assert metadata.file_identifier == "csw-wrapped-test-id"
        assert metadata.title == "CSW Wrapped Dataset"
        assert metadata.contact_organization == "Test Organization"
        assert metadata.contact_email == "test@example.com"
        assert metadata.keywords is not None
        assert "test" in metadata.keywords
        assert metadata.license_url == "https://creativecommons.org/licenses/by/4.0/"
