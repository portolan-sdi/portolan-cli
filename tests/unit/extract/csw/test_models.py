"""Tests for CSW/ISO metadata models."""

from __future__ import annotations

import pytest

pytestmark = pytest.mark.unit


class TestISOMetadata:
    """Tests for ISOMetadata dataclass."""

    def test_creates_with_all_fields(self) -> None:
        """ISOMetadata stores all extracted fields."""
        from portolan_cli.extract.csw.models import ISOMetadata

        metadata = ISOMetadata(
            file_identifier="9a8322bd-f53a-4f99-ad9e-753b45bdee85",
            title="INSPIRE - Bâtiments en Wallonie (BE)",
            abstract="Cette série de couches de données...",
            keywords=["Bâtiments", "Building", "emprise"],
            contact_organization="Service public de Wallonie (SPW)",
            contact_email="helpdesk.carto@spw.wallonie.be",
            license_url="https://creativecommons.org/licenses/by/4.0/",
            license_text="CC-BY 4.0",
            access_constraints="Pas de limitation d'accès public",
            lineage="Le présent standard de données INSPIRE...",
            thumbnail_url="https://example.com/thumbnail.png",
            scale_denominator=10000,
            topic_category="imageryBaseMapsEarthCover",
            maintenance_frequency="asNeeded",
            date_created="2020-10-29",
            date_revised="2020-10-29",
            date_published="2020-10-29",
        )

        assert metadata.file_identifier == "9a8322bd-f53a-4f99-ad9e-753b45bdee85"
        assert metadata.title == "INSPIRE - Bâtiments en Wallonie (BE)"
        assert "Bâtiments" in metadata.keywords
        assert metadata.license_url == "https://creativecommons.org/licenses/by/4.0/"

    def test_creates_with_minimal_fields(self) -> None:
        """ISOMetadata works with only required fields."""
        from portolan_cli.extract.csw.models import ISOMetadata

        metadata = ISOMetadata(
            file_identifier="abc-123",
            title="Test Dataset",
        )

        assert metadata.file_identifier == "abc-123"
        assert metadata.title == "Test Dataset"
        assert metadata.abstract is None
        assert metadata.keywords is None
        assert metadata.license_url is None

    def test_to_extracted_metadata(self) -> None:
        """ISOMetadata converts to ExtractedMetadata for seeding."""
        from portolan_cli.extract.csw.models import ISOMetadata

        iso = ISOMetadata(
            file_identifier="abc-123",
            title="Buildings Dataset",
            abstract="All buildings in the region",
            keywords=["Buildings", "Structures"],
            contact_organization="GIS Department",
            contact_email="gis@example.com",
            license_url="https://creativecommons.org/licenses/by/4.0/",
            license_text="CC-BY 4.0",
            lineage="Derived from aerial photography",
        )

        extracted = iso.to_extracted_metadata(source_url="https://example.com/wfs")

        assert extracted.source_type == "csw_iso19139"
        assert extracted.source_url == "https://example.com/wfs"
        assert extracted.description == "All buildings in the region"
        assert extracted.keywords == ["Buildings", "Structures"]
        assert extracted.attribution == "GIS Department"
        assert "CC-BY 4.0" in (extracted.license_raw or "")

    def test_to_extracted_metadata_uses_title_if_no_abstract(self) -> None:
        """Uses title as description when abstract is missing."""
        from portolan_cli.extract.csw.models import ISOMetadata

        iso = ISOMetadata(
            file_identifier="abc-123",
            title="Buildings Dataset",
            abstract=None,
        )

        extracted = iso.to_extracted_metadata(source_url="https://example.com/wfs")

        assert extracted.description == "Buildings Dataset"

    def test_get_license_spdx_identifier(self) -> None:
        """Extracts SPDX identifier from license URL."""
        from portolan_cli.extract.csw.models import ISOMetadata

        # CC-BY-4.0
        iso = ISOMetadata(
            file_identifier="abc",
            title="Test",
            license_url="https://creativecommons.org/licenses/by/4.0/",
        )
        assert iso.get_license_spdx() == "CC-BY-4.0"

        # CC0
        iso2 = ISOMetadata(
            file_identifier="abc",
            title="Test",
            license_url="https://creativecommons.org/publicdomain/zero/1.0/",
        )
        assert iso2.get_license_spdx() == "CC0-1.0"

        # Unknown
        iso3 = ISOMetadata(
            file_identifier="abc",
            title="Test",
            license_url="https://example.com/custom-license",
        )
        assert iso3.get_license_spdx() is None

    def test_has_useful_metadata(self) -> None:
        """Checks if metadata has more than just identifier/title."""
        from portolan_cli.extract.csw.models import ISOMetadata

        # Minimal - not useful
        minimal = ISOMetadata(file_identifier="abc", title="Test")
        assert minimal.has_useful_metadata() is False

        # Has abstract - useful
        with_abstract = ISOMetadata(
            file_identifier="abc",
            title="Test",
            abstract="Real description here",
        )
        assert with_abstract.has_useful_metadata() is True

        # Sparse keywords (< 3) alone are NOT useful (too weak)
        with_sparse_keywords = ISOMetadata(
            file_identifier="abc",
            title="Test",
            keywords=["buildings", "structures"],
        )
        assert with_sparse_keywords.has_useful_metadata() is False

        # 3+ keywords are useful
        with_keywords = ISOMetadata(
            file_identifier="abc",
            title="Test",
            keywords=["buildings", "structures", "urban"],
        )
        assert with_keywords.has_useful_metadata() is True

        # Has license - useful
        with_license = ISOMetadata(
            file_identifier="abc",
            title="Test",
            license_url="https://creativecommons.org/licenses/by/4.0/",
        )
        assert with_license.has_useful_metadata() is True
