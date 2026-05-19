"""Tests for WFS metadata extraction.

These tests verify the WFS metadata extraction which parses
GetCapabilities response to extract service-level metadata.
"""

from __future__ import annotations

import pytest

from portolan_cli.extract.wfs.metadata import WFSMetadata, extract_wfs_metadata

pytestmark = pytest.mark.unit


class TestExtractWfsMetadata:
    """Tests for extract_wfs_metadata function."""

    def test_extract_from_capabilities_dict(self) -> None:
        """Extract metadata from parsed capabilities dict."""
        caps = {
            "service_title": "Belgium Building Footprints",
            "service_abstract": "INSPIRE buildings data for Belgium",
            "provider_name": "SPW - Wallonia Geoportal",
            "keywords": ["buildings", "INSPIRE", "Belgium"],
            "fees": "None",
            "access_constraints": "CC-BY 4.0",
        }

        metadata = extract_wfs_metadata(caps, source_url="https://example.com/wfs")

        assert metadata.description == "INSPIRE buildings data for Belgium"
        assert metadata.attribution == "SPW - Wallonia Geoportal"
        assert metadata.keywords == ["buildings", "INSPIRE", "Belgium"]
        assert "CC-BY 4.0" in (metadata.license_info_raw or "")

    def test_extract_with_missing_fields(self) -> None:
        """Missing fields result in None values."""
        caps = {
            "service_title": "Test Service",
        }

        metadata = extract_wfs_metadata(caps, source_url="https://example.com/wfs")

        assert metadata.source_url == "https://example.com/wfs"
        assert metadata.description is None
        assert metadata.attribution is None
        assert metadata.keywords is None

    def test_extract_empty_capabilities(self) -> None:
        """Empty capabilities dict produces minimal metadata."""
        metadata = extract_wfs_metadata({}, source_url="https://example.com/wfs")

        assert metadata.source_url == "https://example.com/wfs"
        assert metadata.description is None


class TestWFSMetadata:
    """Tests for WFSMetadata dataclass."""

    def test_to_extracted_conversion(self) -> None:
        """WFSMetadata converts to common ExtractedMetadata."""
        wfs_metadata = WFSMetadata(
            source_url="https://example.com/wfs",
            service_title="Test WFS",
            service_abstract="Test description",
            provider_name="Test Provider",
            keywords=["test", "wfs"],
            fees="None",
            access_constraints="Public domain",
        )

        extracted = wfs_metadata.to_extracted()

        assert extracted.source_type == "wfs"
        assert extracted.source_url == "https://example.com/wfs"
        assert extracted.description == "Test description"
        assert extracted.attribution == "Test Provider"
        assert extracted.keywords == ["test", "wfs"]

    def test_to_extracted_with_none_values(self) -> None:
        """Conversion handles None values gracefully."""
        wfs_metadata = WFSMetadata(
            source_url="https://example.com/wfs",
            service_title=None,
            service_abstract=None,
            provider_name=None,
            keywords=None,
            fees=None,
            access_constraints=None,
        )

        extracted = wfs_metadata.to_extracted()

        assert extracted.source_url == "https://example.com/wfs"
        assert extracted.description is None
        assert extracted.attribution is None

    def test_to_extracted_detects_geoserver_boilerplate(self) -> None:
        """GeoServer default description is detected as boilerplate."""
        wfs_metadata = WFSMetadata(
            source_url="https://example.com/wfs",
            service_title="INSPIRE Buildings Wallonia",
            service_abstract="This is the reference implementation of WFS 1.0.0 and WFS 1.1.0, supports all WFS operations including Transaction.",
            provider_name="SPW",
        )

        extracted = wfs_metadata.to_extracted()

        # Should use service_title as fallback, not boilerplate
        assert extracted.description == "INSPIRE Buildings Wallonia"

    def test_to_extracted_detects_mapserver_boilerplate(self) -> None:
        """MapServer default WFS title is detected as boilerplate."""
        wfs_metadata = WFSMetadata(
            source_url="https://example.com/wfs",
            service_title="City GIS Data",
            service_abstract="MapServer Web Feature Service",  # Actual boilerplate phrase
            provider_name="City GIS",
        )

        extracted = wfs_metadata.to_extracted()

        assert extracted.description == "City GIS Data"

    def test_to_extracted_keeps_real_description(self) -> None:
        """Real descriptions are kept, not replaced by title."""
        wfs_metadata = WFSMetadata(
            source_url="https://example.com/wfs",
            service_title="Buildings WFS",
            service_abstract="INSPIRE buildings data for the Wallonia region of Belgium, including footprints and 3D models.",
            provider_name="SPW",
        )

        extracted = wfs_metadata.to_extracted()

        assert (
            extracted.description
            == "INSPIRE buildings data for the Wallonia region of Belgium, including footprints and 3D models."
        )

    def test_to_extracted_title_fallback_when_abstract_none(self) -> None:
        """When abstract is None, use service_title as description."""
        wfs_metadata = WFSMetadata(
            source_url="https://example.com/wfs",
            service_title="INSPIRE Buildings Wallonia",
            service_abstract=None,
            provider_name="SPW",
        )

        extracted = wfs_metadata.to_extracted()

        assert extracted.description == "INSPIRE Buildings Wallonia"

    def test_to_extracted_none_when_both_missing(self) -> None:
        """When both abstract and title are None, description is None."""
        wfs_metadata = WFSMetadata(
            source_url="https://example.com/wfs",
            service_title=None,
            service_abstract=None,
        )

        extracted = wfs_metadata.to_extracted()

        assert extracted.description is None

    def test_to_extracted_none_when_both_boilerplate(self) -> None:
        """When both abstract and title are boilerplate, description is None."""
        wfs_metadata = WFSMetadata(
            source_url="https://example.com/wfs",
            service_title="GeoServer Web Feature Service",
            service_abstract="This is the reference implementation of WFS 1.0.0",
            provider_name="SPW",
        )

        extracted = wfs_metadata.to_extracted()

        # Both are boilerplate, so description should be None
        assert extracted.description is None

    def test_to_extracted_skips_boilerplate_title(self) -> None:
        """Don't use boilerplate title as fallback."""
        wfs_metadata = WFSMetadata(
            source_url="https://example.com/wfs",
            service_title="Web Feature Service",
            service_abstract=None,
            provider_name="SPW",
        )

        extracted = wfs_metadata.to_extracted()

        # Title is generic boilerplate, don't use it
        assert extracted.description is None


class TestIsBoilerplateDescription:
    """Tests for boilerplate detection."""

    def test_geoserver_reference_implementation(self) -> None:
        """Detect GeoServer default text."""
        from portolan_cli.extract.wfs.metadata import is_boilerplate_description

        text = "This is the reference implementation of WFS 1.0.0 and WFS 1.1.0, supports all WFS operations including Transaction."
        assert is_boilerplate_description(text) is True

    def test_mapserver_wfs_boilerplate(self) -> None:
        """Detect MapServer WFS boilerplate phrase."""
        from portolan_cli.extract.wfs.metadata import is_boilerplate_description

        assert is_boilerplate_description("MapServer Web Feature Service") is True

    def test_geoserver_mention_not_boilerplate(self) -> None:
        """Mere mention of GeoServer is NOT boilerplate (could be legitimate)."""
        from portolan_cli.extract.wfs.metadata import is_boilerplate_description

        # "Powered by GeoServer" is a legitimate description, not default text
        assert is_boilerplate_description("Powered by GeoServer") is False
        # But actual processing info is fine
        assert is_boilerplate_description("Data processed using GeoServer tools") is False

    def test_real_description_not_boilerplate(self) -> None:
        """Real descriptions are not flagged."""
        from portolan_cli.extract.wfs.metadata import is_boilerplate_description

        text = "INSPIRE buildings data for the Wallonia region of Belgium"
        assert is_boilerplate_description(text) is False

    def test_none_not_boilerplate(self) -> None:
        """None is not boilerplate (it's just missing)."""
        from portolan_cli.extract.wfs.metadata import is_boilerplate_description

        assert is_boilerplate_description(None) is False

    def test_empty_string_not_boilerplate(self) -> None:
        """Empty string is not boilerplate."""
        from portolan_cli.extract.wfs.metadata import is_boilerplate_description

        assert is_boilerplate_description("") is False

    def test_geoserver_wfs_title_boilerplate(self) -> None:
        """GeoServer default WFS title is boilerplate."""
        from portolan_cli.extract.wfs.metadata import is_boilerplate_description

        assert is_boilerplate_description("GeoServer Web Feature Service") is True

    def test_generic_wfs_title_boilerplate(self) -> None:
        """Generic 'Web Feature Service' title is boilerplate."""
        from portolan_cli.extract.wfs.metadata import is_boilerplate_description

        assert is_boilerplate_description("Web Feature Service") is True
