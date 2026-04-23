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
