"""Integration tests for FeatureServer metadata.yaml auto-seeding.

Wave 3A: Verify that extraction automatically seeds .portolan/metadata.yaml
from extracted ArcGIS service metadata.

Tests verify:
- metadata.yaml is created after extraction
- Source URL is preserved from service
- TODO markers exist for required human fields (contact.email, license)
- Existing metadata.yaml files are NOT overwritten (overwrite=False)
"""

from __future__ import annotations

from pathlib import Path
from typing import TYPE_CHECKING
from unittest.mock import patch

import pytest
import yaml

from portolan_cli.extract.arcgis.discovery import LayerInfo, ServiceDiscoveryResult

if TYPE_CHECKING:
    pass


# Valid test URL that passes URL parser validation
TEST_FEATURE_SERVER_URL = (
    "https://services.arcgis.com/abc123/ArcGIS/rest/services/Census/FeatureServer"
)


def _create_extraction_mock(
    output_dir: Path,
    layer_name: str = "census_blocks",
) -> tuple[int, int, float]:
    """Create mock extraction result and the parquet file.

    The _extract_single_layer function writes the parquet file, so our mock
    must also create it for downstream code (add_files) to work.
    """
    layer_slug = layer_name
    collection_dir = output_dir / layer_slug
    parquet_path = collection_dir / f"{layer_slug}.parquet"

    # Create the directory and a dummy parquet file
    collection_dir.mkdir(parents=True, exist_ok=True)
    # Write minimal valid parquet (just needs to exist for add_files)
    parquet_path.write_bytes(b"PAR1")  # Minimal parquet magic bytes

    # Return (features, size_bytes, duration)
    return (100, 1024, 1.5)


class TestFeatureServerMetadataSeeding:
    """Integration tests for auto-seeding metadata.yaml from FeatureServer extraction."""

    @pytest.fixture
    def mock_discovery_result(self) -> ServiceDiscoveryResult:
        """Create a mock discovery result with test layers and metadata."""
        return ServiceDiscoveryResult(
            layers=[
                LayerInfo(id=0, name="Census_Blocks", layer_type="Feature Layer"),
            ],
            service_description="Updated quarterly. Source: US Census Bureau.",
            description="Census data including demographics and boundaries",
            copyright_text="City of Philadelphia",
            author="Philadelphia GIS Team",
            keywords="census, demographics, boundaries",
            access_information="Data may be incomplete for recent additions",
            license_info="Public domain - freely redistributable",
        )

    @pytest.mark.integration
    def test_extraction_creates_metadata_yaml(
        self,
        tmp_path: Path,
        mock_discovery_result: ServiceDiscoveryResult,
    ) -> None:
        """Extraction creates .portolan/metadata.yaml from service metadata."""
        from portolan_cli.extract.arcgis.orchestrator import (
            ExtractionOptions,
            extract_arcgis_catalog,
        )

        output_dir = tmp_path / "output"

        def mock_extract_side_effect(
            service_url: str,
            layer: LayerInfo,
            output_path: Path,
            options: ExtractionOptions,
        ) -> tuple[int, int, float]:
            """Side effect that creates the actual parquet file."""
            output_path.parent.mkdir(parents=True, exist_ok=True)
            output_path.write_bytes(b"PAR1")  # Minimal parquet magic bytes
            return (100, 1024, 1.5)

        with (
            patch("portolan_cli.extract.arcgis.orchestrator.discover_layers") as mock_discover,
            patch("portolan_cli.extract.arcgis.orchestrator._extract_single_layer") as mock_extract,
        ):
            mock_discover.return_value = mock_discovery_result
            mock_extract.side_effect = mock_extract_side_effect

            extract_arcgis_catalog(
                url=TEST_FEATURE_SERVER_URL,
                output_dir=output_dir,
                options=ExtractionOptions(raw=False),
            )

        # Verify metadata.yaml was created
        metadata_path = output_dir / ".portolan" / "metadata.yaml"
        assert metadata_path.exists(), "metadata.yaml should be created after extraction"

        # Load and verify content
        with metadata_path.open() as f:
            metadata = yaml.safe_load(f)

        assert metadata is not None
        assert "source_url" in metadata
        assert metadata["source_url"] == TEST_FEATURE_SERVER_URL

    @pytest.mark.integration
    def test_seeded_metadata_has_todo_markers(
        self,
        tmp_path: Path,
        mock_discovery_result: ServiceDiscoveryResult,
    ) -> None:
        """Seeded metadata.yaml contains TODO markers for required human fields."""
        from portolan_cli.extract.arcgis.orchestrator import (
            ExtractionOptions,
            extract_arcgis_catalog,
        )

        output_dir = tmp_path / "output"

        def mock_extract_side_effect(
            service_url: str,
            layer: LayerInfo,
            output_path: Path,
            options: ExtractionOptions,
        ) -> tuple[int, int, float]:
            output_path.parent.mkdir(parents=True, exist_ok=True)
            output_path.write_bytes(b"PAR1")
            return (100, 1024, 1.5)

        with (
            patch("portolan_cli.extract.arcgis.orchestrator.discover_layers") as mock_discover,
            patch("portolan_cli.extract.arcgis.orchestrator._extract_single_layer") as mock_extract,
        ):
            mock_discover.return_value = mock_discovery_result
            mock_extract.side_effect = mock_extract_side_effect

            extract_arcgis_catalog(
                url=TEST_FEATURE_SERVER_URL,
                output_dir=output_dir,
                options=ExtractionOptions(raw=False),
            )

        metadata_path = output_dir / ".portolan" / "metadata.yaml"
        content = metadata_path.read_text()

        # Check for TODO markers in required fields
        assert "TODO" in content, "Should contain TODO markers for incomplete fields"

        # Load structured content
        metadata = yaml.safe_load(content)

        # contact.email should have a TODO placeholder
        contact = metadata.get("contact", {})
        email = contact.get("email", "")
        assert "TODO" in str(email) or email == "", "contact.email should be TODO or empty"

        # license should have a TODO placeholder (since licenseInfo isn't SPDX)
        license_val = metadata.get("license", "")
        assert "TODO" in str(license_val) or license_val == "", "license should be TODO or empty"

    @pytest.mark.integration
    def test_seeded_metadata_preserves_extracted_fields(
        self,
        tmp_path: Path,
        mock_discovery_result: ServiceDiscoveryResult,
    ) -> None:
        """Seeded metadata.yaml preserves fields extracted from service."""
        from portolan_cli.extract.arcgis.orchestrator import (
            ExtractionOptions,
            extract_arcgis_catalog,
        )

        output_dir = tmp_path / "output"

        def mock_extract_side_effect(
            service_url: str,
            layer: LayerInfo,
            output_path: Path,
            options: ExtractionOptions,
        ) -> tuple[int, int, float]:
            output_path.parent.mkdir(parents=True, exist_ok=True)
            output_path.write_bytes(b"PAR1")
            return (100, 1024, 1.5)

        with (
            patch("portolan_cli.extract.arcgis.orchestrator.discover_layers") as mock_discover,
            patch("portolan_cli.extract.arcgis.orchestrator._extract_single_layer") as mock_extract,
        ):
            mock_discover.return_value = mock_discovery_result
            mock_extract.side_effect = mock_extract_side_effect

            extract_arcgis_catalog(
                url=TEST_FEATURE_SERVER_URL,
                output_dir=output_dir,
                options=ExtractionOptions(raw=False),
            )

        metadata_path = output_dir / ".portolan" / "metadata.yaml"
        metadata = yaml.safe_load(metadata_path.read_text())

        # Verify extracted fields are preserved
        assert metadata.get("attribution") == "City of Philadelphia"
        assert metadata.get("contact", {}).get("name") == "Philadelphia GIS Team"
        assert metadata.get("keywords") == ["census", "demographics", "boundaries"]
        assert "quarterly" in (metadata.get("processing_notes") or "").lower()

    @pytest.mark.integration
    def test_existing_metadata_yaml_not_overwritten(
        self,
        tmp_path: Path,
        mock_discovery_result: ServiceDiscoveryResult,
    ) -> None:
        """Existing metadata.yaml is NOT overwritten (overwrite=False default)."""
        from portolan_cli.extract.arcgis.orchestrator import (
            ExtractionOptions,
            extract_arcgis_catalog,
        )

        output_dir = tmp_path / "output"

        # Pre-create metadata.yaml with custom content
        portolan_dir = output_dir / ".portolan"
        portolan_dir.mkdir(parents=True)
        metadata_path = portolan_dir / "metadata.yaml"

        existing_content = {
            "contact": {
                "name": "Existing User",
                "email": "existing@example.com",
            },
            "license": "CC-BY-4.0",
            "source_url": "https://original-source.com",
        }
        with metadata_path.open("w") as f:
            yaml.dump(existing_content, f)

        def mock_extract_side_effect(
            service_url: str,
            layer: LayerInfo,
            output_path: Path,
            options: ExtractionOptions,
        ) -> tuple[int, int, float]:
            output_path.parent.mkdir(parents=True, exist_ok=True)
            output_path.write_bytes(b"PAR1")
            return (100, 1024, 1.5)

        with (
            patch("portolan_cli.extract.arcgis.orchestrator.discover_layers") as mock_discover,
            patch("portolan_cli.extract.arcgis.orchestrator._extract_single_layer") as mock_extract,
        ):
            mock_discover.return_value = mock_discovery_result
            mock_extract.side_effect = mock_extract_side_effect

            extract_arcgis_catalog(
                url=TEST_FEATURE_SERVER_URL,
                output_dir=output_dir,
                options=ExtractionOptions(raw=False),
            )

        # Verify original content was preserved
        metadata = yaml.safe_load(metadata_path.read_text())

        assert metadata["contact"]["name"] == "Existing User"
        assert metadata["contact"]["email"] == "existing@example.com"
        assert metadata["license"] == "CC-BY-4.0"
        assert metadata["source_url"] == "https://original-source.com"

    @pytest.mark.integration
    def test_raw_mode_skips_metadata_seeding(
        self,
        tmp_path: Path,
        mock_discovery_result: ServiceDiscoveryResult,
    ) -> None:
        """raw=True mode skips auto-init and metadata seeding."""
        from portolan_cli.extract.arcgis.orchestrator import (
            ExtractionOptions,
            extract_arcgis_catalog,
        )

        output_dir = tmp_path / "output"

        def mock_extract_side_effect(
            service_url: str,
            layer: LayerInfo,
            output_path: Path,
            options: ExtractionOptions,
        ) -> tuple[int, int, float]:
            output_path.parent.mkdir(parents=True, exist_ok=True)
            output_path.write_bytes(b"PAR1")
            return (100, 1024, 1.5)

        with (
            patch("portolan_cli.extract.arcgis.orchestrator.discover_layers") as mock_discover,
            patch("portolan_cli.extract.arcgis.orchestrator._extract_single_layer") as mock_extract,
        ):
            mock_discover.return_value = mock_discovery_result
            mock_extract.side_effect = mock_extract_side_effect

            extract_arcgis_catalog(
                url=TEST_FEATURE_SERVER_URL,
                output_dir=output_dir,
                options=ExtractionOptions(raw=True),  # Raw mode
            )

        # metadata.yaml should NOT be created in raw mode
        # (since _auto_init_catalog is skipped entirely)
        metadata_path = output_dir / ".portolan" / "metadata.yaml"
        # Note: .portolan dir may still exist for extraction-report.json
        # but metadata.yaml should not be seeded
        if metadata_path.exists():
            content = metadata_path.read_text()
            # If file exists (e.g., from manual creation), it should be empty or not have seeded content
            if content.strip():
                metadata = yaml.safe_load(content)
                # Should NOT have auto-seeded source_url
                assert metadata is None or metadata.get("source_url") != TEST_FEATURE_SERVER_URL, (
                    "metadata.yaml should not be seeded in raw mode"
                )
        # File not existing is the expected case for raw mode
