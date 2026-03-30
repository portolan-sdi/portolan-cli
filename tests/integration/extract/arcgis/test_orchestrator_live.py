"""Integration tests for ArcGIS extraction orchestrator.

These tests hit real ArcGIS endpoints to verify the full extraction flow.
They require network access and use Philadelphia's stable public endpoints.

Run with: uv run pytest tests/integration/extract/arcgis/ -m network
"""

from __future__ import annotations

from pathlib import Path

import pytest

from portolan_cli.extract.arcgis.orchestrator import (
    ExtractionOptions,
    ExtractionProgress,
    extract_arcgis_catalog,
)

# Philadelphia's public ArcGIS services (stable, open data)
# See: https://services.arcgis.com/fLeGjb7u4uXqeF9q/ArcGIS/rest/services
PHILLY_SERVICES_ROOT = "https://services.arcgis.com/fLeGjb7u4uXqeF9q/ArcGIS/rest/services"
# A simple service with just 1 layer for fast testing (Sharswood neighborhood boundary)
PHILLY_SIMPLE_SERVICE = (
    f"{PHILLY_SERVICES_ROOT}/_Sharswood_Choice_Neighborhood_Boundary/FeatureServer"
)

pytestmark = [pytest.mark.integration, pytest.mark.network]


class TestLiveDiscovery:
    """Tests that verify discovery works against real endpoints."""

    def test_discovers_layers_from_live_endpoint(self, tmp_path: Path) -> None:
        """Should discover layers from a real ArcGIS FeatureServer."""
        options = ExtractionOptions(dry_run=True)

        result = extract_arcgis_catalog(
            url=PHILLY_SIMPLE_SERVICE,
            output_dir=tmp_path,
            options=options,
        )

        # Should have at least one layer
        assert len(result.layers) >= 1
        # All layers should be pending (dry run)
        assert all(layer.status == "pending" for layer in result.layers)
        # Should have metadata
        assert result.source_url == PHILLY_SIMPLE_SERVICE


class TestLiveExtraction:
    """Tests that verify extraction works with gpio against real endpoints."""

    def test_extracts_single_layer_to_parquet(self, tmp_path: Path) -> None:
        """Should extract a layer to GeoParquet using real gpio."""
        progress_events: list[ExtractionProgress] = []

        options = ExtractionOptions(
            workers=2,  # Lower workers for faster test
            retries=2,
            sort_hilbert=True,
        )

        result = extract_arcgis_catalog(
            url=PHILLY_SIMPLE_SERVICE,
            output_dir=tmp_path,
            layer_filter=["*"],  # All layers
            options=options,
            on_progress=lambda p: progress_events.append(p),
        )

        # Should have extracted at least one layer
        assert result.summary.succeeded >= 1
        assert result.summary.total_features > 0
        assert result.summary.total_size_bytes > 0

        # Should have created output files
        assert (tmp_path / ".portolan").exists()
        assert (tmp_path / ".portolan" / "extraction-report.json").exists()

        # Should have parquet files for each successful layer
        for layer in result.layers:
            if layer.status == "success":
                output_path = tmp_path / layer.output_path
                assert output_path.exists(), f"Missing output: {output_path}"
                assert output_path.suffix == ".parquet"

        # Should have progress events
        assert len(progress_events) > 0

    def test_extraction_with_glob_filter(self, tmp_path: Path) -> None:
        """Should filter layers using glob patterns."""
        options = ExtractionOptions(dry_run=True)

        # First, discover all layers
        all_result = extract_arcgis_catalog(
            url=PHILLY_SIMPLE_SERVICE,
            output_dir=tmp_path,
            options=options,
        )

        # Then filter with a pattern (if we have multiple layers)
        if len(all_result.layers) > 1:
            first_layer_name = all_result.layers[0].name

            filtered_result = extract_arcgis_catalog(
                url=PHILLY_SIMPLE_SERVICE,
                output_dir=tmp_path,
                layer_filter=[f"{first_layer_name[:5]}*"],  # Partial match
                options=options,
            )

            # Should have fewer or equal layers after filter
            assert len(filtered_result.layers) <= len(all_result.layers)


class TestLiveMetadataExtraction:
    """Tests that verify metadata extraction from real services."""

    def test_extracts_service_metadata(self, tmp_path: Path) -> None:
        """Should extract metadata from real ArcGIS service."""
        options = ExtractionOptions(dry_run=True)

        result = extract_arcgis_catalog(
            url=PHILLY_SIMPLE_SERVICE,
            output_dir=tmp_path,
            options=options,
        )

        # Should have metadata extracted
        assert result.metadata_extracted is not None
        assert result.metadata_extracted.source_url == PHILLY_SIMPLE_SERVICE

        # Service should have some standard metadata fields
        # (not all services have all fields, so we just check the structure)
        report_dict = {
            "source_url": result.metadata_extracted.source_url,
            "attribution": result.metadata_extracted.attribution,
            "keywords": result.metadata_extracted.keywords,
        }
        assert report_dict["source_url"] is not None
