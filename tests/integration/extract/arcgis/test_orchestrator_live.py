"""Integration tests for ArcGIS extraction orchestrator.

These tests hit real ArcGIS endpoints to verify the full extraction flow.
They require network access and use Philadelphia's stable public endpoints.

Run with: uv run pytest tests/integration/extract/arcgis/ -m network
"""

from __future__ import annotations

from pathlib import Path

import httpx
import pytest

from portolan_cli.extract.arcgis.discovery import ArcGISDiscoveryError
from portolan_cli.extract.arcgis.orchestrator import (
    ExtractionOptions,
    ServicesRootDiscoveryResult,
    extract_arcgis_catalog,
    list_services,
)
from portolan_cli.extract.common.progress import ExtractionProgress

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


# South African national data portal — services live in a NationalDatasets folder.
SA_ROOT = "https://nspdr.dlrrd.gov.za/server/rest/services"
# JRC federated server — ALL services are in folders, zero top-level services.
JRC_ROOT = "https://arcgis-maps.jrc.ec.europa.eu/federated_server/rest/services"


def _list_services_or_skip(root: str, *, timeout: float = 60.0) -> ServicesRootDiscoveryResult:
    """Call ``list_services``, skipping only on a genuine connectivity outage.

    The folder-traversal tests below point at third-party government / EU ArcGIS
    servers that periodically go offline. A live external-dependency outage must
    not hard-fail the suite, but a *real* failure a live-up server can surface
    (an HTTP error, invalid JSON, or an embedded ArcGIS error payload) must still
    fail loudly so regressions are caught.

    ``ArcGISDiscoveryError`` is a flat type with no connectivity/HTTP subclass or
    error-code attribute, so we discriminate via the cause chain: ``_fetch_json``
    wraps transport failures as ``... from <httpx.RequestError>`` (connection
    refused, DNS failure, timeout), whereas HTTP-status (>=400, incl. 499),
    invalid-JSON, and embedded-error cases carry no ``httpx.RequestError`` cause.
    Only the transport case is treated as an unreachable endpoint and skipped;
    everything else is re-raised.
    """
    try:
        return list_services(root, timeout=timeout)
    except ArcGISDiscoveryError as exc:
        if isinstance(exc.__cause__, httpx.RequestError):
            pytest.skip(f"live ArcGIS endpoint unreachable: {exc}")
        raise


@pytest.mark.network
@pytest.mark.slow
def test_sa_root_traverses_folders() -> None:
    result = _list_services_or_skip(SA_ROOT, timeout=60.0)
    # This test asserts the NationalDatasets folder's services are surfaced, so
    # it can only run if that folder was actually traversed. Folder-level fetch
    # errors are recorded (never raised) in coverage.folders_skipped; when the
    # live server leaves NationalDatasets un-traversed the run is an outage, not
    # a recursion regression, so skip before asserting. A healthy server
    # traverses it and the assertions run.
    if result.coverage is not None and "NationalDatasets" in {
        folder for folder, _reason in result.coverage.folders_skipped
    }:
        pytest.skip("NationalDatasets folder unreachable on live SA endpoint")
    names = [s.name for s in result.services]
    # NationalDatasets services live in a folder; recursion must surface them
    assert any(n.startswith("NationalDatasets/") for n in names)
    assert result.coverage is not None
    assert "NationalDatasets" in result.coverage.folders_visited


@pytest.mark.network
@pytest.mark.slow
def test_jrc_root_has_only_folder_services() -> None:
    # JRC root has ZERO top-level services; everything is in folders.
    result = _list_services_or_skip(JRC_ROOT, timeout=60.0)
    # A total outage can leave the root reachable but every folder un-traversed,
    # yielding zero services (an outage, not a regression). Gate on "nothing
    # surfaced": routinely secured system folders (e.g. Utilities, which returns
    # 499 Token Required on healthy servers too) land in folders_skipped and must
    # NOT skip this test, so a non-empty folders_skipped alone is not enough.
    if not result.services and result.coverage is not None and result.coverage.folders_skipped:
        pytest.skip("live JRC endpoint degraded; no services surfaced")
    assert len(result.services) > 0
    assert all("/" in s.name for s in result.services)
