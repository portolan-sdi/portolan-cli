"""Unit tests for Issue #353: Extract should add rel:via provenance link.

Tests that extraction commands add a `via` link to collection.json
pointing to the original data source URL.

STAC `via` link relation:
- rel: "via"
- href: The source URL (e.g., ArcGIS FeatureServer URL)
- type: "text/html" (for web service URLs)
- title: Describes the source (e.g., "Source ArcGIS Feature Service")
"""

from __future__ import annotations

import json
import shutil
from pathlib import Path

import pytest

from tests.support.extract import build_arcgis_catalog, make_layer_result, make_report

pytestmark = [pytest.mark.unit, pytest.mark.usefixtures("no_extract_side_requests")]


FIXTURES_DIR = Path(__file__).parent.parent / "fixtures"


class TestViaLinkGeneration:
    """Tests for adding `via` provenance link to collection.json."""

    def test_auto_init_adds_via_link_to_collection(self, tmp_path: Path) -> None:
        """build_arcgis_catalog adds via link to each collection."""
        from portolan_cli.licensing import ResolvedLicense

        output_dir = tmp_path / "test_catalog"
        output_dir.mkdir()

        # Copy a real fixture
        layer_dir = output_dir / "test_layer"
        layer_dir.mkdir()
        fixture_path = FIXTURES_DIR / "simple.parquet"
        output_parquet = layer_dir / f"{layer_dir.name}.parquet"
        shutil.copy(fixture_path, output_parquet)

        source_url = (
            "https://services.arcgis.com/abc/arcgis/rest/services/TestService/FeatureServer"
        )
        report = make_report(
            [
                make_layer_result(
                    layer_id=0,
                    name="test_layer",
                    output_path="test_layer/test_layer.parquet",
                    size_bytes=output_parquet.stat().st_size,
                )
            ],
            source_url,
        )

        build_arcgis_catalog(
            output_dir, report, ResolvedLicense(license_id="CC-BY-4.0", license_url=None)
        )

        # Verify collection.json has via link
        collection_json_path = output_dir / "test_layer" / "collection.json"
        assert collection_json_path.exists(), "collection.json should exist"

        collection_data = json.loads(collection_json_path.read_text())
        links = collection_data.get("links", [])

        # Find via link
        via_links = [link for link in links if link.get("rel") == "via"]
        assert len(via_links) == 1, "Should have exactly one via link"

        via_link = via_links[0]
        # Via link uses layer-specific URL (service URL + layer ID)
        expected_url = f"{source_url}/0"  # Layer ID 0
        assert via_link["href"] == expected_url, (
            f"via href should be layer URL, got {via_link['href']}"
        )
        assert via_link.get("type") == "text/html", "via type should be text/html"
        assert "title" in via_link, "via link should have a title"

    def test_via_link_uses_layer_specific_url_when_available(self, tmp_path: Path) -> None:
        """When layer URL differs from service URL, use layer-specific URL."""
        from portolan_cli.licensing import ResolvedLicense

        output_dir = tmp_path / "test_catalog"
        output_dir.mkdir()

        layer_dir = output_dir / "census_tracts"
        layer_dir.mkdir()
        fixture_path = FIXTURES_DIR / "simple.parquet"
        output_parquet = layer_dir / f"{layer_dir.name}.parquet"
        shutil.copy(fixture_path, output_parquet)

        # Service URL is the FeatureServer, but layer is index 3
        service_url = (
            "https://services.arcgis.com/abc/arcgis/rest/services/Demographics/FeatureServer"
        )
        layer_url = f"{service_url}/3"

        report = make_report(
            [
                make_layer_result(
                    layer_id=3,
                    name="Census Tracts",
                    output_path="census_tracts/census_tracts.parquet",
                    size_bytes=output_parquet.stat().st_size,
                )
            ],
            service_url,
        )

        build_arcgis_catalog(
            output_dir, report, ResolvedLicense(license_id="CC-BY-4.0", license_url=None)
        )

        # Verify via link points to layer-specific URL
        collection_json_path = output_dir / "census_tracts" / "collection.json"
        collection_data = json.loads(collection_json_path.read_text())
        links = collection_data.get("links", [])

        via_links = [link for link in links if link.get("rel") == "via"]
        assert len(via_links) == 1

        # Should use layer URL (service URL + layer index)
        expected_url = layer_url
        assert via_links[0]["href"] == expected_url

    def test_no_via_link_for_failed_layers(self, tmp_path: Path) -> None:
        """Failed layers don't get collections, so no via link needed."""
        from portolan_cli.licensing import ResolvedLicense

        output_dir = tmp_path / "test_catalog"
        output_dir.mkdir()
        (output_dir / ".portolan").mkdir()

        source_url = "https://services.arcgis.com/abc/arcgis/rest/services/Test/FeatureServer"
        report = make_report(
            [
                make_layer_result(
                    layer_id=0,
                    name="Failed Layer",
                    status="failed",
                    output_path=None,
                    error="Network error",
                )
            ],
            source_url,
        )

        # Should not create catalog (no successful extractions)
        build_arcgis_catalog(
            output_dir, report, ResolvedLicense(license_id="CC-BY-4.0", license_url=None)
        )

        # No collection.json should exist
        assert not (output_dir / "failed_layer" / "collection.json").exists()


class TestViaLinkHelper:
    """Tests for the via link helper function."""

    def test_add_via_link_to_collection(self, tmp_path: Path) -> None:
        """add_via_link modifies collection.json in place."""
        from portolan_cli.stac import add_via_link

        # Create a minimal collection.json
        collection_path = tmp_path / "collection.json"
        collection_data = {
            "type": "Collection",
            "id": "test",
            "stac_version": "1.1.0",
            "links": [
                {"rel": "self", "href": "./collection.json"},
                {"rel": "root", "href": "../catalog.json"},
            ],
        }
        collection_path.write_text(json.dumps(collection_data))

        source_url = "https://example.com/arcgis/rest/services/Test/FeatureServer/0"
        add_via_link(collection_path, source_url)

        # Read back and verify
        updated = json.loads(collection_path.read_text())
        via_links = [link for link in updated["links"] if link.get("rel") == "via"]

        assert len(via_links) == 1
        assert via_links[0]["href"] == source_url
        assert via_links[0]["type"] == "text/html"
        assert "title" in via_links[0]

    def test_add_via_link_idempotent(self, tmp_path: Path) -> None:
        """Adding same via link twice doesn't duplicate."""
        from portolan_cli.stac import add_via_link

        collection_path = tmp_path / "collection.json"
        collection_data = {
            "type": "Collection",
            "id": "test",
            "stac_version": "1.1.0",
            "links": [],
        }
        collection_path.write_text(json.dumps(collection_data))

        source_url = "https://example.com/service"

        # Add twice
        add_via_link(collection_path, source_url)
        add_via_link(collection_path, source_url)

        # Should still have only one via link
        updated = json.loads(collection_path.read_text())
        via_links = [link for link in updated["links"] if link.get("rel") == "via"]
        assert len(via_links) == 1

    def test_add_via_link_custom_title(self, tmp_path: Path) -> None:
        """add_via_link accepts optional custom title."""
        from portolan_cli.stac import add_via_link

        collection_path = tmp_path / "collection.json"
        collection_data = {
            "type": "Collection",
            "id": "test",
            "stac_version": "1.1.0",
            "links": [],
        }
        collection_path.write_text(json.dumps(collection_data))

        source_url = "https://example.com/service"
        add_via_link(collection_path, source_url, title="Custom Source Title")

        updated = json.loads(collection_path.read_text())
        via_links = [link for link in updated["links"] if link.get("rel") == "via"]
        assert via_links[0]["title"] == "Custom Source Title"
