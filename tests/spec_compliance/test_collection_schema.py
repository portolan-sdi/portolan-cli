"""Spec compliance tests for collection.json output.

Validates that CLI-generated collection.json files conform to the schema
defined in portolan-spec/schema/collection.schema.json.

See: https://github.com/portolan-sdi/portolan-spec/issues/23
"""

from __future__ import annotations

import json
import shutil
from pathlib import Path
from typing import TYPE_CHECKING, Any

import pytest
from click.testing import CliRunner

from portolan_cli.cli import cli

if TYPE_CHECKING:
    from collections.abc import Callable


class TestCollectionSchemaCompliance:
    """Test that collection.json output complies with the spec schema."""

    @pytest.fixture
    def runner(self) -> CliRunner:
        """Create a Click test runner."""
        return CliRunner()

    @pytest.mark.integration
    def test_add_geojson_creates_valid_collection_json(
        self,
        runner: CliRunner,
        tmp_path: Path,
        valid_points_geojson: Path,
        portolan_collection_schema: dict[str, Any],
        validate_stac: Callable[[dict[str, Any], dict[str, Any]], list[str]],
    ) -> None:
        """portolan add creates a schema-compliant collection.json for GeoJSON."""
        with runner.isolated_filesystem(temp_dir=tmp_path):
            # Initialize catalog
            result = runner.invoke(cli, ["init", "--auto"])
            assert result.exit_code == 0, f"init failed: {result.output}"

            # Copy test fixture to a subdirectory (required by portolan add)
            collection_dir = Path("points")
            collection_dir.mkdir()
            shutil.copy(valid_points_geojson, collection_dir / "points.geojson")

            # Add the file
            result = runner.invoke(cli, ["add", str(collection_dir / "points.geojson")])
            assert result.exit_code == 0, f"add failed: {result.output}"

            # Find collection.json
            collection_path = collection_dir / "collection.json"
            assert collection_path.exists(), f"collection.json not found at {collection_path}"

            data = json.loads(collection_path.read_text())
            errors = validate_stac(data, portolan_collection_schema)

            assert not errors, "Schema validation failed:\n" + "\n".join(errors)

    @pytest.mark.integration
    def test_add_geoparquet_creates_valid_collection_json(
        self,
        runner: CliRunner,
        tmp_path: Path,
        valid_points_parquet: Path,
        portolan_collection_schema: dict[str, Any],
        validate_stac: Callable[[dict[str, Any], dict[str, Any]], list[str]],
    ) -> None:
        """portolan add creates a schema-compliant collection.json for GeoParquet."""
        with runner.isolated_filesystem(temp_dir=tmp_path):
            # Initialize catalog
            result = runner.invoke(cli, ["init", "--auto"])
            assert result.exit_code == 0, f"init failed: {result.output}"

            # Copy test fixture to a subdirectory
            collection_dir = Path("buildings")
            collection_dir.mkdir()
            shutil.copy(valid_points_parquet, collection_dir / "buildings.parquet")

            # Add the file
            result = runner.invoke(cli, ["add", str(collection_dir / "buildings.parquet")])
            assert result.exit_code == 0, f"add failed: {result.output}"

            # Find collection.json
            collection_path = collection_dir / "collection.json"
            assert collection_path.exists(), "collection.json not found"

            data = json.loads(collection_path.read_text())
            errors = validate_stac(data, portolan_collection_schema)

            assert not errors, "Schema validation failed:\n" + "\n".join(errors)


class TestCollectionRequiredFields:
    """Test that collection.json contains all required STAC fields."""

    @pytest.fixture
    def runner(self) -> CliRunner:
        """Create a Click test runner."""
        return CliRunner()

    @pytest.mark.integration
    def test_collection_has_required_stac_fields(
        self,
        runner: CliRunner,
        tmp_path: Path,
        valid_points_geojson: Path,
    ) -> None:
        """collection.json MUST have type, stac_version, id, description, license, extent, links."""
        with runner.isolated_filesystem(temp_dir=tmp_path):
            runner.invoke(cli, ["init", "--auto"])

            collection_dir = Path("points")
            collection_dir.mkdir()
            shutil.copy(valid_points_geojson, collection_dir / "points.geojson")

            result = runner.invoke(cli, ["add", str(collection_dir / "points.geojson")])
            assert result.exit_code == 0, f"add failed: {result.output}"

            collection_path = collection_dir / "collection.json"
            data = json.loads(collection_path.read_text())

            required_fields = [
                "type",
                "stac_version",
                "id",
                "description",
                "license",
                "extent",
                "links",
            ]
            missing = [f for f in required_fields if f not in data]

            assert not missing, f"Missing required STAC fields: {missing}"

    @pytest.mark.integration
    def test_collection_type_is_collection(
        self,
        runner: CliRunner,
        tmp_path: Path,
        valid_points_geojson: Path,
    ) -> None:
        """collection.json type field MUST be 'Collection'."""
        with runner.isolated_filesystem(temp_dir=tmp_path):
            runner.invoke(cli, ["init", "--auto"])

            collection_dir = Path("points")
            collection_dir.mkdir()
            shutil.copy(valid_points_geojson, collection_dir / "points.geojson")
            runner.invoke(cli, ["add", str(collection_dir / "points.geojson")])

            collection_path = collection_dir / "collection.json"
            data = json.loads(collection_path.read_text())

            assert data.get("type") == "Collection", (
                f"type should be 'Collection', got: {data.get('type')}"
            )

    @pytest.mark.integration
    def test_collection_stac_version_format(
        self,
        runner: CliRunner,
        tmp_path: Path,
        valid_points_geojson: Path,
    ) -> None:
        """stac_version MUST match pattern ^1\\.[0-9]+\\.[0-9]+$."""
        import re

        with runner.isolated_filesystem(temp_dir=tmp_path):
            runner.invoke(cli, ["init", "--auto"])

            collection_dir = Path("points")
            collection_dir.mkdir()
            shutil.copy(valid_points_geojson, collection_dir / "points.geojson")
            runner.invoke(cli, ["add", str(collection_dir / "points.geojson")])

            collection_path = collection_dir / "collection.json"
            data = json.loads(collection_path.read_text())

            stac_version = data.get("stac_version")
            pattern = re.compile(r"^1\.[0-9]+\.[0-9]+$")

            assert stac_version is not None, "stac_version is missing"
            assert pattern.match(stac_version), (
                f"stac_version '{stac_version}' does not match pattern 1.x.x"
            )


class TestCollectionExtent:
    """Test that collection extent is properly formatted."""

    @pytest.fixture
    def runner(self) -> CliRunner:
        """Create a Click test runner."""
        return CliRunner()

    @pytest.mark.integration
    def test_extent_has_spatial_and_temporal(
        self,
        runner: CliRunner,
        tmp_path: Path,
        valid_points_geojson: Path,
    ) -> None:
        """extent MUST have spatial and temporal sub-objects."""
        with runner.isolated_filesystem(temp_dir=tmp_path):
            runner.invoke(cli, ["init", "--auto"])

            collection_dir = Path("points")
            collection_dir.mkdir()
            shutil.copy(valid_points_geojson, collection_dir / "points.geojson")
            runner.invoke(cli, ["add", str(collection_dir / "points.geojson")])

            collection_path = collection_dir / "collection.json"
            data = json.loads(collection_path.read_text())

            extent = data.get("extent", {})
            assert "spatial" in extent, "extent.spatial is missing"
            assert "temporal" in extent, "extent.temporal is missing"

    @pytest.mark.integration
    def test_spatial_extent_has_bbox(
        self,
        runner: CliRunner,
        tmp_path: Path,
        valid_points_geojson: Path,
    ) -> None:
        """extent.spatial MUST have bbox array."""
        with runner.isolated_filesystem(temp_dir=tmp_path):
            runner.invoke(cli, ["init", "--auto"])

            collection_dir = Path("points")
            collection_dir.mkdir()
            shutil.copy(valid_points_geojson, collection_dir / "points.geojson")
            runner.invoke(cli, ["add", str(collection_dir / "points.geojson")])

            collection_path = collection_dir / "collection.json"
            data = json.loads(collection_path.read_text())

            spatial = data.get("extent", {}).get("spatial", {})
            assert "bbox" in spatial, "extent.spatial.bbox is missing"
            assert isinstance(spatial["bbox"], list), "bbox must be an array"
            assert len(spatial["bbox"]) > 0, "bbox must not be empty"

    @pytest.mark.integration
    def test_temporal_extent_has_interval(
        self,
        runner: CliRunner,
        tmp_path: Path,
        valid_points_geojson: Path,
    ) -> None:
        """extent.temporal MUST have interval array."""
        with runner.isolated_filesystem(temp_dir=tmp_path):
            runner.invoke(cli, ["init", "--auto"])

            collection_dir = Path("points")
            collection_dir.mkdir()
            shutil.copy(valid_points_geojson, collection_dir / "points.geojson")
            runner.invoke(cli, ["add", str(collection_dir / "points.geojson")])

            collection_path = collection_dir / "collection.json"
            data = json.loads(collection_path.read_text())

            temporal = data.get("extent", {}).get("temporal", {})
            assert "interval" in temporal, "extent.temporal.interval is missing"
            assert isinstance(temporal["interval"], list), "interval must be an array"


class TestCollectionLinks:
    """Test that collection links are properly formatted."""

    @pytest.fixture
    def runner(self) -> CliRunner:
        """Create a Click test runner."""
        return CliRunner()

    @pytest.mark.integration
    def test_links_have_required_fields(
        self,
        runner: CliRunner,
        tmp_path: Path,
        valid_points_geojson: Path,
    ) -> None:
        """Each link MUST have rel and href."""
        with runner.isolated_filesystem(temp_dir=tmp_path):
            runner.invoke(cli, ["init", "--auto"])

            collection_dir = Path("points")
            collection_dir.mkdir()
            shutil.copy(valid_points_geojson, collection_dir / "points.geojson")
            runner.invoke(cli, ["add", str(collection_dir / "points.geojson")])

            collection_path = collection_dir / "collection.json"
            data = json.loads(collection_path.read_text())

            links = data.get("links", [])
            for i, link in enumerate(links):
                assert "rel" in link, f"Link {i} missing 'rel' field"
                assert "href" in link, f"Link {i} missing 'href' field"

    @pytest.mark.integration
    def test_has_root_link(
        self,
        runner: CliRunner,
        tmp_path: Path,
        valid_points_geojson: Path,
    ) -> None:
        """Collection MUST have a root link to the catalog."""
        with runner.isolated_filesystem(temp_dir=tmp_path):
            runner.invoke(cli, ["init", "--auto"])

            collection_dir = Path("points")
            collection_dir.mkdir()
            shutil.copy(valid_points_geojson, collection_dir / "points.geojson")
            runner.invoke(cli, ["add", str(collection_dir / "points.geojson")])

            collection_path = collection_dir / "collection.json"
            data = json.loads(collection_path.read_text())

            links = data.get("links", [])
            root_links = [link for link in links if link.get("rel") == "root"]

            assert root_links, "Collection missing root link"

    @pytest.mark.integration
    def test_structural_links_are_relative(
        self,
        runner: CliRunner,
        tmp_path: Path,
        valid_points_geojson: Path,
    ) -> None:
        """Root, self, parent links SHOULD be relative paths (SELF_CONTAINED)."""
        import re

        with runner.isolated_filesystem(temp_dir=tmp_path):
            runner.invoke(cli, ["init", "--auto"])

            collection_dir = Path("points")
            collection_dir.mkdir()
            shutil.copy(valid_points_geojson, collection_dir / "points.geojson")
            runner.invoke(cli, ["add", str(collection_dir / "points.geojson")])

            collection_path = collection_dir / "collection.json"
            data = json.loads(collection_path.read_text())

            links = data.get("links", [])
            structural_rels = {"root", "self", "parent", "child"}

            for link in links:
                rel = link.get("rel", "")
                href = link.get("href", "")

                if rel not in structural_rels:
                    continue

                # Check for absolute paths (should be relative)
                assert not href.startswith("/"), f"Link rel='{rel}' has Unix absolute path: {href}"
                assert not href.startswith("file://"), f"Link rel='{rel}' has file:// URL: {href}"
                assert not re.match(r"^[A-Za-z]:", href), (
                    f"Link rel='{rel}' has Windows absolute path: {href}"
                )


class TestCollectionAssets:
    """Test that collection assets are properly formatted."""

    @pytest.fixture
    def runner(self) -> CliRunner:
        """Create a Click test runner."""
        return CliRunner()

    @pytest.mark.integration
    def test_assets_have_required_href(
        self,
        runner: CliRunner,
        tmp_path: Path,
        valid_points_geojson: Path,
    ) -> None:
        """Each asset MUST have an href field."""
        with runner.isolated_filesystem(temp_dir=tmp_path):
            runner.invoke(cli, ["init", "--auto"])

            collection_dir = Path("points")
            collection_dir.mkdir()
            shutil.copy(valid_points_geojson, collection_dir / "points.geojson")
            runner.invoke(cli, ["add", str(collection_dir / "points.geojson")])

            collection_path = collection_dir / "collection.json"
            data = json.loads(collection_path.read_text())

            assets = data.get("assets", {})
            for asset_key, asset in assets.items():
                assert "href" in asset, f"Asset '{asset_key}' missing 'href' field"
                assert asset["href"], f"Asset '{asset_key}' has empty href"

    @pytest.mark.integration
    def test_geoparquet_asset_has_correct_media_type(
        self,
        runner: CliRunner,
        tmp_path: Path,
        valid_points_parquet: Path,
    ) -> None:
        """GeoParquet assets SHOULD have media type application/vnd.apache.parquet."""
        with runner.isolated_filesystem(temp_dir=tmp_path):
            runner.invoke(cli, ["init", "--auto"])

            collection_dir = Path("buildings")
            collection_dir.mkdir()
            shutil.copy(valid_points_parquet, collection_dir / "buildings.parquet")
            runner.invoke(cli, ["add", str(collection_dir / "buildings.parquet")])

            collection_path = collection_dir / "collection.json"
            data = json.loads(collection_path.read_text())

            assets = data.get("assets", {})
            parquet_assets = [
                (k, v) for k, v in assets.items() if v.get("href", "").endswith(".parquet")
            ]

            assert parquet_assets, "No parquet assets found"

            for asset_key, asset in parquet_assets:
                media_type = asset.get("type")
                # Accept either the full media type or the common variant
                valid_types = [
                    "application/vnd.apache.parquet",
                    "application/x-parquet",
                    "application/parquet",
                ]
                assert media_type in valid_types, (
                    f"Asset '{asset_key}' has unexpected media type: {media_type}. "
                    f"Expected one of: {valid_types}"
                )
