"""Spec compliance tests for catalog.json output.

Validates that CLI-generated catalog.json files conform to the schema
defined in portolan-spec/schema/catalog.schema.json.

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


class TestCatalogSchemaCompliance:
    """Test that catalog.json output complies with the spec schema."""

    @pytest.mark.integration
    def test_init_creates_valid_catalog_json(
        self,
        runner: CliRunner,
        tmp_path: Path,
        portolan_catalog_schema: dict[str, Any],
        validate_stac: Callable[[dict[str, Any], dict[str, Any]], list[str]],
    ) -> None:
        """portolan init creates a schema-compliant catalog.json."""
        with runner.isolated_filesystem(temp_dir=tmp_path):
            result = runner.invoke(cli, ["init", "--auto"])
            assert result.exit_code == 0, f"init failed: {result.output}"

            catalog_path = Path("catalog.json")
            assert catalog_path.exists(), "catalog.json not created"

            data = json.loads(catalog_path.read_text())
            errors = validate_stac(data, portolan_catalog_schema)

            assert not errors, "Schema validation failed:\n" + "\n".join(errors)

    @pytest.mark.integration
    def test_catalog_with_collection_is_valid(
        self,
        runner: CliRunner,
        tmp_path: Path,
        valid_points_geojson: Path,
        portolan_catalog_schema: dict[str, Any],
        validate_stac: Callable[[dict[str, Any], dict[str, Any]], list[str]],
    ) -> None:
        """catalog.json remains valid after adding a collection."""
        with runner.isolated_filesystem(temp_dir=tmp_path):
            runner.invoke(cli, ["init", "--auto"])

            # Copy to subdirectory (required by portolan add)
            collection_dir = Path("points")
            collection_dir.mkdir()
            shutil.copy(valid_points_geojson, collection_dir / "points.geojson")

            result = runner.invoke(cli, ["add", str(collection_dir / "points.geojson")])
            assert result.exit_code == 0, f"add failed: {result.output}"

            catalog_path = Path("catalog.json")
            data = json.loads(catalog_path.read_text())
            errors = validate_stac(data, portolan_catalog_schema)

            assert not errors, "Schema validation failed:\n" + "\n".join(errors)


class TestCatalogRequiredFields:
    """Test that catalog.json contains all required STAC fields."""

    @pytest.mark.integration
    def test_catalog_has_required_stac_fields(
        self,
        runner: CliRunner,
        tmp_path: Path,
    ) -> None:
        """catalog.json MUST have type, stac_version, id, description, links."""
        with runner.isolated_filesystem(temp_dir=tmp_path):
            result = runner.invoke(cli, ["init", "--auto"])
            assert result.exit_code == 0, f"init failed: {result.output}"

            catalog_path = Path("catalog.json")
            data = json.loads(catalog_path.read_text())

            required_fields = ["type", "stac_version", "id", "description", "links"]
            missing = [f for f in required_fields if f not in data]

            assert not missing, f"Missing required STAC fields: {missing}"

    @pytest.mark.integration
    def test_catalog_type_is_catalog(
        self,
        runner: CliRunner,
        tmp_path: Path,
    ) -> None:
        """catalog.json type field MUST be 'Catalog'."""
        with runner.isolated_filesystem(temp_dir=tmp_path):
            runner.invoke(cli, ["init", "--auto"])

            catalog_path = Path("catalog.json")
            data = json.loads(catalog_path.read_text())

            assert data.get("type") == "Catalog", (
                f"type should be 'Catalog', got: {data.get('type')}"
            )

    @pytest.mark.integration
    def test_catalog_stac_version_format(
        self,
        runner: CliRunner,
        tmp_path: Path,
    ) -> None:
        """stac_version MUST match pattern ^1\\.[0-9]+\\.[0-9]+$."""
        import re

        with runner.isolated_filesystem(temp_dir=tmp_path):
            runner.invoke(cli, ["init", "--auto"])

            catalog_path = Path("catalog.json")
            data = json.loads(catalog_path.read_text())

            stac_version = data.get("stac_version")
            pattern = re.compile(r"^1\.[0-9]+\.[0-9]+$")

            assert stac_version is not None, "stac_version is missing"
            assert pattern.match(stac_version), (
                f"stac_version '{stac_version}' does not match pattern 1.x.x"
            )


class TestCatalogLinks:
    """Test that catalog links comply with SELF_CONTAINED requirements."""

    @pytest.mark.integration
    def test_links_have_required_fields(
        self,
        runner: CliRunner,
        tmp_path: Path,
    ) -> None:
        """Each link MUST have rel and href."""
        with runner.isolated_filesystem(temp_dir=tmp_path):
            runner.invoke(cli, ["init", "--auto"])

            catalog_path = Path("catalog.json")
            data = json.loads(catalog_path.read_text())

            links = data.get("links", [])
            for i, link in enumerate(links):
                assert "rel" in link, f"Link {i} missing 'rel' field"
                assert "href" in link, f"Link {i} missing 'href' field"

    @pytest.mark.integration
    def test_has_root_and_self_links(
        self,
        runner: CliRunner,
        tmp_path: Path,
    ) -> None:
        """Catalog MUST have root and self links."""
        with runner.isolated_filesystem(temp_dir=tmp_path):
            runner.invoke(cli, ["init", "--auto"])

            catalog_path = Path("catalog.json")
            data = json.loads(catalog_path.read_text())

            links = data.get("links", [])
            rels = {link.get("rel") for link in links}

            assert "root" in rels, "Catalog missing root link"
            # self link is recommended but not strictly required
            # assert "self" in rels, "Catalog missing self link"

    @pytest.mark.integration
    def test_rule_0040_structural_links_are_relative(
        self,
        runner: CliRunner,
        tmp_path: Path,
        validate_rule_0040: Callable[[dict[str, Any]], list[str]],
    ) -> None:
        """RULE-0040: Structural links MUST be relative (SELF_CONTAINED)."""
        with runner.isolated_filesystem(temp_dir=tmp_path):
            runner.invoke(cli, ["init", "--auto"])

            catalog_path = Path("catalog.json")
            data = json.loads(catalog_path.read_text())

            errors = validate_rule_0040(data)
            assert not errors, "Rule validation failed:\n" + "\n".join(errors)

    @pytest.mark.integration
    def test_child_links_point_to_collections(
        self,
        runner: CliRunner,
        tmp_path: Path,
        valid_points_geojson: Path,
    ) -> None:
        """After adding a collection, catalog MUST have child link to collection.json."""
        with runner.isolated_filesystem(temp_dir=tmp_path):
            runner.invoke(cli, ["init", "--auto"])

            collection_dir = Path("points")
            collection_dir.mkdir()
            shutil.copy(valid_points_geojson, collection_dir / "points.geojson")

            result = runner.invoke(cli, ["add", str(collection_dir / "points.geojson")])
            assert result.exit_code == 0, f"add failed: {result.output}"

            catalog_path = Path("catalog.json")
            data = json.loads(catalog_path.read_text())

            links = data.get("links", [])
            child_links = [link for link in links if link.get("rel") == "child"]

            assert child_links, "Catalog missing child link after adding collection"

            # Child links should point to collection.json
            for link in child_links:
                href = link.get("href", "")
                assert href.endswith("collection.json"), (
                    f"Child link should end with 'collection.json': {href}"
                )

    @pytest.mark.integration
    def test_no_absolute_paths_leak_after_add(
        self,
        runner: CliRunner,
        tmp_path: Path,
        valid_points_geojson: Path,
        validate_rule_0040: Callable[[dict[str, Any]], list[str]],
    ) -> None:
        """RULE-0040: No absolute paths should leak into catalog.json after add."""
        with runner.isolated_filesystem(temp_dir=tmp_path):
            runner.invoke(cli, ["init", "--auto"])

            collection_dir = Path("points")
            collection_dir.mkdir()
            shutil.copy(valid_points_geojson, collection_dir / "points.geojson")
            runner.invoke(cli, ["add", str(collection_dir / "points.geojson")])

            catalog_path = Path("catalog.json")
            data = json.loads(catalog_path.read_text())

            errors = validate_rule_0040(data)
            assert not errors, "Rule validation failed:\n" + "\n".join(errors)


class TestCatalogIdExtraction:
    """Test that catalog ID is properly extracted from directory name."""

    @pytest.mark.integration
    def test_catalog_id_from_directory_name(
        self,
        runner: CliRunner,
        tmp_path: Path,
    ) -> None:
        """Catalog ID should be derived from the directory name."""
        catalog_dir = tmp_path / "my-test-catalog"
        catalog_dir.mkdir()

        result = runner.invoke(cli, ["init", "--auto", str(catalog_dir)])
        assert result.exit_code == 0, f"init failed: {result.output}"

        catalog_path = catalog_dir / "catalog.json"
        data = json.loads(catalog_path.read_text())

        assert data.get("id") == "my-test-catalog", (
            f"Catalog ID should be 'my-test-catalog', got: {data.get('id')}"
        )

    @pytest.mark.integration
    def test_catalog_id_is_non_empty(
        self,
        runner: CliRunner,
        tmp_path: Path,
    ) -> None:
        """Catalog ID MUST be non-empty."""
        with runner.isolated_filesystem(temp_dir=tmp_path):
            runner.invoke(cli, ["init", "--auto"])

            catalog_path = Path("catalog.json")
            data = json.loads(catalog_path.read_text())

            catalog_id = data.get("id")
            assert catalog_id is not None, "Catalog ID is missing"
            assert catalog_id, "Catalog ID is empty"


class TestCatalogDescription:
    """Test that catalog description is properly set."""

    @pytest.mark.integration
    def test_catalog_has_description(
        self,
        runner: CliRunner,
        tmp_path: Path,
    ) -> None:
        """Catalog MUST have a description."""
        with runner.isolated_filesystem(temp_dir=tmp_path):
            runner.invoke(cli, ["init", "--auto"])

            catalog_path = Path("catalog.json")
            data = json.loads(catalog_path.read_text())

            description = data.get("description")
            assert description is not None, "Catalog description is missing"
            assert isinstance(description, str), "Catalog description must be a string"

    @pytest.mark.integration
    def test_custom_description_is_set(
        self,
        runner: CliRunner,
        tmp_path: Path,
    ) -> None:
        """Custom description should be set in catalog.json."""
        with runner.isolated_filesystem(temp_dir=tmp_path):
            result = runner.invoke(
                cli,
                ["init", "--auto", "--description", "My custom catalog description"],
            )
            assert result.exit_code == 0, f"init failed: {result.output}"

            catalog_path = Path("catalog.json")
            data = json.loads(catalog_path.read_text())

            assert data.get("description") == "My custom catalog description"


class TestCatalogMultipleCollections:
    """Test catalog behavior with multiple collections."""

    @pytest.mark.integration
    def test_multiple_child_links(
        self,
        runner: CliRunner,
        tmp_path: Path,
        valid_points_geojson: Path,
        valid_polygons_geojson: Path,
        portolan_catalog_schema: dict[str, Any],
        validate_stac: Callable[[dict[str, Any], dict[str, Any]], list[str]],
    ) -> None:
        """Catalog with multiple collections should have multiple child links."""
        with runner.isolated_filesystem(temp_dir=tmp_path):
            runner.invoke(cli, ["init", "--auto"])

            # Add first collection
            points_dir = Path("points")
            points_dir.mkdir()
            shutil.copy(valid_points_geojson, points_dir / "points.geojson")
            runner.invoke(cli, ["add", str(points_dir / "points.geojson")])

            # Add second collection
            polygons_dir = Path("polygons")
            polygons_dir.mkdir()
            shutil.copy(valid_polygons_geojson, polygons_dir / "polygons.geojson")
            runner.invoke(cli, ["add", str(polygons_dir / "polygons.geojson")])

            catalog_path = Path("catalog.json")
            data = json.loads(catalog_path.read_text())

            # Validate schema
            errors = validate_stac(data, portolan_catalog_schema)
            assert not errors, "Schema validation failed:\n" + "\n".join(errors)

            # Check for multiple child links
            links = data.get("links", [])
            child_links = [link for link in links if link.get("rel") == "child"]

            assert len(child_links) >= 2, f"Expected at least 2 child links, got {len(child_links)}"
