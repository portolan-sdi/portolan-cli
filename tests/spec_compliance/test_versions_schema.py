"""Spec compliance tests for versions.json output.

Validates that CLI-generated versions.json files conform to the schema
defined in portolan-spec/schema/versions.schema.json.

Note: There are TWO different versions.json structures:
- Catalog-level (root): schema_version, catalog_id, created, collections
- Collection-level: spec_version, current_version, versions

The portolan-spec schema (versions.schema.json) describes the collection-level
format. Catalog-level uses catalog-versions.schema.json.

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


class TestCatalogVersionsSchemaCompliance:
    """Test that catalog-level versions.json complies with the schema."""

    @pytest.mark.integration
    def test_init_creates_valid_catalog_versions_json(
        self,
        runner: CliRunner,
        tmp_path: Path,
        catalog_versions_schema: dict[str, Any],
        validate_versions: Callable[[dict[str, Any], dict[str, Any]], list[str]],
    ) -> None:
        """portolan init creates a schema-compliant catalog-level versions.json."""
        with runner.isolated_filesystem(temp_dir=tmp_path):
            result = runner.invoke(cli, ["init", "--auto"])
            assert result.exit_code == 0, f"init failed: {result.output}"

            versions_path = Path("versions.json")
            assert versions_path.exists(), "versions.json not created"

            data = json.loads(versions_path.read_text())
            errors = validate_versions(data, catalog_versions_schema)

            assert not errors, "Schema validation failed:\n" + "\n".join(errors)


class TestCollectionVersionsSchemaCompliance:
    """Test that collection-level versions.json complies with the spec schema."""

    @pytest.mark.integration
    def test_add_creates_valid_versions_json(
        self,
        runner: CliRunner,
        tmp_path: Path,
        valid_points_geojson: Path,
        versions_schema: dict[str, Any],
        validate_versions: Callable[[dict[str, Any], dict[str, Any]], list[str]],
    ) -> None:
        """portolan add creates a schema-compliant collection versions.json."""
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

            # Check collection versions.json
            versions_path = collection_dir / "versions.json"
            assert versions_path.exists(), f"Collection versions.json not found at {versions_path}"

            data = json.loads(versions_path.read_text())
            errors = validate_versions(data, versions_schema)

            assert not errors, "Schema validation failed:\n" + "\n".join(errors)

    @pytest.mark.integration
    def test_add_geoparquet_creates_valid_versions_json(
        self,
        runner: CliRunner,
        tmp_path: Path,
        valid_points_parquet: Path,
        versions_schema: dict[str, Any],
        validate_versions: Callable[[dict[str, Any], dict[str, Any]], list[str]],
    ) -> None:
        """portolan add with GeoParquet creates a schema-compliant versions.json."""
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

            # Check collection versions.json
            versions_path = collection_dir / "versions.json"
            assert versions_path.exists(), "Collection versions.json not found"

            data = json.loads(versions_path.read_text())
            errors = validate_versions(data, versions_schema)

            assert not errors, "Schema validation failed:\n" + "\n".join(errors)


class TestVersionsSemanticRules:
    """Test semantic rules from rules.yaml that can't be expressed in JSON Schema."""

    @pytest.mark.integration
    def test_rule_0012_current_version_consistency(
        self,
        runner: CliRunner,
        tmp_path: Path,
        valid_points_geojson: Path,
        validate_rule_0012: Callable[[dict[str, Any]], list[str]],
    ) -> None:
        """RULE-0012: current_version MUST match last entry in versions array."""
        with runner.isolated_filesystem(temp_dir=tmp_path):
            result = runner.invoke(cli, ["init", "--auto"])
            assert result.exit_code == 0, f"init failed: {result.output}"

            collection_dir = Path("points")
            collection_dir.mkdir()
            shutil.copy(valid_points_geojson, collection_dir / "points.geojson")

            result = runner.invoke(cli, ["add", str(collection_dir / "points.geojson")])
            assert result.exit_code == 0, f"add failed: {result.output}"

            versions_path = collection_dir / "versions.json"
            data = json.loads(versions_path.read_text())

            errors = validate_rule_0012(data)
            assert not errors, "Rule validation failed:\n" + "\n".join(errors)

    @pytest.mark.integration
    def test_rule_0013_changes_reference_assets(
        self,
        runner: CliRunner,
        tmp_path: Path,
        valid_points_geojson: Path,
        validate_rule_0013: Callable[[dict[str, Any]], list[str]],
    ) -> None:
        """RULE-0013: changes array MUST only reference keys that exist in assets."""
        with runner.isolated_filesystem(temp_dir=tmp_path):
            result = runner.invoke(cli, ["init", "--auto"])
            assert result.exit_code == 0, f"init failed: {result.output}"

            collection_dir = Path("points")
            collection_dir.mkdir()
            shutil.copy(valid_points_geojson, collection_dir / "points.geojson")

            result = runner.invoke(cli, ["add", str(collection_dir / "points.geojson")])
            assert result.exit_code == 0, f"add failed: {result.output}"

            versions_path = collection_dir / "versions.json"
            data = json.loads(versions_path.read_text())

            errors = validate_rule_0013(data)
            assert not errors, "Rule validation failed:\n" + "\n".join(errors)

    @pytest.mark.integration
    def test_rule_0014_version_uniqueness(
        self,
        runner: CliRunner,
        tmp_path: Path,
        valid_points_geojson: Path,
        validate_rule_0014: Callable[[dict[str, Any]], list[str]],
    ) -> None:
        """RULE-0014: Version strings MUST be unique within versions array."""
        with runner.isolated_filesystem(temp_dir=tmp_path):
            result = runner.invoke(cli, ["init", "--auto"])
            assert result.exit_code == 0, f"init failed: {result.output}"

            collection_dir = Path("points")
            collection_dir.mkdir()
            shutil.copy(valid_points_geojson, collection_dir / "points.geojson")

            result = runner.invoke(cli, ["add", str(collection_dir / "points.geojson")])
            assert result.exit_code == 0, f"add failed: {result.output}"

            versions_path = collection_dir / "versions.json"
            data = json.loads(versions_path.read_text())

            errors = validate_rule_0014(data)
            assert not errors, "Rule validation failed:\n" + "\n".join(errors)


class TestVersionsTimestampFormat:
    """Test that timestamps comply with the ISO 8601 UTC format (ending in 'Z')."""

    @pytest.mark.integration
    def test_created_timestamp_format(
        self,
        runner: CliRunner,
        tmp_path: Path,
        valid_points_geojson: Path,
    ) -> None:
        """Version timestamps MUST be ISO 8601 UTC (ending with 'Z')."""
        import re

        with runner.isolated_filesystem(temp_dir=tmp_path):
            result = runner.invoke(cli, ["init", "--auto"])
            assert result.exit_code == 0, f"init failed: {result.output}"

            collection_dir = Path("points")
            collection_dir.mkdir()
            shutil.copy(valid_points_geojson, collection_dir / "points.geojson")

            result = runner.invoke(cli, ["add", str(collection_dir / "points.geojson")])
            assert result.exit_code == 0, f"add failed: {result.output}"

            versions_path = collection_dir / "versions.json"
            data = json.loads(versions_path.read_text())

            # The schema pattern from versions.schema.json
            timestamp_pattern = re.compile(
                r"^[0-9]{4}-[0-9]{2}-[0-9]{2}T[0-9]{2}:[0-9]{2}:[0-9]{2}(\.[0-9]+)?Z$"
            )

            for i, version in enumerate(data.get("versions", [])):
                created = version.get("created")
                assert created is not None, f"Version {i} missing 'created' field"
                assert timestamp_pattern.match(created), (
                    f"Version {i} has invalid timestamp format: {created}. "
                    f"Expected ISO 8601 UTC (e.g., '2024-01-15T10:30:00Z')"
                )


class TestVersionsChecksumFormat:
    """Test that checksums comply with the SHA-256 hex format."""

    @pytest.mark.integration
    def test_sha256_checksum_format(
        self,
        runner: CliRunner,
        tmp_path: Path,
        valid_points_geojson: Path,
    ) -> None:
        """Asset checksums MUST be 64-character lowercase hex strings."""
        import re

        with runner.isolated_filesystem(temp_dir=tmp_path):
            result = runner.invoke(cli, ["init", "--auto"])
            assert result.exit_code == 0, f"init failed: {result.output}"

            collection_dir = Path("points")
            collection_dir.mkdir()
            shutil.copy(valid_points_geojson, collection_dir / "points.geojson")

            result = runner.invoke(cli, ["add", str(collection_dir / "points.geojson")])
            assert result.exit_code == 0, f"add failed: {result.output}"

            versions_path = collection_dir / "versions.json"
            data = json.loads(versions_path.read_text())

            # SHA-256 pattern from schema (accepts both upper and lowercase)
            sha256_pattern = re.compile(r"^[a-fA-F0-9]{64}$")

            for version in data.get("versions", []):
                for asset_key, asset in version.get("assets", {}).items():
                    sha256 = asset.get("sha256")
                    assert sha256 is not None, f"Asset '{asset_key}' missing 'sha256'"
                    assert sha256_pattern.match(sha256), (
                        f"Asset '{asset_key}' has invalid SHA-256 format: {sha256}"
                    )
