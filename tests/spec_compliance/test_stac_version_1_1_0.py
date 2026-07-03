"""Spec compliance tests pinning STAC 1.1.0 standardization (issue #568).

Portolan standardizes on STAC 1.1.0 everywhere: the shipped schemas reference
the upstream v1.1.0 STAC schemas, generated STAC documents emit ``1.1.0``, and
the default collection license is the STAC 1.1 ``other`` keyword (the deprecated
``proprietary`` value is no longer emitted).
"""

from __future__ import annotations

import json
import shutil
from pathlib import Path

import pytest
from click.testing import CliRunner

from portolan_cli.cli import cli


class TestShippedSchemasReferenceStac110:
    """The shipped Portolan schemas must $ref the upstream STAC 1.1.0 schemas."""

    @pytest.mark.integration
    def test_collection_schema_refs_stac_110(self, schemas_dir: Path) -> None:
        """collection.schema.json references the STAC v1.1.0 collection schema."""
        schema = json.loads((schemas_dir / "collection.schema.json").read_text())
        refs = [entry.get("$ref", "") for entry in schema["allOf"]]
        stac_refs = [ref for ref in refs if "schemas.stacspec.org" in ref]

        assert stac_refs, "collection.schema.json must reference an upstream STAC schema"
        assert all("/v1.1.0/" in ref for ref in stac_refs), (
            f"collection.schema.json must reference STAC v1.1.0, got: {stac_refs}"
        )

    @pytest.mark.integration
    def test_catalog_schema_refs_stac_110(self, schemas_dir: Path) -> None:
        """catalog.schema.json references the STAC v1.1.0 catalog schema."""
        schema = json.loads((schemas_dir / "catalog.schema.json").read_text())
        refs = [entry.get("$ref", "") for entry in schema["allOf"]]
        stac_refs = [ref for ref in refs if "schemas.stacspec.org" in ref]

        assert stac_refs, "catalog.schema.json must reference an upstream STAC schema"
        assert all("/v1.1.0/" in ref for ref in stac_refs), (
            f"catalog.schema.json must reference STAC v1.1.0, got: {stac_refs}"
        )


class TestGeneratedOutputIsStac110:
    """CLI-generated STAC documents must declare stac_version 1.1.0."""

    @pytest.mark.integration
    def test_init_catalog_json_is_stac_110(self, runner: CliRunner, tmp_path: Path) -> None:
        """portolan init writes catalog.json with stac_version 1.1.0."""
        with runner.isolated_filesystem(temp_dir=tmp_path):
            result = runner.invoke(cli, ["init", "--auto"])
            assert result.exit_code == 0, f"init failed: {result.output}"

            data = json.loads(Path("catalog.json").read_text())
            assert data.get("stac_version") == "1.1.0"

    @pytest.mark.integration
    def test_add_collection_json_is_stac_110_and_license_other(
        self, runner: CliRunner, tmp_path: Path, valid_points_geojson: Path
    ) -> None:
        """portolan add writes collection.json with stac_version 1.1.0 and license 'other'."""
        with runner.isolated_filesystem(temp_dir=tmp_path):
            result = runner.invoke(cli, ["init", "--auto"])
            assert result.exit_code == 0, f"init failed: {result.output}"

            collection_dir = Path("points")
            collection_dir.mkdir()
            shutil.copy(valid_points_geojson, collection_dir / "points.geojson")
            result = runner.invoke(cli, ["add", str(collection_dir / "points.geojson")])
            assert result.exit_code == 0, f"add failed: {result.output}"

            data = json.loads((collection_dir / "collection.json").read_text())
            assert data.get("stac_version") == "1.1.0"
            # STAC 1.1 deprecates "proprietary"; the default is now "other" (issue #568).
            assert data.get("license") == "other"
