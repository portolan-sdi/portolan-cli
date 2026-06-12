"""Integration tests for file:size population (Issue #501).

Verifies that `portolan add` correctly populates file:size and file:checksum
on STAC assets, and that collection/catalog aggregates are computed.
"""

from __future__ import annotations

import json
import shutil
from pathlib import Path

import pytest
from click.testing import CliRunner

from portolan_cli.cli import cli


@pytest.fixture
def runner() -> CliRunner:
    """Create a CLI test runner."""
    return CliRunner()


@pytest.fixture
def initialized_catalog(tmp_path: Path) -> Path:
    """Create an initialized Portolan catalog using CLI."""
    result = CliRunner().invoke(cli, ["init", str(tmp_path), "--auto"])
    assert result.exit_code == 0, f"Init failed: {result.output}"
    return tmp_path


@pytest.fixture
def sample_parquet_fixture() -> Path:
    """Path to sample GeoParquet fixture."""
    fixture_path = Path(__file__).parent.parent / "fixtures" / "simple.parquet"
    if not fixture_path.exists():
        pytest.skip("GeoParquet fixture not found")
    return fixture_path


@pytest.fixture
def sample_parquet_in_catalog(initialized_catalog: Path, sample_parquet_fixture: Path) -> Path:
    """Copy sample parquet into catalog for testing."""
    collection_dir = initialized_catalog / "test-data"
    collection_dir.mkdir()
    target = collection_dir / "data.parquet"
    shutil.copy(sample_parquet_fixture, target)
    return target


class TestFileSizePopulation:
    """Tests for file:size and file:checksum population."""

    @pytest.mark.integration
    def test_add_populates_file_size_on_asset(
        self, runner: CliRunner, initialized_catalog: Path, sample_parquet_in_catalog: Path
    ) -> None:
        """portolan add should populate file:size on STAC assets."""
        # Act
        result = runner.invoke(
            cli,
            ["add", "--portolan-dir", str(initialized_catalog), str(sample_parquet_in_catalog)],
            catch_exceptions=False,
        )

        # Assert - add succeeded
        assert result.exit_code == 0, f"Add failed: {result.output}"

        # Find collection.json
        collection_jsons = list(initialized_catalog.rglob("collection.json"))
        assert len(collection_jsons) == 1, "Expected exactly one collection.json"

        collection_data = json.loads(collection_jsons[0].read_text())
        assets = collection_data.get("assets", {})

        # At least one asset should exist
        assert len(assets) > 0, "No assets found in collection"

        # Check that file:size is populated
        for asset_key, asset in assets.items():
            assert "file:size" in asset, f"Asset {asset_key} missing file:size"
            assert isinstance(asset["file:size"], int), "file:size should be int"
            assert asset["file:size"] > 0, "file:size should be positive"

    @pytest.mark.integration
    def test_add_populates_file_checksum_on_asset(
        self, runner: CliRunner, initialized_catalog: Path, sample_parquet_in_catalog: Path
    ) -> None:
        """portolan add should populate file:checksum on STAC assets."""
        # Act
        result = runner.invoke(
            cli,
            ["add", "--portolan-dir", str(initialized_catalog), str(sample_parquet_in_catalog)],
            catch_exceptions=False,
        )

        assert result.exit_code == 0, f"Add failed: {result.output}"

        # Find collection.json
        collection_jsons = list(initialized_catalog.rglob("collection.json"))
        collection_data = json.loads(collection_jsons[0].read_text())
        assets = collection_data.get("assets", {})

        # Check that file:checksum is populated with sha256 prefix
        for asset_key, asset in assets.items():
            assert "file:checksum" in asset, f"Asset {asset_key} missing file:checksum"
            checksum = asset["file:checksum"]
            assert checksum.startswith("sha256:"), "Checksum should start with sha256:"
            assert len(checksum) > 70, f"Checksum too short: {checksum}"

    @pytest.mark.integration
    def test_add_populates_collection_aggregates(
        self, runner: CliRunner, initialized_catalog: Path, sample_parquet_in_catalog: Path
    ) -> None:
        """portolan add should populate portolan:total_size_bytes on collection."""
        # Act
        result = runner.invoke(
            cli,
            ["add", "--portolan-dir", str(initialized_catalog), str(sample_parquet_in_catalog)],
            catch_exceptions=False,
        )

        assert result.exit_code == 0, f"Add failed: {result.output}"

        # Find collection.json
        collection_jsons = list(initialized_catalog.rglob("collection.json"))
        collection_data = json.loads(collection_jsons[0].read_text())

        # Check aggregates
        assert "portolan:total_size_bytes" in collection_data
        assert "portolan:asset_count" in collection_data
        assert collection_data["portolan:total_size_bytes"] > 0
        assert collection_data["portolan:asset_count"] > 0

    @pytest.mark.integration
    def test_add_populates_catalog_aggregates(
        self, runner: CliRunner, initialized_catalog: Path, sample_parquet_in_catalog: Path
    ) -> None:
        """portolan add should populate portolan:total_size_bytes on catalog."""
        # Act
        result = runner.invoke(
            cli,
            ["add", "--portolan-dir", str(initialized_catalog), str(sample_parquet_in_catalog)],
            catch_exceptions=False,
        )

        assert result.exit_code == 0, f"Add failed: {result.output}"

        # Check catalog.json
        catalog_json = initialized_catalog / "catalog.json"
        assert catalog_json.exists()
        catalog_data = json.loads(catalog_json.read_text())

        # Check aggregates
        assert "portolan:total_size_bytes" in catalog_data
        assert "portolan:asset_count" in catalog_data
        assert "portolan:collection_count" in catalog_data
        assert catalog_data["portolan:total_size_bytes"] > 0
        assert catalog_data["portolan:asset_count"] > 0
        assert catalog_data["portolan:collection_count"] == 1

    @pytest.mark.integration
    def test_add_declares_file_extension(
        self, runner: CliRunner, initialized_catalog: Path, sample_parquet_in_catalog: Path
    ) -> None:
        """portolan add should declare file extension when file:size is present."""
        # Act
        result = runner.invoke(
            cli,
            ["add", "--portolan-dir", str(initialized_catalog), str(sample_parquet_in_catalog)],
            catch_exceptions=False,
        )

        assert result.exit_code == 0, f"Add failed: {result.output}"

        # Find collection.json
        collection_jsons = list(initialized_catalog.rglob("collection.json"))
        collection_data = json.loads(collection_jsons[0].read_text())

        # Check file extension is declared
        extensions = collection_data.get("stac_extensions", [])
        file_ext = "https://stac-extensions.github.io/file/v2.1.0/schema.json"
        assert file_ext in extensions, f"File extension not declared. Got: {extensions}"
