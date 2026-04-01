"""Integration tests for metadata.yaml defaults in the add workflow.

These tests verify that defaults from metadata.yaml are applied during
`portolan add` when source files lack certain metadata.
"""

import json
from pathlib import Path

import pytest
import yaml
from click.testing import CliRunner

from portolan_cli.cli import cli


@pytest.fixture
def catalog_with_defaults(tmp_path: Path) -> Path:
    """Create a catalog with metadata.yaml defaults configured."""
    catalog_root = tmp_path / "catalog"
    catalog_root.mkdir()

    # Initialize catalog structure
    (catalog_root / "catalog.json").write_text(
        json.dumps(
            {
                "type": "Catalog",
                "stac_version": "1.1.0",
                "id": "test-catalog",
                "description": "Test catalog",
                "links": [],
            }
        )
    )

    # Create .portolan directory with config
    portolan_dir = catalog_root / ".portolan"
    portolan_dir.mkdir()

    (portolan_dir / "config.yaml").write_text(
        yaml.dump({"version": 1, "statistics": {"enabled": False}})
    )

    # Create metadata.yaml with defaults
    (portolan_dir / "metadata.yaml").write_text(
        yaml.dump(
            {
                "contact": {"name": "Test", "email": "test@example.com"},
                "license": "CC-BY-4.0",
                "defaults": {
                    "temporal": {"year": 2025},
                    "raster": {"nodata": 0},
                },
            }
        )
    )

    # Create collection directory
    collection_dir = catalog_root / "test-collection"
    collection_dir.mkdir()

    return catalog_root


@pytest.fixture
def simple_cog(tmp_path: Path) -> Path:
    """Create a simple COG without nodata set."""
    pytest.importorskip("rasterio")
    import numpy as np
    import rasterio
    from rasterio.transform import from_bounds

    cog_path = tmp_path / "test.tif"

    # Create a 10x10 RGB image
    data = np.random.randint(0, 255, (3, 10, 10), dtype=np.uint8)
    transform = from_bounds(-75.2, 39.9, -75.1, 40.0, 10, 10)

    profile = {
        "driver": "GTiff",
        "dtype": "uint8",
        "width": 10,
        "height": 10,
        "count": 3,
        "crs": "EPSG:4326",
        "transform": transform,
        "nodata": None,  # Explicitly no nodata
    }

    with rasterio.open(cog_path, "w", **profile) as dst:
        dst.write(data)

    return cog_path


@pytest.mark.integration
class TestAddWithTemporalDefaults:
    """Test that temporal defaults from metadata.yaml are applied."""

    def test_temporal_default_applied_when_no_datetime_flag(
        self,
        catalog_with_defaults: Path,
        simple_cog: Path,
    ) -> None:
        """Item gets temporal default when --datetime not provided."""
        # Copy COG into collection
        collection_dir = catalog_with_defaults / "test-collection"
        item_dir = collection_dir / "test-item"
        item_dir.mkdir()

        import shutil

        dest = item_dir / "test.tif"
        shutil.copy(simple_cog, dest)

        # Run add without --datetime
        runner = CliRunner()
        result = runner.invoke(
            cli,
            [
                "add",
                "--portolan-dir",
                str(catalog_with_defaults),
                str(dest),
            ],
            catch_exceptions=False,
        )

        assert result.exit_code == 0, result.output

        # Check item.json has datetime from defaults
        item_json = item_dir / "test-item.json"
        assert item_json.exists(), f"Expected {item_json}"

        with open(item_json) as f:
            item = json.load(f)

        # Should have datetime from year: 2025 default
        assert "datetime" in item["properties"]
        assert item["properties"]["datetime"] is not None
        assert "2025" in item["properties"]["datetime"]

    def test_datetime_flag_overrides_default(
        self,
        catalog_with_defaults: Path,
        simple_cog: Path,
    ) -> None:
        """--datetime flag overrides metadata.yaml defaults."""
        collection_dir = catalog_with_defaults / "test-collection"
        item_dir = collection_dir / "test-item2"
        item_dir.mkdir()

        import shutil

        dest = item_dir / "test.tif"
        shutil.copy(simple_cog, dest)

        runner = CliRunner()
        result = runner.invoke(
            cli,
            [
                "add",
                "--portolan-dir",
                str(catalog_with_defaults),
                str(dest),
                "--datetime",
                "2024-06-15",
            ],
            catch_exceptions=False,
        )

        assert result.exit_code == 0, result.output

        item_json = item_dir / "test-item2.json"
        with open(item_json) as f:
            item = json.load(f)

        # Should have explicitly provided datetime, not default
        assert "2024-06-15" in item["properties"]["datetime"]


@pytest.mark.integration
class TestAddWithRasterNodataDefaults:
    """Test that raster nodata defaults from metadata.yaml are applied."""

    def test_nodata_default_applied_to_bands(
        self,
        catalog_with_defaults: Path,
        simple_cog: Path,
    ) -> None:
        """Item bands get nodata default when source has none."""
        collection_dir = catalog_with_defaults / "test-collection"
        item_dir = collection_dir / "test-nodata"
        item_dir.mkdir()

        import shutil

        dest = item_dir / "test.tif"
        shutil.copy(simple_cog, dest)

        runner = CliRunner()
        result = runner.invoke(
            cli,
            [
                "add",
                "--portolan-dir",
                str(catalog_with_defaults),
                str(dest),
            ],
            catch_exceptions=False,
        )

        assert result.exit_code == 0, result.output

        item_json = item_dir / "test-nodata.json"
        with open(item_json) as f:
            item = json.load(f)

        # Check bands have nodata from defaults
        bands = item["properties"].get("bands", [])
        assert len(bands) == 3, f"Expected 3 bands, got {len(bands)}"

        for i, band in enumerate(bands):
            assert "nodata" in band, f"Band {i} missing nodata"
            assert band["nodata"] == 0.0, f"Band {i} nodata should be 0.0"


@pytest.fixture
def cog_with_nodata(tmp_path: Path) -> Path:
    """Create a COG WITH nodata already set."""
    pytest.importorskip("rasterio")
    import numpy as np
    import rasterio
    from rasterio.transform import from_bounds

    cog_path = tmp_path / "test_with_nodata.tif"

    data = np.random.randint(0, 255, (3, 10, 10), dtype=np.uint8)
    transform = from_bounds(-75.2, 39.9, -75.1, 40.0, 10, 10)

    profile = {
        "driver": "GTiff",
        "dtype": "uint8",
        "width": 10,
        "height": 10,
        "count": 3,
        "crs": "EPSG:4326",
        "transform": transform,
        "nodata": 255,  # Source file HAS nodata
    }

    with rasterio.open(cog_path, "w", **profile) as dst:
        dst.write(data)

    return cog_path


@pytest.mark.integration
class TestSourceNodataPreserved:
    """Test that source file nodata is preserved, not overridden by defaults."""

    def test_source_nodata_not_overridden_by_default(
        self,
        catalog_with_defaults: Path,
        cog_with_nodata: Path,
    ) -> None:
        """Source file nodata (255) should be preserved, not replaced by default (0)."""
        collection_dir = catalog_with_defaults / "test-collection"
        item_dir = collection_dir / "test-preserve-nodata"
        item_dir.mkdir()

        import shutil

        dest = item_dir / "test.tif"
        shutil.copy(cog_with_nodata, dest)

        runner = CliRunner()
        result = runner.invoke(
            cli,
            [
                "add",
                "--portolan-dir",
                str(catalog_with_defaults),
                str(dest),
            ],
            catch_exceptions=False,
        )

        assert result.exit_code == 0, result.output

        item_json = item_dir / "test-preserve-nodata.json"
        with open(item_json) as f:
            item = json.load(f)

        bands = item["properties"].get("bands", [])
        assert len(bands) == 3

        # Source nodata (255) should be preserved, NOT replaced by default (0)
        for i, band in enumerate(bands):
            assert "nodata" in band, f"Band {i} missing nodata"
            assert band["nodata"] == 255.0, (
                f"Band {i} should have source nodata 255.0, got {band['nodata']}"
            )


@pytest.mark.integration
class TestInvalidDefaultsFailFast:
    """Test that invalid metadata.yaml defaults cause add to fail gracefully."""

    def test_invalid_temporal_year_type_fails(
        self,
        tmp_path: Path,
        simple_cog: Path,
    ) -> None:
        """Add fails gracefully when temporal.year is wrong type."""
        catalog_root = tmp_path / "catalog"
        catalog_root.mkdir()

        (catalog_root / "catalog.json").write_text(
            json.dumps(
                {
                    "type": "Catalog",
                    "stac_version": "1.1.0",
                    "id": "test-catalog",
                    "description": "Test",
                    "links": [],
                }
            )
        )

        portolan_dir = catalog_root / ".portolan"
        portolan_dir.mkdir()
        (portolan_dir / "config.yaml").write_text(yaml.dump({"version": 1}))

        # Invalid: year is string instead of int
        (portolan_dir / "metadata.yaml").write_text(
            yaml.dump(
                {
                    "contact": {"name": "Test", "email": "test@example.com"},
                    "license": "CC-BY-4.0",
                    "defaults": {"temporal": {"year": "2025"}},  # String!
                }
            )
        )

        collection_dir = catalog_root / "test-collection"
        collection_dir.mkdir()
        item_dir = collection_dir / "test-item"
        item_dir.mkdir()

        import shutil

        dest = item_dir / "test.tif"
        shutil.copy(simple_cog, dest)

        runner = CliRunner()
        result = runner.invoke(
            cli,
            ["add", "--portolan-dir", str(catalog_root), str(dest)],
        )

        # Should fail with validation error
        assert result.exit_code != 0
        assert "year" in result.output.lower() or "integer" in result.output.lower()

    def test_per_band_nodata_mismatch_fails(
        self,
        tmp_path: Path,
        simple_cog: Path,  # 3 bands
    ) -> None:
        """Add fails when per-band nodata list doesn't match band count."""
        catalog_root = tmp_path / "catalog"
        catalog_root.mkdir()

        (catalog_root / "catalog.json").write_text(
            json.dumps(
                {
                    "type": "Catalog",
                    "stac_version": "1.1.0",
                    "id": "test-catalog",
                    "description": "Test",
                    "links": [],
                }
            )
        )

        portolan_dir = catalog_root / ".portolan"
        portolan_dir.mkdir()
        (portolan_dir / "config.yaml").write_text(yaml.dump({"version": 1}))

        # Invalid: 4 nodata values for a 3-band image
        (portolan_dir / "metadata.yaml").write_text(
            yaml.dump(
                {
                    "contact": {"name": "Test", "email": "test@example.com"},
                    "license": "CC-BY-4.0",
                    "defaults": {"raster": {"nodata": [0, 0, 255, 127]}},  # 4 values!
                }
            )
        )

        collection_dir = catalog_root / "test-collection"
        collection_dir.mkdir()
        item_dir = collection_dir / "test-item"
        item_dir.mkdir()

        import shutil

        dest = item_dir / "test.tif"
        shutil.copy(simple_cog, dest)

        runner = CliRunner()
        result = runner.invoke(
            cli,
            ["add", "--portolan-dir", str(catalog_root), str(dest)],
        )

        # Should fail with mismatch error
        assert result.exit_code != 0
        assert "4 values" in result.output or "3 bands" in result.output


@pytest.mark.integration
class TestHierarchyOverride:
    """Test that collection-level defaults override catalog-level defaults."""

    def test_collection_defaults_override_catalog_defaults(
        self,
        tmp_path: Path,
        simple_cog: Path,
    ) -> None:
        """Collection-level metadata.yaml defaults override catalog-level."""
        catalog_root = tmp_path / "catalog"
        catalog_root.mkdir()

        (catalog_root / "catalog.json").write_text(
            json.dumps(
                {
                    "type": "Catalog",
                    "stac_version": "1.1.0",
                    "id": "test-catalog",
                    "description": "Test",
                    "links": [],
                }
            )
        )

        # Catalog-level defaults: year=2024, nodata=255
        catalog_portolan = catalog_root / ".portolan"
        catalog_portolan.mkdir()
        (catalog_portolan / "config.yaml").write_text(yaml.dump({"version": 1}))
        (catalog_portolan / "metadata.yaml").write_text(
            yaml.dump(
                {
                    "contact": {"name": "Test", "email": "test@example.com"},
                    "license": "CC-BY-4.0",
                    "defaults": {
                        "temporal": {"year": 2024},
                        "raster": {"nodata": 255},
                    },
                }
            )
        )

        # Collection-level: overrides year to 2025, inherits nodata=255
        collection_dir = catalog_root / "test-collection"
        collection_dir.mkdir()
        collection_portolan = collection_dir / ".portolan"
        collection_portolan.mkdir()
        (collection_portolan / "metadata.yaml").write_text(
            yaml.dump(
                {
                    "defaults": {"temporal": {"year": 2025}},  # Override
                }
            )
        )

        item_dir = collection_dir / "test-item"
        item_dir.mkdir()

        import shutil

        dest = item_dir / "test.tif"
        shutil.copy(simple_cog, dest)

        runner = CliRunner()
        result = runner.invoke(
            cli,
            ["add", "--portolan-dir", str(catalog_root), str(dest)],
            catch_exceptions=False,
        )

        assert result.exit_code == 0, result.output

        item_json = item_dir / "test-item.json"
        with open(item_json) as f:
            item = json.load(f)

        # Should use collection year (2025), not catalog year (2024)
        assert "2025" in item["properties"]["datetime"]

        # Should use catalog nodata (255) since collection didn't override it
        bands = item["properties"].get("bands", [])
        for band in bands:
            assert band.get("nodata") == 255.0
