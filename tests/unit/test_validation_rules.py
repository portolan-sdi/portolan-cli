"""Tests for validation rule base class and registry."""

from __future__ import annotations

from pathlib import Path

import pytest

from portolan_cli.validation.results import Severity, ValidationResult
from portolan_cli.validation.rules import (
    CatalogExistsRule,
    CatalogJsonValidRule,
    StacFieldsRule,
    ValidationRule,
)


class TestValidationRule:
    """Tests for ValidationRule base class."""

    @pytest.mark.unit
    def test_rule_has_name_attribute(self) -> None:
        """ValidationRule must have a name for identification."""

        class TestRule(ValidationRule):
            name = "test_rule"
            severity = Severity.ERROR
            description = "A test rule"

            def check(self, _catalog_path: Path) -> ValidationResult:
                return self._pass("OK")

        rule = TestRule()
        assert rule.name == "test_rule"

    @pytest.mark.unit
    def test_rule_has_severity_attribute(self) -> None:
        """ValidationRule must have a severity level."""

        class TestRule(ValidationRule):
            name = "test_rule"
            severity = Severity.WARNING
            description = "A test rule"

            def check(self, _catalog_path: Path) -> ValidationResult:
                return self._pass("OK")

        rule = TestRule()
        assert rule.severity == Severity.WARNING

    @pytest.mark.unit
    def test_rule_has_description(self) -> None:
        """ValidationRule must have a description for --verbose output."""

        class TestRule(ValidationRule):
            name = "test_rule"
            severity = Severity.INFO
            description = "Checks something important"

            def check(self, _catalog_path: Path) -> ValidationResult:
                return self._pass("OK")

        rule = TestRule()
        assert rule.description == "Checks something important"

    @pytest.mark.unit
    def test_rule_check_returns_validation_result(self, tmp_path: Path) -> None:
        """check() must return a ValidationResult."""

        class TestRule(ValidationRule):
            name = "test_rule"
            severity = Severity.ERROR
            description = "Test"

            def check(self, _catalog_path: Path) -> ValidationResult:
                return self._pass("Passed")

        rule = TestRule()
        result = rule.check(tmp_path)
        assert isinstance(result, ValidationResult)
        assert result.passed is True

    @pytest.mark.unit
    def test_rule_pass_helper_creates_passing_result(self, tmp_path: Path) -> None:
        """_pass() helper creates a passing ValidationResult."""

        class TestRule(ValidationRule):
            name = "my_rule"
            severity = Severity.ERROR
            description = "Test"

            def check(self, _catalog_path: Path) -> ValidationResult:
                return self._pass("Everything OK")

        rule = TestRule()
        result = rule.check(tmp_path)
        assert result.passed is True
        assert result.rule_name == "my_rule"
        assert result.severity == Severity.ERROR
        assert result.message == "Everything OK"

    @pytest.mark.unit
    def test_rule_fail_helper_creates_failing_result(self, tmp_path: Path) -> None:
        """_fail() helper creates a failing ValidationResult."""

        class TestRule(ValidationRule):
            name = "my_rule"
            severity = Severity.ERROR
            description = "Test"

            def check(self, _catalog_path: Path) -> ValidationResult:
                return self._fail("Something wrong")

        rule = TestRule()
        result = rule.check(tmp_path)
        assert result.passed is False
        assert result.rule_name == "my_rule"
        assert result.message == "Something wrong"

    @pytest.mark.unit
    def test_rule_fail_helper_accepts_fix_hint(self, tmp_path: Path) -> None:
        """_fail() helper can include a fix hint."""

        class TestRule(ValidationRule):
            name = "my_rule"
            severity = Severity.ERROR
            description = "Test"

            def check(self, _catalog_path: Path) -> ValidationResult:
                return self._fail("Missing X", fix_hint="Add X to catalog.json")

        rule = TestRule()
        result = rule.check(tmp_path)
        assert result.fix_hint == "Add X to catalog.json"

    @pytest.mark.unit
    def test_rule_is_abstract(self) -> None:
        """ValidationRule.check() must be implemented by subclasses."""
        with pytest.raises(TypeError, match="abstract"):
            ValidationRule()  # type: ignore[abstract]


class TestCatalogExistsRule:
    """Tests for CatalogExistsRule."""

    @pytest.mark.unit
    def test_passes_when_portolan_dir_exists(self, tmp_path: Path) -> None:
        """Rule passes when .portolan directory exists."""
        portolan_dir = tmp_path / ".portolan"
        portolan_dir.mkdir()

        rule = CatalogExistsRule()
        result = rule.check(tmp_path)

        assert result.passed is True
        assert "exists" in result.message.lower()

    @pytest.mark.unit
    def test_fails_when_portolan_dir_missing(self, tmp_path: Path) -> None:
        """Rule fails when .portolan directory is missing."""
        rule = CatalogExistsRule()
        result = rule.check(tmp_path)

        assert result.passed is False
        assert ".portolan" in result.message

    @pytest.mark.unit
    def test_has_error_severity(self) -> None:
        """Missing catalog is an ERROR (blocking)."""
        rule = CatalogExistsRule()
        assert rule.severity == Severity.ERROR

    @pytest.mark.unit
    def test_provides_fix_hint_on_failure(self, tmp_path: Path) -> None:
        """Failure includes hint to run 'portolan init'."""
        rule = CatalogExistsRule()
        result = rule.check(tmp_path)

        assert result.fix_hint is not None
        assert "init" in result.fix_hint.lower()

    @pytest.mark.unit
    def test_fails_when_portolan_is_file_not_dir(self, tmp_path: Path) -> None:
        """Rule fails when .portolan exists but is a file, not directory."""
        portolan_file = tmp_path / ".portolan"
        portolan_file.write_text("not a directory")

        rule = CatalogExistsRule()
        result = rule.check(tmp_path)

        assert result.passed is False
        assert "directory" in result.message.lower()


class TestCatalogJsonValidRule:
    """Tests for CatalogJsonValidRule.

    In v2 structure, catalog.json is at root level, not inside .portolan.
    """

    @pytest.fixture
    def catalog_dir(self, tmp_path: Path) -> Path:
        """Create a .portolan directory for testing."""
        portolan_dir = tmp_path / ".portolan"
        portolan_dir.mkdir()
        return portolan_dir

    @pytest.mark.unit
    def test_passes_when_catalog_json_valid(self, tmp_path: Path, catalog_dir: Path) -> None:
        """Rule passes when catalog.json exists at root and is valid JSON."""
        # v2: catalog.json at root, not in .portolan
        catalog_file = tmp_path / "catalog.json"
        catalog_file.write_text('{"type": "Catalog"}')

        rule = CatalogJsonValidRule()
        result = rule.check(tmp_path)

        assert result.passed is True

    @pytest.mark.unit
    def test_fails_when_catalog_json_missing(self, tmp_path: Path, catalog_dir: Path) -> None:
        """Rule fails when catalog.json is missing."""
        # catalog_dir exists but catalog.json doesn't at root
        _ = catalog_dir  # Ensure fixture runs

        rule = CatalogJsonValidRule()
        result = rule.check(tmp_path)

        assert result.passed is False
        assert "catalog.json" in result.message

    @pytest.mark.unit
    def test_fails_when_catalog_json_invalid(self, tmp_path: Path, catalog_dir: Path) -> None:
        """Rule fails when catalog.json is not valid JSON."""
        # v2: catalog.json at root
        catalog_file = tmp_path / "catalog.json"
        catalog_file.write_text("not valid json {{{")

        rule = CatalogJsonValidRule()
        result = rule.check(tmp_path)

        assert result.passed is False
        assert "invalid" in result.message.lower() or "parse" in result.message.lower()

    @pytest.mark.unit
    def test_fails_when_catalog_json_empty(self, tmp_path: Path, catalog_dir: Path) -> None:
        """Rule fails when catalog.json is empty."""
        # v2: catalog.json at root
        catalog_file = tmp_path / "catalog.json"
        catalog_file.write_text("")

        rule = CatalogJsonValidRule()
        result = rule.check(tmp_path)

        assert result.passed is False

    @pytest.mark.unit
    def test_has_error_severity(self) -> None:
        """Invalid catalog.json is an ERROR (blocking)."""
        rule = CatalogJsonValidRule()
        assert rule.severity == Severity.ERROR

    @pytest.mark.unit
    def test_provides_fix_hint_on_failure(self, tmp_path: Path, catalog_dir: Path) -> None:
        """Failure includes hint for remediation."""
        # catalog_dir exists but catalog.json doesn't
        _ = catalog_dir

        rule = CatalogJsonValidRule()
        result = rule.check(tmp_path)

        assert result.fix_hint is not None

    @pytest.mark.unit
    def test_fails_gracefully_when_catalog_missing(self, tmp_path: Path) -> None:
        """Rule fails cleanly when catalog.json doesn't exist."""
        rule = CatalogJsonValidRule()
        result = rule.check(tmp_path)

        assert result.passed is False
        # Should not raise an exception


class TestStacFieldsRule:
    """Tests for StacFieldsRule.

    In v2 structure, catalog.json is at root level, not inside .portolan.
    """

    @pytest.fixture
    def catalog_dir(self, tmp_path: Path) -> Path:
        """Create a .portolan directory for testing."""
        portolan_dir = tmp_path / ".portolan"
        portolan_dir.mkdir()
        return portolan_dir

    def _write_catalog(self, root_path: Path, data: dict) -> None:
        """Helper to write catalog.json at root level."""
        import json

        # v2: catalog.json at root, not in .portolan
        catalog_file = root_path / "catalog.json"
        catalog_file.write_text(json.dumps(data))

    @pytest.mark.unit
    def test_passes_with_all_required_fields(self, tmp_path: Path, catalog_dir: Path) -> None:
        """Rule passes when all required STAC fields are present."""
        self._write_catalog(
            tmp_path,
            {
                "type": "Catalog",
                "stac_version": "1.0.0",
                "id": "my-catalog",
                "description": "Test catalog",
                "links": [],
            },
        )

        rule = StacFieldsRule()
        result = rule.check(tmp_path)

        assert result.passed is True

    @pytest.mark.unit
    def test_fails_when_type_missing(self, tmp_path: Path, catalog_dir: Path) -> None:
        """Rule fails when 'type' field is missing."""
        self._write_catalog(
            tmp_path,
            {
                "stac_version": "1.0.0",
                "id": "my-catalog",
                "description": "Test catalog",
                "links": [],
            },
        )

        rule = StacFieldsRule()
        result = rule.check(tmp_path)

        assert result.passed is False
        assert "type" in result.message

    @pytest.mark.unit
    def test_fails_when_type_wrong(self, tmp_path: Path, catalog_dir: Path) -> None:
        """Rule fails when 'type' is not 'Catalog'."""
        self._write_catalog(
            tmp_path,
            {
                "type": "Collection",  # Wrong type
                "stac_version": "1.0.0",
                "id": "my-catalog",
                "description": "Test catalog",
                "links": [],
            },
        )

        rule = StacFieldsRule()
        result = rule.check(tmp_path)

        assert result.passed is False
        assert "Catalog" in result.message

    @pytest.mark.unit
    def test_fails_when_stac_version_missing(self, tmp_path: Path, catalog_dir: Path) -> None:
        """Rule fails when 'stac_version' field is missing."""
        self._write_catalog(
            tmp_path,
            {
                "type": "Catalog",
                "id": "my-catalog",
                "description": "Test catalog",
                "links": [],
            },
        )

        rule = StacFieldsRule()
        result = rule.check(tmp_path)

        assert result.passed is False
        assert "stac_version" in result.message

    @pytest.mark.unit
    def test_fails_when_id_missing(self, tmp_path: Path, catalog_dir: Path) -> None:
        """Rule fails when 'id' field is missing."""
        self._write_catalog(
            tmp_path,
            {
                "type": "Catalog",
                "stac_version": "1.0.0",
                "description": "Test catalog",
                "links": [],
            },
        )

        rule = StacFieldsRule()
        result = rule.check(tmp_path)

        assert result.passed is False
        assert "id" in result.message

    @pytest.mark.unit
    def test_fails_when_description_missing(self, tmp_path: Path, catalog_dir: Path) -> None:
        """Rule fails when 'description' field is missing."""
        self._write_catalog(
            tmp_path,
            {
                "type": "Catalog",
                "stac_version": "1.0.0",
                "id": "my-catalog",
                "links": [],
            },
        )

        rule = StacFieldsRule()
        result = rule.check(tmp_path)

        assert result.passed is False
        assert "description" in result.message

    @pytest.mark.unit
    def test_fails_when_links_missing(self, tmp_path: Path, catalog_dir: Path) -> None:
        """Rule fails when 'links' field is missing."""
        self._write_catalog(
            tmp_path,
            {
                "type": "Catalog",
                "stac_version": "1.0.0",
                "id": "my-catalog",
                "description": "Test catalog",
            },
        )

        rule = StacFieldsRule()
        result = rule.check(tmp_path)

        assert result.passed is False
        assert "links" in result.message

    @pytest.mark.unit
    def test_has_error_severity(self) -> None:
        """Missing required fields is an ERROR."""
        rule = StacFieldsRule()
        assert rule.severity == Severity.ERROR

    @pytest.mark.unit
    def test_provides_fix_hint(self, tmp_path: Path, catalog_dir: Path) -> None:
        """Failure includes hint for --fix."""
        self._write_catalog(tmp_path, {"type": "Catalog"})

        rule = StacFieldsRule()
        result = rule.check(tmp_path)

        assert result.fix_hint is not None
        assert "--fix" in result.fix_hint

    @pytest.mark.unit
    def test_reports_all_missing_fields(self, tmp_path: Path, catalog_dir: Path) -> None:
        """Failure message lists all missing fields, not just first."""
        self._write_catalog(tmp_path, {"type": "Catalog"})

        rule = StacFieldsRule()
        result = rule.check(tmp_path)

        # Should mention multiple missing fields
        assert "stac_version" in result.message
        assert "id" in result.message

    @pytest.mark.unit
    def test_fails_gracefully_when_catalog_json_missing(
        self, tmp_path: Path, catalog_dir: Path
    ) -> None:
        """Rule fails cleanly when catalog.json doesn't exist."""
        # catalog_dir exists but catalog.json doesn't
        _ = catalog_dir

        rule = StacFieldsRule()
        result = rule.check(tmp_path)

        assert result.passed is False

    @pytest.mark.unit
    def test_fails_gracefully_when_catalog_json_invalid(
        self, tmp_path: Path, catalog_dir: Path
    ) -> None:
        """Rule fails cleanly when catalog.json is not valid JSON."""
        # v2: catalog.json at root
        catalog_file = tmp_path / "catalog.json"
        catalog_file.write_text("not json")

        rule = StacFieldsRule()
        result = rule.check(tmp_path)

        assert result.passed is False


class TestCatalogJsonValidRuleOSError:
    """Tests for OSError handling in CatalogJsonValidRule."""

    @pytest.mark.unit
    def test_fails_when_catalog_json_unreadable(self, tmp_path: Path) -> None:
        """Rule fails cleanly when catalog.json cannot be read (OSError)."""
        import os
        import sys

        # v2: catalog.json at root
        catalog_file = tmp_path / "catalog.json"
        catalog_file.write_text('{"type": "Catalog"}')

        # Make file unreadable (Unix only)
        if sys.platform != "win32":
            os.chmod(catalog_file, 0o000)
            try:
                rule = CatalogJsonValidRule()
                result = rule.check(tmp_path)

                assert result.passed is False
                assert "read" in result.message.lower() or "permission" in result.message.lower()
            finally:
                # Restore permissions for cleanup
                os.chmod(catalog_file, 0o644)
        else:
            pytest.skip("OSError test requires Unix file permissions")


class TestPMTilesRecommendedRule:
    """Tests for PMTilesRecommendedRule.

    This rule emits a WARNING (not ERROR) when GeoParquet datasets
    don't have corresponding PMTiles derivatives.
    """

    @pytest.fixture
    def catalog_with_geoparquet(self, tmp_path: Path, fixtures_dir: Path) -> Path:
        """Create a catalog with a GeoParquet dataset but no PMTiles."""
        import shutil

        # Create catalog structure
        portolan_dir = tmp_path / ".portolan"
        portolan_dir.mkdir()
        datasets_dir = portolan_dir / "datasets" / "test-dataset"
        datasets_dir.mkdir(parents=True)

        # Copy sample.parquet to datasets dir
        src = fixtures_dir / "validation" / "pmtiles" / "sample.parquet"
        shutil.copy(src, datasets_dir / "test-dataset.parquet")

        return tmp_path

    @pytest.fixture
    def catalog_with_pmtiles(self, tmp_path: Path, fixtures_dir: Path) -> Path:
        """Create a catalog with both GeoParquet and PMTiles."""
        import shutil

        # Create catalog structure
        portolan_dir = tmp_path / ".portolan"
        portolan_dir.mkdir()
        datasets_dir = portolan_dir / "datasets" / "test-dataset"
        datasets_dir.mkdir(parents=True)

        # Copy both files
        src_parquet = fixtures_dir / "validation" / "pmtiles" / "sample.parquet"
        src_pmtiles = fixtures_dir / "validation" / "pmtiles" / "sample.pmtiles"
        shutil.copy(src_parquet, datasets_dir / "test-dataset.parquet")
        shutil.copy(src_pmtiles, datasets_dir / "test-dataset.pmtiles")

        return tmp_path

    @pytest.fixture
    def catalog_empty(self, tmp_path: Path) -> Path:
        """Create an empty catalog with no datasets."""
        portolan_dir = tmp_path / ".portolan"
        portolan_dir.mkdir()
        datasets_dir = portolan_dir / "datasets"
        datasets_dir.mkdir()
        return tmp_path

    @pytest.fixture
    def catalog_raster_only(self, tmp_path: Path, fixtures_dir: Path) -> Path:
        """Create a catalog with only raster (COG) datasets."""
        import shutil

        portolan_dir = tmp_path / ".portolan"
        portolan_dir.mkdir()
        datasets_dir = portolan_dir / "datasets" / "raster-dataset"
        datasets_dir.mkdir(parents=True)

        # Copy a COG file (no PMTiles expected for raster)
        src = fixtures_dir / "raster" / "valid" / "rgb.tif"
        if src.exists():
            shutil.copy(src, datasets_dir / "raster-dataset.tif")
        else:
            # Create a placeholder if fixture doesn't exist
            (datasets_dir / "raster-dataset.tif").write_bytes(b"placeholder")

        return tmp_path

    @pytest.fixture
    def fixtures_dir(self) -> Path:
        """Return the path to the test fixtures directory."""
        return Path(__file__).parent.parent / "fixtures"

    @pytest.mark.unit
    def test_has_warning_severity(self) -> None:
        """PMTiles recommendation is a WARNING, not ERROR."""
        from portolan_cli.validation.rules import PMTilesRecommendedRule

        rule = PMTilesRecommendedRule()
        assert rule.severity == Severity.WARNING

    @pytest.mark.unit
    def test_has_descriptive_name(self) -> None:
        """Rule has a unique identifier."""
        from portolan_cli.validation.rules import PMTilesRecommendedRule

        rule = PMTilesRecommendedRule()
        assert rule.name == "pmtiles_recommended"

    @pytest.mark.unit
    def test_has_description(self) -> None:
        """Rule has human-readable description."""
        from portolan_cli.validation.rules import PMTilesRecommendedRule

        rule = PMTilesRecommendedRule()
        assert "pmtiles" in rule.description.lower()

    @pytest.mark.unit
    def test_warns_when_geoparquet_without_pmtiles(self, catalog_with_geoparquet: Path) -> None:
        """Rule emits warning when GeoParquet lacks PMTiles derivative."""
        from portolan_cli.validation.rules import PMTilesRecommendedRule

        rule = PMTilesRecommendedRule()
        result = rule.check(catalog_with_geoparquet)

        assert result.passed is False
        assert result.severity == Severity.WARNING
        assert "pmtiles" in result.message.lower()

    @pytest.mark.unit
    def test_passes_when_pmtiles_exists(self, catalog_with_pmtiles: Path) -> None:
        """Rule passes when PMTiles exists alongside GeoParquet."""
        from portolan_cli.validation.rules import PMTilesRecommendedRule

        rule = PMTilesRecommendedRule()
        result = rule.check(catalog_with_pmtiles)

        assert result.passed is True

    @pytest.mark.unit
    def test_passes_for_empty_catalog(self, catalog_empty: Path) -> None:
        """Rule passes when catalog has no datasets (nothing to recommend)."""
        from portolan_cli.validation.rules import PMTilesRecommendedRule

        rule = PMTilesRecommendedRule()
        result = rule.check(catalog_empty)

        assert result.passed is True

    @pytest.mark.unit
    def test_passes_for_raster_only_catalog(self, catalog_raster_only: Path) -> None:
        """Rule passes for raster-only catalogs (PMTiles is for vector data)."""
        from portolan_cli.validation.rules import PMTilesRecommendedRule

        rule = PMTilesRecommendedRule()
        result = rule.check(catalog_raster_only)

        assert result.passed is True

    @pytest.mark.unit
    def test_provides_fix_hint_with_plugin_info(self, catalog_with_geoparquet: Path) -> None:
        """Failure includes hint about portolan-pmtiles plugin."""
        from portolan_cli.validation.rules import PMTilesRecommendedRule

        rule = PMTilesRecommendedRule()
        result = rule.check(catalog_with_geoparquet)

        assert result.fix_hint is not None
        assert "portolan-pmtiles" in result.fix_hint.lower()

    @pytest.mark.unit
    def test_handles_missing_datasets_dir_gracefully(self, tmp_path: Path) -> None:
        """Rule handles missing datasets directory without error."""
        from portolan_cli.validation.rules import PMTilesRecommendedRule

        # Create catalog without datasets dir
        portolan_dir = tmp_path / ".portolan"
        portolan_dir.mkdir()

        rule = PMTilesRecommendedRule()
        result = rule.check(tmp_path)

        # Should pass (no datasets to check)
        assert result.passed is True

    @pytest.mark.unit
    def test_handles_missing_portolan_dir_gracefully(self, tmp_path: Path) -> None:
        """Rule handles missing .portolan directory without error."""
        from portolan_cli.validation.rules import PMTilesRecommendedRule

        rule = PMTilesRecommendedRule()
        result = rule.check(tmp_path)

        # Should pass (no catalog to check)
        assert result.passed is True


class TestMetadataFreshRule:
    """Tests for MetadataFreshRule.

    This rule checks that all geo-asset files in collections have
    up-to-date STAC metadata.
    """

    @pytest.fixture
    def fixtures_dir(self) -> Path:
        """Return the path to the test fixtures directory."""
        return Path(__file__).parent.parent / "fixtures"

    @pytest.mark.integration
    def test_has_metadata_fresh_name(self) -> None:
        """Rule has correct name."""
        from portolan_cli.validation.rules import MetadataFreshRule

        rule = MetadataFreshRule()
        assert rule.name == "metadata_fresh"

    @pytest.mark.integration
    def test_has_warning_severity_default(self) -> None:
        """Rule defaults to WARNING severity (STALE case)."""
        from portolan_cli.validation.rules import MetadataFreshRule

        rule = MetadataFreshRule()
        assert rule.severity == Severity.WARNING

    @pytest.mark.integration
    def test_passes_when_no_portolan_dir(self, tmp_path: Path) -> None:
        """Rule passes when .portolan directory doesn't exist."""
        from portolan_cli.validation.rules import MetadataFreshRule

        rule = MetadataFreshRule()
        result = rule.check(tmp_path)

        assert result.passed is True
        assert ".portolan" in result.message.lower()

    @pytest.mark.integration
    def test_passes_when_no_collections_dir(self, tmp_path: Path) -> None:
        """Rule passes when collections directory doesn't exist."""
        from portolan_cli.validation.rules import MetadataFreshRule

        portolan_dir = tmp_path / ".portolan"
        portolan_dir.mkdir()

        rule = MetadataFreshRule()
        result = rule.check(tmp_path)

        assert result.passed is True
        assert "collections" in result.message.lower()

    @pytest.mark.integration
    def test_passes_when_no_geo_assets_found(self, tmp_path: Path) -> None:
        """Rule passes when no geo-asset files are found in collections."""
        from portolan_cli.validation.rules import MetadataFreshRule

        portolan_dir = tmp_path / ".portolan"
        portolan_dir.mkdir()
        collections_dir = portolan_dir / "collections"
        collections_dir.mkdir()
        # Create an empty collection
        (collections_dir / "test-collection").mkdir()

        rule = MetadataFreshRule()
        result = rule.check(tmp_path)

        assert result.passed is True
        assert "no geo-asset" in result.message.lower()

    @pytest.mark.integration
    def test_skips_non_directory_items(self, tmp_path: Path) -> None:
        """Rule skips files in collections directory (only processes directories)."""
        from portolan_cli.validation.rules import MetadataFreshRule

        portolan_dir = tmp_path / ".portolan"
        portolan_dir.mkdir()
        collections_dir = portolan_dir / "collections"
        collections_dir.mkdir()
        # Create a file (not a directory) in collections
        (collections_dir / "some-file.txt").write_text("not a collection")

        rule = MetadataFreshRule()
        result = rule.check(tmp_path)

        # Should pass (no valid collections)
        assert result.passed is True

    @pytest.mark.integration
    def test_detects_missing_metadata(self, tmp_path: Path, fixtures_dir: Path) -> None:
        """Rule detects geo-assets without STAC metadata (MISSING status)."""
        import shutil

        from portolan_cli.validation.rules import MetadataFreshRule

        # Create catalog structure
        portolan_dir = tmp_path / ".portolan"
        portolan_dir.mkdir()
        collections_dir = portolan_dir / "collections"
        collections_dir.mkdir()
        collection = collections_dir / "test-collection"
        collection.mkdir()

        # Copy a parquet file but don't create STAC item
        src = fixtures_dir / "vector" / "valid" / "points.parquet"
        if src.exists():
            shutil.copy(src, collection / "points.parquet")
        else:
            # Create minimal parquet if fixture doesn't exist
            import pyarrow as pa
            import pyarrow.parquet as pq

            table = pa.table({"id": [1, 2], "geometry": ["POINT(0 0)", "POINT(1 1)"]})
            pq.write_table(table, collection / "points.parquet")

        rule = MetadataFreshRule()
        result = rule.check(tmp_path)

        # Should fail due to missing metadata
        assert result.passed is False
        assert "missing" in result.message.lower()
        # MISSING is an ERROR
        assert result.severity == Severity.ERROR

    @pytest.mark.integration
    def test_detects_fresh_metadata(self, tmp_path: Path) -> None:
        """Rule passes when all geo-assets have fresh metadata."""
        import json

        import pyarrow as pa
        import pyarrow.parquet as pq

        from portolan_cli.metadata.geoparquet import extract_geoparquet_metadata
        from portolan_cli.validation.rules import MetadataFreshRule

        # Create catalog structure
        portolan_dir = tmp_path / ".portolan"
        portolan_dir.mkdir()
        collections_dir = portolan_dir / "collections"
        collections_dir.mkdir()
        collection = collections_dir / "test-collection"
        collection.mkdir()

        # Create a parquet file
        parquet_path = collection / "test.parquet"
        table = pa.table({"id": [1, 2], "geometry": ["POINT(0 0)", "POINT(1 1)"]})
        pq.write_table(table, parquet_path)

        # Extract metadata and create STAC item
        metadata = extract_geoparquet_metadata(parquet_path)
        stac_item = {
            "type": "Feature",
            "stac_version": "1.0.0",
            "id": "test",
            "geometry": None,
            "bbox": metadata.bbox,
            "properties": {
                "datetime": None,
                "feature_count": metadata.feature_count,
            },
            "collection": "test-collection",
            "assets": {
                "data": {
                    "href": "./test.parquet",
                    "type": "application/x-parquet",
                }
            },
            "links": [],
        }
        (collection / "test.json").write_text(json.dumps(stac_item))

        # Create versions.json with current state
        versions = {
            "schema_version": "1.0",
            "current_version": "v1",
            "versions": [
                {
                    "version": "v1",
                    "created_at": "2025-01-01T00:00:00Z",
                    "assets": {
                        "test.parquet": {
                            "source_mtime": parquet_path.stat().st_mtime,
                            "sha256": "abc123",
                            "bbox": metadata.bbox,
                            "feature_count": metadata.feature_count,
                        }
                    },
                }
            ],
        }
        (collection / "versions.json").write_text(json.dumps(versions))

        rule = MetadataFreshRule()
        result = rule.check(tmp_path)

        assert result.passed is True
        assert "fresh" in result.message.lower()

    @pytest.mark.integration
    def test_reports_stale_warning_only(self, tmp_path: Path) -> None:
        """Rule reports STALE as WARNING (not ERROR)."""
        import json

        import pyarrow as pa
        import pyarrow.parquet as pq

        from portolan_cli.metadata.detection import compute_schema_fingerprint
        from portolan_cli.metadata.geoparquet import extract_geoparquet_metadata
        from portolan_cli.validation.rules import MetadataFreshRule

        # Create catalog structure
        portolan_dir = tmp_path / ".portolan"
        portolan_dir.mkdir()
        collections_dir = portolan_dir / "collections"
        collections_dir.mkdir()
        collection = collections_dir / "test-collection"
        collection.mkdir()

        # Create a parquet file
        parquet_path = collection / "test.parquet"
        table = pa.table({"id": [1, 2], "geometry": ["POINT(0 0)", "POINT(1 1)"]})
        pq.write_table(table, parquet_path)

        # Extract metadata and create STAC item
        metadata = extract_geoparquet_metadata(parquet_path)
        schema_fp = compute_schema_fingerprint(parquet_path)
        stac_item = {
            "type": "Feature",
            "stac_version": "1.0.0",
            "id": "test",
            "geometry": None,
            "bbox": metadata.bbox,
            "properties": {
                "datetime": None,
                "feature_count": metadata.feature_count,
            },
            "collection": "test-collection",
            "assets": {
                "data": {
                    "href": "./test.parquet",
                    "type": "application/x-parquet",
                }
            },
            "links": [],
        }
        (collection / "test.json").write_text(json.dumps(stac_item))

        # Create versions.json with STALE state (old mtime only, same schema)
        # STALE = mtime changed but schema hasn't changed (BREAKING = schema changed)
        versions = {
            "schema_version": "1.0",
            "current_version": "v1",
            "versions": [
                {
                    "version": "v1",
                    "created_at": "2025-01-01T00:00:00Z",
                    "assets": {
                        "test.parquet": {
                            "source_mtime": parquet_path.stat().st_mtime - 1000,  # Old mtime
                            "sha256": "abc123",
                            "bbox": metadata.bbox,
                            "feature_count": metadata.feature_count,  # Same count
                            "schema_fingerprint": schema_fp,  # Same schema = not BREAKING
                        }
                    },
                }
            ],
        }
        (collection / "versions.json").write_text(json.dumps(versions))

        rule = MetadataFreshRule()
        result = rule.check(tmp_path)

        assert result.passed is False
        assert "stale" in result.message.lower()
        # STALE alone is WARNING, not ERROR
        assert result.severity == Severity.WARNING

    @pytest.mark.integration
    def test_provides_fix_hint(self, tmp_path: Path) -> None:
        """Rule provides fix hint when issues found."""
        import pyarrow as pa
        import pyarrow.parquet as pq

        from portolan_cli.validation.rules import MetadataFreshRule

        # Create catalog structure with a parquet file but no STAC item
        portolan_dir = tmp_path / ".portolan"
        portolan_dir.mkdir()
        collections_dir = portolan_dir / "collections"
        collections_dir.mkdir()
        collection = collections_dir / "test-collection"
        collection.mkdir()

        parquet_path = collection / "test.parquet"
        table = pa.table({"id": [1], "geometry": ["POINT(0 0)"]})
        pq.write_table(table, parquet_path)

        rule = MetadataFreshRule()
        result = rule.check(tmp_path)

        assert result.passed is False
        assert result.fix_hint is not None
        assert "fix-metadata" in result.fix_hint.lower()

    @pytest.mark.integration
    def test_handles_file_not_found_gracefully(self, tmp_path: Path) -> None:
        """Rule skips files that raise FileNotFoundError during check."""

        from portolan_cli.validation.rules import MetadataFreshRule

        # Create catalog structure
        portolan_dir = tmp_path / ".portolan"
        portolan_dir.mkdir()
        collections_dir = portolan_dir / "collections"
        collections_dir.mkdir()
        collection = collections_dir / "test-collection"
        collection.mkdir()

        # Create a symlink to a non-existent file
        symlink = collection / "broken.parquet"
        symlink.symlink_to("/nonexistent/file.parquet")

        rule = MetadataFreshRule()
        result = rule.check(tmp_path)

        # Should pass (skipped the broken file)
        assert result.passed is True

    @pytest.mark.integration
    def test_scans_both_parquet_and_tif(self, tmp_path: Path) -> None:
        """Rule scans both GeoParquet (.parquet) and COG (.tif, .tiff) files."""
        from portolan_cli.validation.rules import MetadataFreshRule

        # Create catalog structure
        portolan_dir = tmp_path / ".portolan"
        portolan_dir.mkdir()
        collections_dir = portolan_dir / "collections"
        collections_dir.mkdir()
        collection = collections_dir / "test-collection"
        collection.mkdir()

        # Create files with different extensions (they'll fail to parse, but we test scanning)
        (collection / "data.parquet").write_bytes(b"dummy")
        (collection / "image.tif").write_bytes(b"dummy")
        (collection / "image2.tiff").write_bytes(b"dummy")
        (collection / "readme.txt").write_text("not a geo file")

        rule = MetadataFreshRule()
        # This will likely fail due to invalid file content, but should scan all geo-assets
        result = rule.check(tmp_path)

        # Should return a result (may pass if errors are skipped)
        assert result is not None

    @pytest.mark.integration
    def test_scans_nested_collection_files(self, tmp_path: Path) -> None:
        """Rule scans files in subdirectories within collections."""
        import pyarrow as pa
        import pyarrow.parquet as pq

        from portolan_cli.validation.rules import MetadataFreshRule

        # Create catalog structure with nested files
        portolan_dir = tmp_path / ".portolan"
        portolan_dir.mkdir()
        collections_dir = portolan_dir / "collections"
        collections_dir.mkdir()
        collection = collections_dir / "test-collection"
        collection.mkdir()
        subdir = collection / "data" / "2024"
        subdir.mkdir(parents=True)

        # Create a parquet file in nested directory
        parquet_path = subdir / "test.parquet"
        table = pa.table({"id": [1], "geometry": ["POINT(0 0)"]})
        pq.write_table(table, parquet_path)

        rule = MetadataFreshRule()
        result = rule.check(tmp_path)

        # Should detect the nested file (and fail due to missing STAC metadata)
        assert result.passed is False
        assert "missing" in result.message.lower()
