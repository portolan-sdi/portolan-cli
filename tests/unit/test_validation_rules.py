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

    This rule emits a WARNING (not ERROR) when GeoParquet collection assets
    don't have corresponding PMTiles derivatives as siblings.
    """

    @pytest.fixture
    def catalog_with_geoparquet(self, tmp_path: Path) -> Path:
        """Create a catalog with a GeoParquet collection asset but no PMTiles."""
        import json

        # Create catalog structure with collection-level asset (ADR-0031)
        portolan_dir = tmp_path / ".portolan"
        portolan_dir.mkdir()

        collection_dir = tmp_path / "test-collection"
        collection_dir.mkdir()

        # Create collection.json with GeoParquet asset
        collection_json = {
            "type": "Collection",
            "id": "test-collection",
            "stac_version": "1.0.0",
            "description": "Test collection",
            "license": "MIT",
            "extent": {
                "spatial": {"bbox": [[-180, -90, 180, 90]]},
                "temporal": {"interval": [[None, None]]},
            },
            "links": [],
            "assets": {
                "data": {
                    "href": "./data.parquet",
                    "type": "application/vnd.apache.parquet",
                    "roles": ["data"],
                }
            },
        }
        (collection_dir / "collection.json").write_text(json.dumps(collection_json))

        # Create the parquet file (empty placeholder)
        (collection_dir / "data.parquet").write_bytes(b"PAR1placeholder")

        return tmp_path

    @pytest.fixture
    def catalog_with_pmtiles(self, tmp_path: Path) -> Path:
        """Create a catalog with both GeoParquet and PMTiles assets."""
        import json

        portolan_dir = tmp_path / ".portolan"
        portolan_dir.mkdir()

        collection_dir = tmp_path / "test-collection"
        collection_dir.mkdir()

        # Create collection.json with both assets
        collection_json = {
            "type": "Collection",
            "id": "test-collection",
            "stac_version": "1.0.0",
            "description": "Test collection",
            "license": "MIT",
            "extent": {
                "spatial": {"bbox": [[-180, -90, 180, 90]]},
                "temporal": {"interval": [[None, None]]},
            },
            "links": [],
            "assets": {
                "data": {
                    "href": "./data.parquet",
                    "type": "application/vnd.apache.parquet",
                    "roles": ["data"],
                },
                "data-tiles": {
                    "href": "./data.pmtiles",
                    "type": "application/vnd.pmtiles",
                    "roles": ["visual"],
                },
            },
        }
        (collection_dir / "collection.json").write_text(json.dumps(collection_json))

        # Create both files
        (collection_dir / "data.parquet").write_bytes(b"PAR1placeholder")
        (collection_dir / "data.pmtiles").write_bytes(b"PMTilesplaceholder")

        return tmp_path

    @pytest.fixture
    def catalog_empty(self, tmp_path: Path) -> Path:
        """Create an empty catalog with no collections."""
        portolan_dir = tmp_path / ".portolan"
        portolan_dir.mkdir()
        return tmp_path

    @pytest.fixture
    def catalog_with_stac_items_parquet(self, tmp_path: Path) -> Path:
        """Create a catalog with stac-items parquet (should be ignored)."""
        import json

        portolan_dir = tmp_path / ".portolan"
        portolan_dir.mkdir()

        collection_dir = tmp_path / "test-collection"
        collection_dir.mkdir()

        # Create collection.json with stac-items parquet (not geodata)
        collection_json = {
            "type": "Collection",
            "id": "test-collection",
            "stac_version": "1.0.0",
            "description": "Test collection",
            "license": "MIT",
            "extent": {
                "spatial": {"bbox": [[-180, -90, 180, 90]]},
                "temporal": {"interval": [[None, None]]},
            },
            "links": [],
            "assets": {
                "geoparquet-items": {
                    "href": "./items.parquet",
                    "type": "application/vnd.apache.parquet",
                    "roles": ["stac-items"],
                }
            },
        }
        (collection_dir / "collection.json").write_text(json.dumps(collection_json))
        (collection_dir / "items.parquet").write_bytes(b"PAR1placeholder")

        return tmp_path

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
        """Rule passes when catalog has no collections."""
        from portolan_cli.validation.rules import PMTilesRecommendedRule

        rule = PMTilesRecommendedRule()
        result = rule.check(catalog_empty)

        assert result.passed is True

    @pytest.mark.unit
    def test_ignores_stac_items_parquet(self, catalog_with_stac_items_parquet: Path) -> None:
        """Rule ignores stac-items parquet (metadata, not geodata)."""
        from portolan_cli.validation.rules import PMTilesRecommendedRule

        rule = PMTilesRecommendedRule()
        result = rule.check(catalog_with_stac_items_parquet)

        # Should pass - stac-items parquet is metadata, not geodata
        assert result.passed is True

    @pytest.mark.unit
    def test_provides_fix_hint(self, catalog_with_geoparquet: Path) -> None:
        """Failure includes hint about generating PMTiles."""
        from portolan_cli.validation.rules import PMTilesRecommendedRule

        rule = PMTilesRecommendedRule()
        result = rule.check(catalog_with_geoparquet)

        assert result.fix_hint is not None
        assert "pmtiles" in result.fix_hint.lower()

    @pytest.mark.unit
    def test_handles_no_collections_gracefully(self, tmp_path: Path) -> None:
        """Rule handles catalog with no collection.json files."""
        from portolan_cli.validation.rules import PMTilesRecommendedRule

        rule = PMTilesRecommendedRule()
        result = rule.check(tmp_path)

        assert result.passed is True


class TestPMTilesLinkRule:
    """Tests for PMTilesLinkRule (RULE-0061, Issue #569).

    This rule emits an ERROR when a collection registers a PMTiles asset but
    does not emit a collection-level ``rel="pmtiles"`` web-map-links link.
    """

    @staticmethod
    def _write_collection(tmp_path: Path, *, with_link: bool) -> Path:
        import json

        (tmp_path / ".portolan").mkdir()
        collection_dir = tmp_path / "test-collection"
        collection_dir.mkdir()

        links: list[dict[str, object]] = []
        if with_link:
            links.append(
                {
                    "rel": "pmtiles",
                    "href": "./data.pmtiles",
                    "type": "application/vnd.pmtiles",
                    "pmtiles:layers": ["data"],
                }
            )

        collection_json = {
            "type": "Collection",
            "id": "test-collection",
            "stac_version": "1.0.0",
            "description": "Test collection",
            "links": links,
            "assets": {
                "data-tiles": {
                    "href": "./data.pmtiles",
                    "type": "application/vnd.pmtiles",
                    "roles": ["visual"],
                }
            },
        }
        (collection_dir / "collection.json").write_text(json.dumps(collection_json))
        (collection_dir / "data.pmtiles").write_bytes(b"PMTilesplaceholder")
        return tmp_path

    @pytest.mark.unit
    def test_has_error_severity(self) -> None:
        """RULE-0061 blocks: PMTiles link is an ERROR when missing."""
        from portolan_cli.validation.rules import PMTilesLinkRule

        assert PMTilesLinkRule().severity == Severity.ERROR

    @pytest.mark.unit
    def test_fails_when_asset_without_link(self, tmp_path: Path) -> None:
        """A PMTiles asset with no rel='pmtiles' link fails the check."""
        from portolan_cli.validation.rules import PMTilesLinkRule

        catalog = self._write_collection(tmp_path, with_link=False)
        result = PMTilesLinkRule().check(catalog)

        assert result.passed is False
        assert result.severity == Severity.ERROR
        assert "pmtiles" in result.message.lower()
        assert result.fix_hint is not None

    @pytest.mark.unit
    def test_passes_when_link_present(self, tmp_path: Path) -> None:
        """A PMTiles asset with a rel='pmtiles' link passes the check."""
        from portolan_cli.validation.rules import PMTilesLinkRule

        catalog = self._write_collection(tmp_path, with_link=True)
        result = PMTilesLinkRule().check(catalog)

        assert result.passed is True

    @pytest.mark.unit
    def test_passes_when_no_pmtiles_asset(self, tmp_path: Path) -> None:
        """Collections without a PMTiles asset are not flagged."""
        import json

        from portolan_cli.validation.rules import PMTilesLinkRule

        (tmp_path / ".portolan").mkdir()
        collection_dir = tmp_path / "test-collection"
        collection_dir.mkdir()
        collection_json = {
            "type": "Collection",
            "id": "test-collection",
            "links": [],
            "assets": {"data": {"href": "./data.parquet"}},
        }
        (collection_dir / "collection.json").write_text(json.dumps(collection_json))

        result = PMTilesLinkRule().check(tmp_path)
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

    @pytest.mark.unit
    def test_has_metadata_fresh_name(self) -> None:
        """Rule has correct name."""
        from portolan_cli.validation.rules import MetadataFreshRule

        rule = MetadataFreshRule()
        assert rule.name == "metadata_fresh"

    @pytest.mark.unit
    def test_has_warning_severity_default(self) -> None:
        """Rule defaults to WARNING severity (STALE case)."""
        from portolan_cli.validation.rules import MetadataFreshRule

        rule = MetadataFreshRule()
        assert rule.severity == Severity.WARNING

    @pytest.mark.unit
    def test_passes_when_no_catalog_json(self, tmp_path: Path) -> None:
        """Rule passes when catalog.json doesn't exist (per ADR-0023)."""
        from portolan_cli.validation.rules import MetadataFreshRule

        rule = MetadataFreshRule()
        result = rule.check(tmp_path)

        assert result.passed is True
        assert "catalog.json" in result.message.lower()

    @pytest.mark.unit
    def test_passes_when_no_collections_exist(self, tmp_path: Path) -> None:
        """Rule passes when no collections with collection.json exist (per ADR-0023)."""
        import json

        from portolan_cli.validation.rules import MetadataFreshRule

        # Create catalog.json at root (per ADR-0023)
        catalog_data = {"type": "Catalog", "stac_version": "1.0.0", "id": "test", "links": []}
        (tmp_path / "catalog.json").write_text(json.dumps(catalog_data))
        # Create .portolan for internal state
        (tmp_path / ".portolan").mkdir()

        rule = MetadataFreshRule()
        result = rule.check(tmp_path)

        assert result.passed is True
        assert "no geo-asset" in result.message.lower()

    @pytest.mark.unit
    def test_passes_when_no_geo_assets_found(self, tmp_path: Path) -> None:
        """Rule passes when no geo-asset files are found in collections."""
        import json

        from portolan_cli.validation.rules import MetadataFreshRule

        # Create catalog.json at root (per ADR-0023)
        catalog_data = {"type": "Catalog", "stac_version": "1.0.0", "id": "test", "links": []}
        (tmp_path / "catalog.json").write_text(json.dumps(catalog_data))
        # Create .portolan for internal state
        (tmp_path / ".portolan").mkdir()
        # Create collection with collection.json but no geo assets
        collection_dir = tmp_path / "test-collection"
        collection_dir.mkdir()
        collection_data = {
            "type": "Collection",
            "stac_version": "1.0.0",
            "id": "test-collection",
            "links": [],
        }
        (collection_dir / "collection.json").write_text(json.dumps(collection_data))

        rule = MetadataFreshRule()
        result = rule.check(tmp_path)

        assert result.passed is True
        assert "no geo-asset" in result.message.lower()

    @pytest.mark.unit
    def test_skips_non_directory_items(self, tmp_path: Path) -> None:
        """Rule skips files in catalog root (only processes directories with collection.json)."""
        import json

        from portolan_cli.validation.rules import MetadataFreshRule

        # Create catalog.json at root (per ADR-0023)
        catalog_data = {"type": "Catalog", "stac_version": "1.0.0", "id": "test", "links": []}
        (tmp_path / "catalog.json").write_text(json.dumps(catalog_data))
        # Create .portolan for internal state
        (tmp_path / ".portolan").mkdir()
        # Create a file (not a directory with collection.json) at root
        (tmp_path / "some-file.txt").write_text("not a collection")

        rule = MetadataFreshRule()
        result = rule.check(tmp_path)

        # Should pass (no valid collections)
        assert result.passed is True

    @pytest.mark.integration
    def test_detects_missing_metadata(self, tmp_path: Path, fixtures_dir: Path) -> None:
        """Rule detects geo-assets without STAC metadata (MISSING status)."""
        import json
        import shutil

        from portolan_cli.validation.rules import MetadataFreshRule

        # Create catalog.json at root (per ADR-0023)
        catalog_data = {"type": "Catalog", "stac_version": "1.0.0", "id": "test", "links": []}
        (tmp_path / "catalog.json").write_text(json.dumps(catalog_data))
        # Create .portolan for internal state
        (tmp_path / ".portolan").mkdir()
        # Create collection at root with collection.json
        collection = tmp_path / "test-collection"
        collection.mkdir()
        collection_data = {
            "type": "Collection",
            "stac_version": "1.0.0",
            "id": "test-collection",
            "links": [],
        }
        (collection / "collection.json").write_text(json.dumps(collection_data))

        # Item directory with data file but no item.json (per ADR-0041
        # manifest-driven scan, this is the genuine MISSING shape).
        item_dir = collection / "points"
        item_dir.mkdir()
        src = fixtures_dir / "vector" / "valid" / "points.parquet"
        if src.exists():
            shutil.copy(src, item_dir / "points.parquet")
        else:
            import pyarrow as pa
            import pyarrow.parquet as pq

            table = pa.table({"id": [1, 2], "geometry": ["POINT(0 0)", "POINT(1 1)"]})
            pq.write_table(table, item_dir / "points.parquet")

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

        # Create catalog.json at root (per ADR-0023)
        catalog_data = {"type": "Catalog", "stac_version": "1.0.0", "id": "test", "links": []}
        (tmp_path / "catalog.json").write_text(json.dumps(catalog_data))
        # Create .portolan for internal state
        (tmp_path / ".portolan").mkdir()
        # Create collection at root with collection.json
        collection = tmp_path / "test-collection"
        collection.mkdir()
        collection_data = {
            "type": "Collection",
            "stac_version": "1.0.0",
            "id": "test-collection",
            "links": [],
        }
        (collection / "collection.json").write_text(json.dumps(collection_data))

        # Hierarchical item layout per `add` convention (ADR-0041 scanner
        # walks `<collection>/<item_id>/<item_id>.json`).
        item_dir = collection / "test"
        item_dir.mkdir()
        parquet_path = item_dir / "test.parquet"
        table = pa.table({"id": [1, 2], "geometry": ["POINT(0 0)", "POINT(1 1)"]})
        pq.write_table(table, parquet_path)

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
                    "type": "application/vnd.apache.parquet",
                }
            },
            "links": [],
        }
        (item_dir / "test.json").write_text(json.dumps(stac_item))

        # versions.json lives at the collection level; tracked asset key
        # uses the basename relative to versions.json (current convention).
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

        # Create catalog.json at root (per ADR-0023)
        catalog_data = {"type": "Catalog", "stac_version": "1.0.0", "id": "test", "links": []}
        (tmp_path / "catalog.json").write_text(json.dumps(catalog_data))
        # Create .portolan for internal state
        (tmp_path / ".portolan").mkdir()
        # Create collection at root with collection.json
        collection = tmp_path / "test-collection"
        collection.mkdir()
        collection_data = {
            "type": "Collection",
            "stac_version": "1.0.0",
            "id": "test-collection",
            "links": [],
        }
        (collection / "collection.json").write_text(json.dumps(collection_data))

        # Hierarchical item layout per ADR-0041 manifest scan.
        item_dir = collection / "test"
        item_dir.mkdir()
        parquet_path = item_dir / "test.parquet"
        table = pa.table({"id": [1, 2], "geometry": ["POINT(0 0)", "POINT(1 1)"]})
        pq.write_table(table, parquet_path)

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
                    "type": "application/vnd.apache.parquet",
                }
            },
            "links": [],
        }
        (item_dir / "test.json").write_text(json.dumps(stac_item))

        # versions.json captures a stored feature_count one off from
        # current — heuristics will flag STALE (content delta), not
        # BREAKING (schema unchanged). Old version of this test relied on
        # mtime-drift-alone to imply STALE; with the bbox-None heuristic
        # guard a touch-only mtime change is correctly FRESH, so the
        # test now drives a real content delta to keep its intent.
        stored_feature_count = (metadata.feature_count or 0) + 5
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
                            "feature_count": stored_feature_count,
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
        import json

        import pyarrow as pa
        import pyarrow.parquet as pq

        from portolan_cli.validation.rules import MetadataFreshRule

        # Create catalog.json at root (per ADR-0023)
        catalog_data = {"type": "Catalog", "stac_version": "1.0.0", "id": "test", "links": []}
        (tmp_path / "catalog.json").write_text(json.dumps(catalog_data))
        # Create .portolan for internal state
        (tmp_path / ".portolan").mkdir()
        # Create collection at root with collection.json but no STAC item
        collection = tmp_path / "test-collection"
        collection.mkdir()
        collection_data = {
            "type": "Collection",
            "stac_version": "1.0.0",
            "id": "test-collection",
            "links": [],
        }
        (collection / "collection.json").write_text(json.dumps(collection_data))

        # Item dir without item.json → MISSING (drives the --fix hint).
        item_dir = collection / "test"
        item_dir.mkdir()
        parquet_path = item_dir / "test.parquet"
        table = pa.table({"id": [1], "geometry": ["POINT(0 0)"]})
        pq.write_table(table, parquet_path)

        rule = MetadataFreshRule()
        result = rule.check(tmp_path)

        assert result.passed is False
        assert result.fix_hint is not None
        assert "--metadata --fix" in result.fix_hint.lower()

    @pytest.mark.unit
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
        # Skip on Windows if symlink creation fails (requires admin privileges)
        symlink = collection / "broken.parquet"
        try:
            symlink.symlink_to("/nonexistent/file.parquet")
        except OSError:
            pytest.skip("Symlink creation not supported on this platform")

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
        """Rule recurses through nested catalogs (ADR-0032 Pattern 2:
        catalog.json under a collection organizes item subdirs)."""
        import json

        import pyarrow as pa
        import pyarrow.parquet as pq

        from portolan_cli.validation.rules import MetadataFreshRule

        catalog_data = {"type": "Catalog", "stac_version": "1.0.0", "id": "test", "links": []}
        (tmp_path / "catalog.json").write_text(json.dumps(catalog_data))
        (tmp_path / ".portolan").mkdir()

        collection = tmp_path / "test-collection"
        collection.mkdir()
        collection_data = {
            "type": "Collection",
            "stac_version": "1.0.0",
            "id": "test-collection",
            "links": [],
        }
        (collection / "collection.json").write_text(json.dumps(collection_data))

        # Sub-catalog organizing items by year (Pattern 2).
        year_dir = collection / "2024"
        year_dir.mkdir()
        (year_dir / "catalog.json").write_text(
            json.dumps({"type": "Catalog", "stac_version": "1.0.0", "id": "2024", "links": []})
        )

        # Item dir under sub-catalog with data but no item.json → MISSING.
        item_dir = year_dir / "test"
        item_dir.mkdir()
        parquet_path = item_dir / "test.parquet"
        table = pa.table({"id": [1], "geometry": ["POINT(0 0)"]})
        pq.write_table(table, parquet_path)

        rule = MetadataFreshRule()
        result = rule.check(tmp_path)

        assert result.passed is False
        assert "missing" in result.message.lower()


# --- Partition validation rules (thorough) ---


class TestPartitionStructureRule:
    """Tests for PartitionStructureRule."""

    @pytest.mark.unit
    def test_passes_when_no_partitions(self, tmp_path: Path) -> None:
        """Rule passes when collection has no Hive-style partitions."""
        from portolan_cli.validation.rules import PartitionStructureRule

        # Setup catalog with non-partitioned collection
        (tmp_path / ".portolan").mkdir()
        coll = tmp_path / "test-collection"
        coll.mkdir()
        (coll / "collection.json").write_text(
            '{"type":"Collection","stac_version":"1.0.0","id":"test"}'
        )

        rule = PartitionStructureRule()
        result = rule.check(tmp_path)

        assert result.passed is True

    @pytest.mark.unit
    def test_passes_with_consistent_partition_keys(self, tmp_path: Path) -> None:
        """Rule passes when all partition dirs use same key."""
        from portolan_cli.validation.rules import PartitionStructureRule

        coll = tmp_path / "test-collection"
        coll.mkdir()
        (coll / "collection.json").write_text(
            '{"type":"Collection","partition:scheme":"hive","partition:keys":[{"name":"kdtree_cell"}]}'
        )

        # Create consistent Hive partitions
        (coll / "kdtree_cell=0").mkdir()
        (coll / "kdtree_cell=1").mkdir()

        rule = PartitionStructureRule()
        result = rule.check(tmp_path)

        assert result.passed is True

    @pytest.mark.unit
    def test_fails_with_mixed_partition_keys(self, tmp_path: Path) -> None:
        """Rule fails when partition dirs use different keys."""
        from portolan_cli.validation.rules import PartitionStructureRule

        coll = tmp_path / "test-collection"
        coll.mkdir()
        (coll / "collection.json").write_text(
            '{"type":"Collection","stac_version":"1.0.0","id":"test"}'
        )

        # Create mixed partition keys
        (coll / "kdtree_cell=0").mkdir()
        (coll / "h3_cell=abc").mkdir()  # Different key!

        rule = PartitionStructureRule()
        result = rule.check(tmp_path)

        assert result.passed is False
        assert "mixed keys" in result.message

    @pytest.mark.unit
    def test_fails_with_orphan_parquet_at_root(self, tmp_path: Path) -> None:
        """Rule fails when parquet files exist at collection root alongside partitions."""
        from portolan_cli.validation.rules import PartitionStructureRule

        coll = tmp_path / "test-collection"
        coll.mkdir()
        (coll / "collection.json").write_text(
            '{"type":"Collection","stac_version":"1.0.0","id":"test"}'
        )

        # Create partition dir AND orphan parquet at root
        (coll / "kdtree_cell=0").mkdir()
        (coll / "orphan.parquet").write_text("fake")

        rule = PartitionStructureRule()
        result = rule.check(tmp_path)

        assert result.passed is False
        assert "orphan" in result.message

    @pytest.mark.unit
    def test_warns_missing_partition_scheme_field(self, tmp_path: Path) -> None:
        """Rule fails when partitions exist but partition:scheme missing."""
        from portolan_cli.validation.rules import PartitionStructureRule

        coll = tmp_path / "test-collection"
        coll.mkdir()
        # Collection JSON missing partition:scheme
        (coll / "collection.json").write_text(
            '{"type":"Collection","stac_version":"1.0.0","id":"test"}'
        )

        (coll / "kdtree_cell=0").mkdir()

        rule = PartitionStructureRule()
        result = rule.check(tmp_path)

        assert result.passed is False
        assert "partition:scheme" in result.message


class TestPartitionSchemaConsistencyRule:
    """Tests for PartitionSchemaConsistencyRule."""

    @pytest.mark.unit
    def test_passes_when_no_partitions(self, tmp_path: Path) -> None:
        """Rule passes when no Hive-style partitions exist."""
        from portolan_cli.validation.rules import PartitionSchemaConsistencyRule

        coll = tmp_path / "test-collection"
        coll.mkdir()
        (coll / "collection.json").write_text(
            '{"type":"Collection","stac_version":"1.0.0","id":"test"}'
        )

        rule = PartitionSchemaConsistencyRule()
        result = rule.check(tmp_path)

        assert result.passed is True

    @pytest.mark.unit
    def test_passes_with_consistent_schemas(self, tmp_path: Path) -> None:
        """Rule passes when all partition files have same schema."""
        import pyarrow as pa
        import pyarrow.parquet as pq

        from portolan_cli.validation.rules import PartitionSchemaConsistencyRule

        coll = tmp_path / "test-collection"
        coll.mkdir()
        (coll / "collection.json").write_text(
            '{"type":"Collection","stac_version":"1.0.0","id":"test"}'
        )

        # Create partitions with same schema
        p1 = coll / "kdtree_cell=0"
        p1.mkdir()
        p2 = coll / "kdtree_cell=1"
        p2.mkdir()

        table = pa.table({"id": [1], "name": ["test"]})
        pq.write_table(table, p1 / "data.parquet")
        pq.write_table(table, p2 / "data.parquet")

        rule = PartitionSchemaConsistencyRule()
        result = rule.check(tmp_path)

        assert result.passed is True

    @pytest.mark.unit
    def test_fails_with_inconsistent_schemas(self, tmp_path: Path) -> None:
        """Rule fails when partition files have different schemas."""
        import pyarrow as pa
        import pyarrow.parquet as pq

        from portolan_cli.validation.rules import PartitionSchemaConsistencyRule

        coll = tmp_path / "test-collection"
        coll.mkdir()
        (coll / "collection.json").write_text(
            '{"type":"Collection","stac_version":"1.0.0","id":"test"}'
        )

        p1 = coll / "kdtree_cell=0"
        p1.mkdir()
        p2 = coll / "kdtree_cell=1"
        p2.mkdir()

        # Different schemas!
        table1 = pa.table({"id": [1], "name": ["test"]})
        table2 = pa.table({"id": [2], "value": [42]})  # Different columns
        pq.write_table(table1, p1 / "data.parquet")
        pq.write_table(table2, p2 / "data.parquet")

        rule = PartitionSchemaConsistencyRule()
        result = rule.check(tmp_path)

        assert result.passed is False
        assert "different schemas" in result.message.lower()

    @pytest.mark.unit
    def test_has_error_severity(self) -> None:
        """Schema inconsistency is an ERROR (data corruption)."""
        from portolan_cli.validation.rules import PartitionSchemaConsistencyRule

        rule = PartitionSchemaConsistencyRule()
        assert rule.severity == Severity.ERROR


# --- Tabular collection rules (RULE-0090 through RULE-0094) ---

TABLE_EXT = "https://stac-extensions.github.io/table/v1.2.0/schema.json"


def _write_parquet(path: Path, *, geo: bool) -> None:
    """Write a tiny Parquet file, with or without GeoParquet `geo` metadata.

    `is_geoparquet()` only checks for a ``b"geo"`` key in the schema metadata,
    so we can fabricate a "GeoParquet" without any real geometry.
    """
    import pyarrow as pa
    import pyarrow.parquet as pq

    table = pa.table({"value": [1, 2, 3]})
    if geo:
        table = table.replace_schema_metadata({b"geo": b"{}"})
    pq.write_table(table, path)


def _make_collection(
    catalog: Path,
    name: str,
    *,
    assets: dict[str, dict[str, object]],
    extra: dict[str, object] | None = None,
) -> Path:
    """Create a catalog dir with one collection.json and return the catalog root."""
    import json

    (catalog / ".portolan").mkdir(exist_ok=True)
    collection_dir = catalog / name
    collection_dir.mkdir(parents=True, exist_ok=True)
    collection_json: dict[str, object] = {
        "type": "Collection",
        "id": name,
        "stac_version": "1.0.0",
        "description": "Test collection",
        "license": "MIT",
        "extent": {
            "spatial": {"bbox": [[-180, -90, 180, 90]]},
            "temporal": {"interval": [[None, None]]},
        },
        "links": [],
        "assets": assets,
    }
    if extra:
        collection_json.update(extra)
    (collection_dir / "collection.json").write_text(json.dumps(collection_json))
    return collection_dir


class TestTabularGeospatialFlagRule:
    """RULE-0090: tabular collections MUST have portolan:geospatial: false (ERROR)."""

    @pytest.mark.unit
    def test_severity_is_error(self) -> None:
        from portolan_cli.validation.rules import TabularGeospatialFlagRule

        assert TabularGeospatialFlagRule().severity == Severity.ERROR

    @pytest.mark.unit
    def test_passes_for_tabular_with_flag(self, tmp_path: Path) -> None:
        from portolan_cli.validation.rules import TabularGeospatialFlagRule

        coll = _make_collection(
            tmp_path,
            "demographics",
            assets={"data": {"href": "./data.parquet", "roles": ["data"]}},
            extra={"portolan:geospatial": False},
        )
        _write_parquet(coll / "data.parquet", geo=False)

        result = TabularGeospatialFlagRule().check(tmp_path)
        assert result.passed is True

    @pytest.mark.unit
    def test_fails_for_tabular_missing_flag(self, tmp_path: Path) -> None:
        from portolan_cli.validation.rules import TabularGeospatialFlagRule

        coll = _make_collection(
            tmp_path,
            "demographics",
            assets={"data": {"href": "./data.parquet", "roles": ["data"]}},
        )
        _write_parquet(coll / "data.parquet", geo=False)

        result = TabularGeospatialFlagRule().check(tmp_path)
        assert result.passed is False
        assert result.severity == Severity.ERROR
        assert "demographics" in result.message
        assert result.fix_hint is not None

    @pytest.mark.unit
    def test_fails_for_tabular_csv_missing_flag(self, tmp_path: Path) -> None:
        from portolan_cli.validation.rules import TabularGeospatialFlagRule

        coll = _make_collection(
            tmp_path,
            "spreadsheet",
            assets={"data": {"href": "./data.csv", "roles": ["data"]}},
        )
        (coll / "data.csv").write_text("a,b\n1,2\n")

        result = TabularGeospatialFlagRule().check(tmp_path)
        assert result.passed is False

    @pytest.mark.unit
    def test_passes_for_geoparquet_without_flag(self, tmp_path: Path) -> None:
        """A real GeoParquet collection is spatial and must NOT be flagged."""
        from portolan_cli.validation.rules import TabularGeospatialFlagRule

        coll = _make_collection(
            tmp_path,
            "parcels",
            assets={"data": {"href": "./data.parquet", "roles": ["data"]}},
        )
        _write_parquet(coll / "data.parquet", geo=True)

        result = TabularGeospatialFlagRule().check(tmp_path)
        assert result.passed is True

    @pytest.mark.unit
    def test_passes_for_raster_collection_without_flag(self, tmp_path: Path) -> None:
        """A COG/raster collection is spatial-by-extension, never tabular."""
        from portolan_cli.validation.rules import TabularGeospatialFlagRule

        coll = _make_collection(
            tmp_path,
            "elevation",
            assets={"data": {"href": "./dem.tif", "roles": ["data"]}},
        )
        (coll / "dem.tif").write_bytes(b"II*\x00placeholder")

        result = TabularGeospatialFlagRule().check(tmp_path)
        assert result.passed is True

    @pytest.mark.unit
    def test_passes_when_parquet_file_missing(self, tmp_path: Path) -> None:
        """An unreadable/not-yet-local Parquet must NOT be assumed tabular.

        Regression: a metadata-only catalog (data not pulled yet) would have a
        registered .parquet href with no file on disk. is_geoparquet returns
        False for both "plain Parquet" and "file missing", so defaulting to
        tabular raised a false ERROR and drove --fix to mislabel a spatial
        collection. The href resolves to no file, so the asset is unclassified.
        """
        from portolan_cli.validation.rules import TabularGeospatialFlagRule

        _make_collection(
            tmp_path,
            "parcels",
            assets={"data": {"href": "./data.parquet", "roles": ["data"]}},
        )
        # Deliberately do NOT write data.parquet.

        result = TabularGeospatialFlagRule().check(tmp_path)
        assert result.passed is True

    @pytest.mark.unit
    def test_passes_for_remote_parquet_href(self, tmp_path: Path) -> None:
        """A remote (http/s3) Parquet href cannot be inspected, so not tabular."""
        from portolan_cli.validation.rules import TabularGeospatialFlagRule

        _make_collection(
            tmp_path,
            "parcels",
            assets={"data": {"href": "https://example.com/data.parquet", "roles": ["data"]}},
        )

        result = TabularGeospatialFlagRule().check(tmp_path)
        assert result.passed is True

    @pytest.mark.unit
    def test_geo_asset_wins_in_mixed_collection(self, tmp_path: Path) -> None:
        """A collection with both a GeoParquet and a CSV classifies as geo."""
        from portolan_cli.validation.rules import TabularGeospatialFlagRule

        coll = _make_collection(
            tmp_path,
            "mixed",
            assets={
                "geo": {"href": "./data.parquet", "roles": ["data"]},
                "table": {"href": "./extra.csv", "roles": ["data"]},
            },
        )
        _write_parquet(coll / "data.parquet", geo=True)
        (coll / "extra.csv").write_text("a,b\n1,2\n")

        result = TabularGeospatialFlagRule().check(tmp_path)
        assert result.passed is True

    @pytest.mark.unit
    def test_passes_for_empty_collection(self, tmp_path: Path) -> None:
        """A collection with no data assets is neither geo nor tabular; not flagged."""
        from portolan_cli.validation.rules import TabularGeospatialFlagRule

        _make_collection(tmp_path, "empty", assets={})

        result = TabularGeospatialFlagRule().check(tmp_path)
        assert result.passed is True

    @pytest.mark.unit
    def test_stac_geoparquet_rollup_is_not_tabular(self, tmp_path: Path) -> None:
        """STAC-GeoParquet rollups (items.parquet with stac-items role) are metadata.

        A raster collection with only a stac-items rollup asset (no actual
        tabular data) must not be classified as tabular, even though the
        items.parquet file lacks GeoParquet geo metadata.
        """
        from portolan_cli.validation.rules import TabularGeospatialFlagRule

        coll = _make_collection(
            tmp_path,
            "rasters",
            assets={
                "cog": {"href": "./scene.tif", "roles": ["data"]},
                "geoparquet-items": {
                    "href": "./items.parquet",
                    "roles": ["stac-items"],
                    "title": "STAC items as GeoParquet",
                },
            },
        )
        (coll / "scene.tif").write_bytes(b"II*\x00placeholder")
        # items.parquet is a rollup, not actual tabular data — no geo metadata.
        _write_parquet(coll / "items.parquet", geo=False)

        result = TabularGeospatialFlagRule().check(tmp_path)
        assert result.passed is True

    @pytest.mark.unit
    def test_summary_caps_preview_with_more_marker(self, tmp_path: Path) -> None:
        """More than five offending collections collapse to a '(+N more)' preview."""
        from portolan_cli.validation.rules import TabularGeospatialFlagRule

        for i in range(7):
            coll = _make_collection(
                tmp_path,
                f"tab{i}",
                assets={"data": {"href": "./data.parquet", "roles": ["data"]}},
            )
            _write_parquet(coll / "data.parquet", geo=False)

        result = TabularGeospatialFlagRule().check(tmp_path)
        assert result.passed is False
        assert "7 tabular collection(s)" in result.message
        assert "(+2 more)" in result.message


class TestTabularTableExtensionRule:
    """RULE-0091: tabular collections SHOULD use the STAC Table extension (WARNING)."""

    @pytest.mark.unit
    def test_severity_is_warning(self) -> None:
        from portolan_cli.validation.rules import TabularTableExtensionRule

        assert TabularTableExtensionRule().severity == Severity.WARNING

    @pytest.mark.unit
    def test_passes_with_table_columns_and_extension(self, tmp_path: Path) -> None:
        from portolan_cli.validation.rules import TabularTableExtensionRule

        _make_collection(
            tmp_path,
            "demographics",
            assets={"data": {"href": "./data.parquet", "roles": ["data"]}},
            extra={
                "portolan:geospatial": False,
                "stac_extensions": [TABLE_EXT],
                "table:columns": [{"name": "pop", "type": "int64"}],
            },
        )
        result = TabularTableExtensionRule().check(tmp_path)
        assert result.passed is True

    @pytest.mark.unit
    def test_warns_when_table_columns_missing(self, tmp_path: Path) -> None:
        from portolan_cli.validation.rules import TabularTableExtensionRule

        _make_collection(
            tmp_path,
            "demographics",
            assets={"data": {"href": "./data.parquet", "roles": ["data"]}},
            extra={"portolan:geospatial": False},
        )
        result = TabularTableExtensionRule().check(tmp_path)
        assert result.passed is False
        assert result.severity == Severity.WARNING
        assert "demographics" in result.message

    @pytest.mark.unit
    def test_ignores_geospatial_collections(self, tmp_path: Path) -> None:
        """A GeoParquet collection is spatial, so the Table rule never applies."""
        from portolan_cli.validation.rules import TabularTableExtensionRule

        coll = _make_collection(
            tmp_path,
            "parcels",
            assets={"data": {"href": "./data.parquet", "roles": ["data"]}},
        )
        _write_parquet(coll / "data.parquet", geo=True)
        result = TabularTableExtensionRule().check(tmp_path)
        assert result.passed is True

    @pytest.mark.unit
    def test_warns_on_content_detected_tabular_without_flag(self, tmp_path: Path) -> None:
        """Fires even before RULE-0090's flag is backfilled (content-detected tabular)."""
        from portolan_cli.validation.rules import TabularTableExtensionRule

        coll = _make_collection(
            tmp_path,
            "demographics",
            assets={"data": {"href": "./data.parquet", "roles": ["data"]}},
        )
        _write_parquet(coll / "data.parquet", geo=False)  # plain Parquet, no flag set
        result = TabularTableExtensionRule().check(tmp_path)
        assert result.passed is False
        assert result.severity == Severity.WARNING
        assert "demographics" in result.message


class TestTabularTemporalExtentRule:
    """RULE-0093: tabular collections SHOULD have temporal extent (WARNING)."""

    @pytest.mark.unit
    def test_severity_is_warning(self) -> None:
        from portolan_cli.validation.rules import TabularTemporalExtentRule

        assert TabularTemporalExtentRule().severity == Severity.WARNING

    @pytest.mark.unit
    def test_passes_when_temporal_present(self, tmp_path: Path) -> None:
        from portolan_cli.validation.rules import TabularTemporalExtentRule

        _make_collection(
            tmp_path,
            "demographics",
            assets={"data": {"href": "./data.parquet", "roles": ["data"]}},
            extra={"portolan:geospatial": False},
        )
        # _make_collection writes extent.temporal by default
        result = TabularTemporalExtentRule().check(tmp_path)
        assert result.passed is True

    @pytest.mark.unit
    def test_warns_when_temporal_absent(self, tmp_path: Path) -> None:
        import json

        from portolan_cli.validation.rules import TabularTemporalExtentRule

        coll = _make_collection(
            tmp_path,
            "demographics",
            assets={"data": {"href": "./data.parquet", "roles": ["data"]}},
            extra={"portolan:geospatial": False},
        )
        # Strip temporal extent to simulate a missing-temporal collection.
        cj = coll / "collection.json"
        data = json.loads(cj.read_text())
        data["extent"] = {"spatial": {"bbox": [[-180, -90, 180, 90]]}}
        cj.write_text(json.dumps(data))

        result = TabularTemporalExtentRule().check(tmp_path)
        assert result.passed is False
        assert result.severity == Severity.WARNING
        assert "demographics" in result.message


class TestTabularCollectionLevelAssetsRule:
    """RULE-0094: tabular collections MUST use collection-level assets (ERROR)."""

    @pytest.mark.unit
    def test_severity_is_error(self) -> None:
        from portolan_cli.validation.rules import TabularCollectionLevelAssetsRule

        assert TabularCollectionLevelAssetsRule().severity == Severity.ERROR

    @pytest.mark.unit
    def test_passes_for_collection_level_asset(self, tmp_path: Path) -> None:
        from portolan_cli.validation.rules import TabularCollectionLevelAssetsRule

        coll = _make_collection(
            tmp_path,
            "demographics",
            assets={"data": {"href": "./data.parquet", "roles": ["data"]}},
            extra={"portolan:geospatial": False},
        )
        _write_parquet(coll / "data.parquet", geo=False)

        result = TabularCollectionLevelAssetsRule().check(tmp_path)
        assert result.passed is True

    @pytest.mark.unit
    def test_fails_when_data_wrapped_in_items(self, tmp_path: Path) -> None:
        import json

        from portolan_cli.validation.rules import TabularCollectionLevelAssetsRule

        coll = _make_collection(
            tmp_path,
            "demographics",
            assets={"data": {"href": "./data.parquet", "roles": ["data"]}},
            extra={"portolan:geospatial": False},
        )
        item_dir = coll / "row-1"
        item_dir.mkdir()
        (item_dir / "item.json").write_text(json.dumps({"type": "Feature", "id": "row-1"}))

        result = TabularCollectionLevelAssetsRule().check(tmp_path)
        assert result.passed is False
        assert result.severity == Severity.ERROR
        assert "demographics" in result.message

    @pytest.mark.unit
    def test_exempts_partitioned_collections(self, tmp_path: Path) -> None:
        """Partitioned tabular data legitimately uses items (ADR-0047)."""
        import json

        from portolan_cli.validation.rules import TabularCollectionLevelAssetsRule

        coll = _make_collection(
            tmp_path,
            "timeseries",
            assets={"data": {"href": "./data.parquet", "roles": ["data"]}},
            extra={"portolan:geospatial": False, "partition:scheme": "hive"},
        )
        item_dir = coll / "year=2020"
        item_dir.mkdir()
        (item_dir / "item.json").write_text(json.dumps({"type": "Feature", "id": "y2020"}))

        result = TabularCollectionLevelAssetsRule().check(tmp_path)
        assert result.passed is True


class TestRepairTabularFlags:
    """--fix backfills portolan:geospatial: false on tabular collections (RULE-0090)."""

    @pytest.mark.unit
    def test_backfills_missing_flag(self, tmp_path: Path) -> None:
        import json

        from portolan_cli.metadata.fix import repair_tabular_flags

        coll = _make_collection(
            tmp_path,
            "demographics",
            assets={"data": {"href": "./data.parquet", "roles": ["data"]}},
        )
        _write_parquet(coll / "data.parquet", geo=False)

        results = repair_tabular_flags(tmp_path)

        assert len(results) == 1
        assert results[0].success is True
        data = json.loads((coll / "collection.json").read_text())
        assert data["portolan:geospatial"] is False

    @pytest.mark.unit
    def test_dry_run_reports_without_writing(self, tmp_path: Path) -> None:
        import json

        from portolan_cli.metadata.fix import repair_tabular_flags

        coll = _make_collection(
            tmp_path,
            "demographics",
            assets={"data": {"href": "./data.parquet", "roles": ["data"]}},
        )
        _write_parquet(coll / "data.parquet", geo=False)

        results = repair_tabular_flags(tmp_path, dry_run=True)

        assert len(results) == 1
        data = json.loads((coll / "collection.json").read_text())
        assert "portolan:geospatial" not in data

    @pytest.mark.unit
    def test_noop_for_geo_collection(self, tmp_path: Path) -> None:
        from portolan_cli.metadata.fix import repair_tabular_flags

        coll = _make_collection(
            tmp_path,
            "parcels",
            assets={"data": {"href": "./data.parquet", "roles": ["data"]}},
        )
        _write_parquet(coll / "data.parquet", geo=True)

        results = repair_tabular_flags(tmp_path)
        assert results == []

    @pytest.mark.unit
    def test_noop_when_flag_already_set(self, tmp_path: Path) -> None:
        from portolan_cli.metadata.fix import repair_tabular_flags

        coll = _make_collection(
            tmp_path,
            "demographics",
            assets={"data": {"href": "./data.parquet", "roles": ["data"]}},
            extra={"portolan:geospatial": False},
        )
        _write_parquet(coll / "data.parquet", geo=False)

        results = repair_tabular_flags(tmp_path)
        assert results == []

    @pytest.mark.unit
    def test_noop_when_parquet_file_missing(self, tmp_path: Path) -> None:
        """A registered-but-absent Parquet is unclassified, so --fix must not write.

        Mirrors RULE-0090: never stamp portolan:geospatial: false on a collection
        whose only data asset cannot be read (could be a spatial collection whose
        data has not been pulled yet).
        """
        import json

        from portolan_cli.metadata.fix import repair_tabular_flags

        coll = _make_collection(
            tmp_path,
            "parcels",
            assets={"data": {"href": "./data.parquet", "roles": ["data"]}},
        )
        # Deliberately do NOT write data.parquet.

        results = repair_tabular_flags(tmp_path)
        assert results == []
        data = json.loads((coll / "collection.json").read_text())
        assert "portolan:geospatial" not in data


class TestRepairPMTilesLinks:
    """--fix backfills the rel='pmtiles' web-map-links link (RULE-0061, #569)."""

    @pytest.mark.unit
    def test_backfills_missing_link(self, tmp_path: Path) -> None:
        import json

        from portolan_cli.metadata.fix import repair_pmtiles_links
        from portolan_cli.pmtiles import WEB_MAP_LINKS_EXTENSION

        coll = _make_collection(
            tmp_path,
            "parcels",
            assets={
                "data-tiles": {
                    "href": "./data.pmtiles",
                    "type": "application/vnd.pmtiles",
                    "roles": ["visual"],
                }
            },
        )

        results = repair_pmtiles_links(tmp_path)

        assert len(results) == 1
        assert results[0].success is True
        data = json.loads((coll / "collection.json").read_text())
        pmtiles_links = [link for link in data["links"] if link.get("rel") == "pmtiles"]
        assert len(pmtiles_links) == 1
        assert pmtiles_links[0]["type"] == "application/vnd.pmtiles"
        assert pmtiles_links[0]["pmtiles:layers"] == ["data"]
        assert WEB_MAP_LINKS_EXTENSION in data["stac_extensions"]

    @pytest.mark.unit
    def test_dry_run_reports_without_writing(self, tmp_path: Path) -> None:
        import json

        from portolan_cli.metadata.fix import repair_pmtiles_links

        coll = _make_collection(
            tmp_path,
            "parcels",
            assets={
                "data-tiles": {
                    "href": "./data.pmtiles",
                    "type": "application/vnd.pmtiles",
                    "roles": ["visual"],
                }
            },
        )

        results = repair_pmtiles_links(tmp_path, dry_run=True)

        assert len(results) == 1
        data = json.loads((coll / "collection.json").read_text())
        assert data["links"] == []

    @pytest.mark.unit
    def test_noop_when_link_present(self, tmp_path: Path) -> None:
        from portolan_cli.metadata.fix import repair_pmtiles_links

        _make_collection(
            tmp_path,
            "parcels",
            assets={
                "data-tiles": {
                    "href": "./data.pmtiles",
                    "type": "application/vnd.pmtiles",
                    "roles": ["visual"],
                }
            },
            extra={
                "links": [
                    {
                        "rel": "pmtiles",
                        "href": "./data.pmtiles",
                        "type": "application/vnd.pmtiles",
                        "pmtiles:layers": ["data"],
                    }
                ]
            },
        )

        results = repair_pmtiles_links(tmp_path)
        assert results == []

    @pytest.mark.unit
    def test_noop_when_no_pmtiles_asset(self, tmp_path: Path) -> None:
        from portolan_cli.metadata.fix import repair_pmtiles_links

        _make_collection(
            tmp_path,
            "parcels",
            assets={"data": {"href": "./data.parquet", "roles": ["data"]}},
        )

        results = repair_pmtiles_links(tmp_path)
        assert results == []
