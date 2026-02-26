"""Validation rule base class and built-in rules.

Each rule checks one aspect of catalog validity. Rules are designed
to be unit-testable in isolation and composable into a validation pipeline.

Per ADR-0011, v0.4 rules only check catalog structure, not dataset contents.
"""

from __future__ import annotations

import json
from abc import ABC, abstractmethod
from pathlib import Path

from portolan_cli.validation.results import Severity, ValidationResult


class ValidationRule(ABC):
    """Base class for all validation rules.

    Subclasses must define:
        name: Unique identifier for the rule
        severity: ERROR (blocking) or WARNING (non-blocking)
        description: Human-readable explanation for --verbose

    Subclasses must implement:
        check(): Run the validation and return a result
    """

    name: str
    severity: Severity
    description: str

    @abstractmethod
    def check(self, catalog_path: Path) -> ValidationResult:
        """Run this validation rule against a catalog.

        Args:
            catalog_path: Path to the directory containing .portolan.

        Returns:
            ValidationResult indicating pass/fail with message.
        """
        ...

    def _pass(self, message: str) -> ValidationResult:
        """Helper to create a passing result."""
        return ValidationResult(
            rule_name=self.name,
            passed=True,
            severity=self.severity,
            message=message,
        )

    def _fail(self, message: str, *, fix_hint: str | None = None) -> ValidationResult:
        """Helper to create a failing result."""
        return ValidationResult(
            rule_name=self.name,
            passed=False,
            severity=self.severity,
            message=message,
            fix_hint=fix_hint,
        )


class CatalogExistsRule(ValidationRule):
    """Check that .portolan directory exists.

    This is the most fundamental check - without the catalog directory,
    no other validation can proceed.
    """

    name = "catalog_exists"
    severity = Severity.ERROR
    description = "Verify .portolan directory exists"

    def check(self, catalog_path: Path) -> ValidationResult:
        """Check for .portolan directory."""
        portolan_dir = catalog_path / ".portolan"

        if not portolan_dir.exists():
            return self._fail(
                f"Catalog not found: {portolan_dir} does not exist",
                fix_hint="Run 'portolan init' to create a catalog",
            )

        if not portolan_dir.is_dir():
            return self._fail(
                f"Invalid catalog: {portolan_dir} exists but is not a directory",
                fix_hint="Remove the file and run 'portolan init'",
            )

        return self._pass(f"Catalog directory exists: {portolan_dir}")


class CatalogJsonValidRule(ValidationRule):
    """Check that catalog.json exists and is valid JSON.

    This rule does NOT check STAC schema compliance - only that the
    file exists and can be parsed as JSON.

    Note: In v2 structure, catalog.json is at root level, not inside .portolan.
    """

    name = "catalog_json_valid"
    severity = Severity.ERROR
    description = "Verify catalog.json exists and is valid JSON"

    def check(self, catalog_path: Path) -> ValidationResult:
        """Check for valid catalog.json at root level."""
        catalog_file = catalog_path / "catalog.json"

        if not catalog_file.exists():
            return self._fail(
                f"Missing catalog.json: {catalog_file} does not exist",
                fix_hint="Run 'portolan init' to create a catalog, or restore from backup",
            )

        try:
            content = catalog_file.read_text()
            if not content.strip():
                return self._fail(
                    f"Empty catalog.json: {catalog_file} has no content",
                    fix_hint="Run 'portolan check --fix' to regenerate catalog.json",
                )
            json.loads(content)
        except json.JSONDecodeError as e:
            return self._fail(
                f"Invalid JSON in catalog.json: {e}",
                fix_hint="Fix the JSON syntax error or restore from backup",
            )
        except OSError as e:
            return self._fail(
                f"Cannot read catalog.json: {e}",
                fix_hint="Check file permissions",
            )

        return self._pass(f"catalog.json is valid JSON: {catalog_file}")


class StacFieldsRule(ValidationRule):
    """Check that catalog.json has required STAC Catalog fields.

    Required fields per STAC spec:
    - type: Must be "Catalog"
    - stac_version: STAC version string
    - id: Unique identifier
    - description: Human-readable description
    - links: Array of Link objects

    Note: In v2 structure, catalog.json is at root level, not inside .portolan.
    """

    name = "stac_fields"
    severity = Severity.ERROR
    description = "Verify catalog.json has required STAC Catalog fields"

    REQUIRED_FIELDS = ("type", "stac_version", "id", "description", "links")

    def check(self, catalog_path: Path) -> ValidationResult:
        """Check for required STAC fields in root catalog.json."""
        catalog_file = catalog_path / "catalog.json"

        # Try to load catalog.json
        try:
            content = catalog_file.read_text()
            catalog = json.loads(content)
        except (OSError, json.JSONDecodeError) as e:
            return self._fail(
                f"Cannot validate STAC fields: {e}",
                fix_hint="Fix catalog.json first (see catalog_json_valid rule)",
            )

        # Check type field specifically
        if "type" not in catalog:
            missing = [f for f in self.REQUIRED_FIELDS if f not in catalog]
            return self._fail(
                f"Missing required STAC fields: {', '.join(missing)}",
                fix_hint="Run 'portolan check --fix' to add default values",
            )

        if catalog.get("type") != "Catalog":
            return self._fail(
                f"Invalid type: expected 'Catalog', got '{catalog.get('type')}'",
                fix_hint="Change 'type' to 'Catalog' in catalog.json",
            )

        # Check other required fields
        missing = [f for f in self.REQUIRED_FIELDS if f not in catalog]
        if missing:
            return self._fail(
                f"Missing required STAC fields: {', '.join(missing)}",
                fix_hint="Run 'portolan check --fix' to add default values",
            )

        return self._pass("All required STAC fields present")


class PMTilesRecommendedRule(ValidationRule):
    """Recommend PMTiles for GeoParquet datasets without them.

    This is a WARNING-level rule - it doesn't block validation,
    just suggests an improvement for web display capabilities.

    PMTiles are generated from GeoParquet using the portolan-pmtiles
    plugin and provide efficient vector tile rendering for web maps.
    """

    name = "pmtiles_recommended"
    severity = Severity.WARNING
    description = "Check if GeoParquet datasets have PMTiles derivatives"

    def check(self, catalog_path: Path) -> ValidationResult:
        """Check for PMTiles derivatives alongside GeoParquet files.

        Args:
            catalog_path: Path to the directory containing .portolan.

        Returns:
            ValidationResult with warning if any GeoParquet lacks PMTiles.
        """
        datasets_dir = catalog_path / ".portolan" / "datasets"

        # Handle missing directories gracefully
        if not datasets_dir.exists():
            return self._pass("No datasets directory found")

        # Find all .parquet files in datasets
        parquet_files = list(datasets_dir.rglob("*.parquet"))

        if not parquet_files:
            return self._pass("No GeoParquet datasets found")

        # Check each parquet file for a corresponding .pmtiles file
        missing_pmtiles: list[str] = []
        for parquet_file in parquet_files:
            # PMTiles file should have same name but .pmtiles extension
            pmtiles_file = parquet_file.with_suffix(".pmtiles")
            if not pmtiles_file.exists():
                # Use relative path for cleaner messages
                rel_path = parquet_file.relative_to(datasets_dir)
                missing_pmtiles.append(str(rel_path))

        if missing_pmtiles:
            if len(missing_pmtiles) == 1:
                msg = f"GeoParquet dataset missing PMTiles: {missing_pmtiles[0]}"
            else:
                msg = f"{len(missing_pmtiles)} GeoParquet datasets missing PMTiles"

            return self._fail(
                msg,
                fix_hint=(
                    "Install portolan-pmtiles plugin and run "
                    "'portolan dataset add --pmtiles' to generate vector tiles"
                ),
            )

        return self._pass(f"All {len(parquet_files)} GeoParquet datasets have PMTiles derivatives")


class MetadataFreshRule(ValidationRule):
    """Check that all geo-asset files have fresh STAC metadata.

    This rule scans for GeoParquet and COG files in collections
    and verifies their STAC item metadata is up-to-date using
    MTIME + heuristic change detection.

    Reports:
    - MISSING: Files without any STAC metadata (ERROR)
    - STALE: Files where content has changed since last metadata generation (WARNING)
    - BREAKING: Files with breaking schema changes (ERROR)
    """

    name = "metadata_fresh"
    severity = Severity.WARNING  # Default to WARNING, but MISSING/BREAKING are ERROR
    description = "Verify all geo-assets have fresh STAC metadata"

    def check(self, catalog_path: Path) -> ValidationResult:
        """Check metadata freshness for all geo-assets in catalog.

        Args:
            catalog_path: Path to the directory containing .portolan.

        Returns:
            ValidationResult indicating overall metadata health.
        """
        from portolan_cli.metadata.detection import check_file_metadata
        from portolan_cli.metadata.models import MetadataCheckResult, MetadataReport

        # Find all collections in the catalog (at root level per ADR-0023)
        catalog_json = catalog_path / "catalog.json"
        if not catalog_json.exists():
            return self._pass("No catalog.json found")

        # Scan for geo-asset files in collections
        check_results: list[MetadataCheckResult] = []
        extensions = {".parquet", ".tif", ".tiff"}

        # Collections are at root level, identified by collection.json
        for collection_dir in catalog_path.iterdir():
            if not collection_dir.is_dir():
                continue
            # Skip .portolan and hidden directories
            if collection_dir.name.startswith("."):
                continue
            # Only process directories with collection.json
            if not (collection_dir / "collection.json").exists():
                continue
            # Find geo-asset files in this collection
            for file_path in collection_dir.rglob("*"):
                if file_path.suffix.lower() in extensions:
                    try:
                        result = check_file_metadata(file_path, collection_dir)
                        check_results.append(result)
                    except (FileNotFoundError, ValueError, OSError):
                        # Skip files we can't check:
                        # - FileNotFoundError: broken symlinks
                        # - ValueError: unsupported format
                        # - OSError: corrupt COGs (rasterio errors inherit from OSError)
                        continue
                    except Exception as e:
                        # Also catch pyarrow errors (ArrowInvalid, ArrowIOError, etc.)
                        # which don't have a consistent base class
                        if "arrow" in type(e).__module__.lower():
                            continue
                        raise  # Re-raise unexpected errors

        if not check_results:
            return self._pass("No geo-asset files found in collections")

        # Build summary report
        report = MetadataReport(results=check_results)

        if report.passed:
            return self._pass(f"All {report.total_count} geo-assets have fresh metadata")

        # Build detailed message about issues
        issues = []
        if report.missing_count > 0:
            issues.append(f"{report.missing_count} missing")
        if report.stale_count > 0:
            issues.append(f"{report.stale_count} stale")
        if report.breaking_count > 0:
            issues.append(f"{report.breaking_count} breaking")

        message = f"Metadata issues found: {', '.join(issues)}"

        # Determine severity based on issue types
        # MISSING and BREAKING are errors, STALE is warning
        has_errors = report.missing_count > 0 or report.breaking_count > 0

        return ValidationResult(
            rule_name=self.name,
            passed=False,
            severity=Severity.ERROR if has_errors else Severity.WARNING,
            message=message,
            fix_hint="Run 'portolan check --fix-metadata' to update STAC metadata",
        )
