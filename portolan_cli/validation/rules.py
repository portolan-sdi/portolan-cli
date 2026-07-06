"""Validation rule base class and built-in rules.

Each rule checks one aspect of catalog validity. Rules are designed
to be unit-testable in isolation and composable into a validation pipeline.

Per ADR-0011, v0.4 rules only check catalog structure, not data contents.
"""

from __future__ import annotations

import json
from abc import ABC, abstractmethod
from pathlib import Path, PurePath
from typing import Any

from portolan_cli.scan_classify import (
    GEO_ASSET_EXTENSIONS,
    TABULAR_EXTENSIONS,
    is_geoparquet,
)
from portolan_cli.validation.results import Severity, ValidationResult

# Substring identifying the STAC Table extension in stac_extensions URLs.
_TABLE_EXTENSION_MARKER = "stac-extensions.github.io/table"


def _resolve_local_href(collection_dir: Path, href: str) -> Path | None:
    """Resolve a STAC asset ``href`` to a local filesystem path.

    Returns ``None`` for remote hrefs (any URL scheme such as ``http://`` or
    ``s3://``), which cannot be inspected on the local filesystem. Relative
    hrefs are resolved against ``collection_dir``; a leading ``./`` is stripped
    first so it does not defeat the join.
    """
    if "://" in href:
        return None
    rel = href[2:] if href.startswith("./") else href
    return collection_dir / rel


def classify_collection_data(collection_dir: Path, data: dict[str, Any]) -> str:
    """Classify a collection's data assets as ``geo``, ``tabular``, or ``empty``.

    Inspects the registered ``assets`` by extension (and, for ``.parquet``, by
    peeking at GeoParquet metadata via :func:`is_geoparquet`). A collection with
    any geospatial asset is ``"geo"``; otherwise one with any tabular asset is
    ``"tabular"``; otherwise ``"empty"``. Classifying by actual content means a
    raster/COG-only collection is correctly ``"geo"`` and never mislabeled.

    A ``.parquet`` asset is only counted when its file is present locally and
    readable: ``is_geoparquet`` cannot distinguish "plain Parquet" from "file
    missing / remote / unreadable" (both yield ``False``). Treating an
    unresolvable Parquet as tabular would raise a false RULE-0090 ERROR — and,
    worse, drive ``--fix`` to stamp ``portolan:geospatial: false`` onto an
    actually-spatial collection (e.g. one checked before its data is pulled, or
    one whose asset href is remote). So such assets are left unclassified.

    Args:
        collection_dir: Directory containing the collection.json (for href resolution).
        data: Parsed collection.json contents.

    Returns:
        One of ``"geo"``, ``"tabular"``, or ``"empty"``.
    """
    assets = data.get("assets", {})
    has_geo = False
    has_tabular = False

    for asset in assets.values():
        if not isinstance(asset, dict):
            continue
        href = asset.get("href", "")
        if not href:
            continue
        # Skip STAC-GeoParquet rollups (items.parquet); they are metadata, not data.
        roles = asset.get("roles", [])
        if "stac-items" in roles:
            continue
        ext = PurePath(href).suffix.lower()
        if ext == ".parquet":
            local = _resolve_local_href(collection_dir, href)
            if local is None or not local.is_file():
                # Remote or not-yet-local Parquet: cannot tell geo from plain,
                # so do not classify it either way.
                continue
            if is_geoparquet(local):
                has_geo = True
            else:
                has_tabular = True
        elif ext in GEO_ASSET_EXTENSIONS:
            has_geo = True
        elif ext in TABULAR_EXTENSIONS:
            has_tabular = True

    if has_geo:
        return "geo"
    if has_tabular:
        return "tabular"
    return "empty"


def _is_tabular_collection(collection_dir: Path, data: dict[str, Any]) -> bool:
    """Return True if a collection is non-spatial (tabular).

    A collection counts as tabular when it is either explicitly flagged
    ``portolan:geospatial: false`` or detected as tabular by asset content
    (:func:`classify_collection_data`). Keying off *both* signals means a single
    ``check`` pass surfaces the schema / temporal / layout recommendations
    (RULE-0091/0093/0094) on a tabular collection even before its
    ``portolan:geospatial`` flag is backfilled (RULE-0090) — instead of hiding
    them until a second pass.
    """
    if data.get("portolan:geospatial") is False:
        return True
    return classify_collection_data(collection_dir, data) == "tabular"


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
    """Recommend PMTiles for GeoParquet collection assets without them.

    This is a WARNING-level rule - it doesn't block validation,
    just suggests an improvement for web display capabilities.

    PMTiles are generated from GeoParquet using gpio-pmtiles and provide
    efficient vector tile rendering for web maps. Per ADR-0031, vector
    data is stored as collection-level assets.
    """

    name = "pmtiles_recommended"
    severity = Severity.WARNING
    description = "Check if GeoParquet collections have PMTiles derivatives"

    def check(self, catalog_path: Path) -> ValidationResult:
        """Check for PMTiles derivatives alongside GeoParquet collection assets.

        Scans all collection.json files for GeoParquet assets and checks
        if sibling PMTiles files exist.

        Args:
            catalog_path: Path to the directory containing .portolan.

        Returns:
            ValidationResult with warning if any GeoParquet lacks PMTiles.
        """
        # Find all collection.json files
        collection_files = list(catalog_path.rglob("collection.json"))

        if not collection_files:
            return self._pass("No collections found")

        missing_pmtiles: list[str] = []
        total_geoparquet = 0

        for collection_json in collection_files:
            collection_dir = collection_json.parent

            try:
                data = json.loads(collection_json.read_text())
            except (json.JSONDecodeError, OSError):
                continue

            assets = data.get("assets", {})

            for _key, asset in assets.items():
                href = asset.get("href", "")
                media_type = asset.get("type", "")
                roles = asset.get("roles", [])

                # Skip stac-items parquet (metadata, not geodata)
                if "stac-items" in roles:
                    continue

                # Check if it's a GeoParquet asset
                is_geoparquet = (
                    media_type == "application/vnd.apache.parquet"
                    or media_type == "application/x-parquet"
                    or href.endswith(".parquet")
                )

                if not is_geoparquet:
                    continue

                total_geoparquet += 1

                # Resolve href to path
                if href.startswith("./"):
                    href = href[2:]
                parquet_path = collection_dir / href

                if not parquet_path.exists():
                    continue

                # Check for sibling PMTiles (both file existence AND asset registration)
                pmtiles_path = parquet_path.with_suffix(".pmtiles")
                pmtiles_filename = pmtiles_path.name

                # Check if PMTiles is registered in collection assets
                pmtiles_registered = any(
                    asset.get("href", "").endswith(pmtiles_filename) for asset in assets.values()
                )

                if not pmtiles_path.exists() or not pmtiles_registered:
                    try:
                        rel_path = parquet_path.relative_to(catalog_path)
                    except ValueError:
                        rel_path = parquet_path
                    missing_pmtiles.append(str(rel_path))

        if total_geoparquet == 0:
            return self._pass("No GeoParquet collection assets found")

        if missing_pmtiles:
            if len(missing_pmtiles) == 1:
                msg = f"GeoParquet missing PMTiles: {missing_pmtiles[0]}"
            else:
                msg = f"{len(missing_pmtiles)} GeoParquet assets missing PMTiles"

            return self._fail(
                msg,
                fix_hint="Run 'portolan add --pmtiles' to generate vector tiles",
            )

        return self._pass(f"All {total_geoparquet} GeoParquet assets have PMTiles derivatives")


def _pmtiles_link_failure_message(missing_link: list[str]) -> str:
    """Build the RULE-0061 failure message for PMTiles assets lacking a link."""
    if len(missing_link) == 1:
        return f"PMTiles asset missing rel='pmtiles' link: {missing_link[0]}"
    return f"{len(missing_link)} PMTiles assets missing rel='pmtiles' link"


def _pmtiles_extension_failure_message(missing_extension: list[str]) -> str:
    """Build the RULE-0061 failure message for an undeclared web-map-links extension."""
    if len(missing_extension) == 1:
        return (
            "Collection with PMTiles asset missing web-map-links "
            f"extension declaration: {missing_extension[0]}"
        )
    return (
        f"{len(missing_extension)} collections with PMTiles assets missing "
        "web-map-links extension declaration"
    )


class PMTilesLinkRule(ValidationRule):
    """Require a ``rel="pmtiles"`` link when a collection has a PMTiles asset.

    Implements RULE-0061: a collection that exposes a PMTiles visualization asset
    MUST also emit a collection-level ``rel="pmtiles"`` link so the derivative is
    discoverable via the web-map-links STAC extension, not only as an asset. This
    is an ERROR-level rule; ``check --fix`` backfills the missing link.
    """

    name = "pmtiles_link"
    severity = Severity.ERROR
    description = "Check collections with PMTiles assets emit a rel='pmtiles' link"

    def check(self, catalog_path: Path) -> ValidationResult:
        """Fail when any collection has a PMTiles asset but no rel='pmtiles' link.

        Args:
            catalog_path: Path to the directory containing .portolan.

        Returns:
            ValidationResult, failing if any PMTiles asset lacks its link.
        """
        collection_files = list(catalog_path.rglob("collection.json"))
        if not collection_files:
            return self._pass("No collections found")

        # Track individual PMTiles assets, not just collections, so the check
        # stays consistent with the per-href repair in repair_pmtiles_links:
        # a collection with two PMTiles assets but only one rel='pmtiles' link
        # must fail (--fix would add the second link).
        missing_link: list[str] = []
        # Collections with a PMTiles asset that do not declare the web-map-links
        # extension in stac_extensions (RULE-0061 assertion 3). --fix backfills it.
        missing_extension: list[str] = []
        total_pmtiles_assets = 0

        for collection_json in collection_files:
            scanned = self._scan_collection(collection_json, catalog_path)
            if scanned is None:
                continue
            count, links_missing, ext_missing = scanned
            total_pmtiles_assets += count
            missing_link.extend(links_missing)
            if ext_missing is not None:
                missing_extension.append(ext_missing)

        if total_pmtiles_assets == 0:
            return self._pass("No PMTiles assets found")

        if missing_link:
            return self._fail(
                _pmtiles_link_failure_message(missing_link),
                fix_hint="Run 'portolan check --fix' to add the web-map-links pmtiles link",
            )

        if missing_extension:
            return self._fail(
                _pmtiles_extension_failure_message(missing_extension),
                fix_hint="Run 'portolan check --fix' to declare the web-map-links extension",
            )

        return self._pass(f"All {total_pmtiles_assets} PMTiles assets have rel='pmtiles' links")

    @staticmethod
    def _scan_collection(
        collection_json: Path, catalog_path: Path
    ) -> tuple[int, list[str], str | None] | None:
        """Scan one collection for PMTiles link/extension gaps.

        Returns ``(asset_count, missing_link_labels, missing_extension_label)``,
        or ``None`` when the collection is hidden, unreadable, or has no PMTiles
        asset. Hidden dirs (e.g. ``.portolan/``) are skipped so the check never
        reports an asset that repair_pmtiles_links deliberately skips — that
        would be a blocking ERROR --fix could never resolve.
        """
        # Import from the framework-free leaf, not pmtiles.py: the latter pulls
        # in output/thumbnail/style (and click/rich/config), which would break
        # the reis extraction seam (issue #563).
        from portolan_cli.pmtiles_links import (
            WEB_MAP_LINKS_EXTENSION,
            pmtiles_asset_hrefs,
            pmtiles_link_hrefs,
        )

        try:
            rel_dir = collection_json.parent.relative_to(catalog_path)
        except ValueError:
            rel_dir = collection_json.parent
        if any(part.startswith(".") for part in rel_dir.parts):
            return None

        try:
            data = json.loads(collection_json.read_text())
        except (json.JSONDecodeError, OSError):
            return None

        pmtiles_hrefs = pmtiles_asset_hrefs(data.get("assets", {}))
        if not pmtiles_hrefs:
            return None

        dir_label = str(rel_dir) if rel_dir.parts else "."
        linked_hrefs = pmtiles_link_hrefs(data.get("links", []))
        missing_links = [
            f"{dir_label}:{href}" for href in pmtiles_hrefs if href not in linked_hrefs
        ]
        # A collection exposing PMTiles MUST declare the web-map-links extension
        # (RULE-0061). --fix declares it, so flagging it here stays fixable.
        ext_missing = (
            dir_label if WEB_MAP_LINKS_EXTENSION not in data.get("stac_extensions", []) else None
        )
        return len(pmtiles_hrefs), missing_links, ext_missing


class MetadataFreshRule(ValidationRule):
    """Check that all registered geo-assets have fresh STAC metadata.

    Delegates to `scan_catalog_metadata` (ADR-0041) so that `check` and
    `check --fix` consume the same MetadataReport. The scanner walks the
    STAC manifest tree (catalog.json -> collection.json -> item.json),
    avoiding the false-MISSING reports that filesystem-walk approaches
    produced for collection-level rollup assets like items.parquet.

    Reports:
    - MISSING: Item directory has data but no item.json (ERROR, auto-fixable).
    - STALE: File changed since last metadata generation (WARNING).
    - BREAKING: Schema-breaking change (ERROR).
    - ORPHANED: File on disk but not registered in any STAC manifest
      (WARNING, not auto-fixable — user must register or delete).
    """

    name = "metadata_fresh"
    severity = Severity.WARNING
    description = "Verify all geo-assets have fresh STAC metadata"

    def check(self, catalog_path: Path) -> ValidationResult:
        """Check metadata freshness for all registered geo-assets.

        Args:
            catalog_path: Path to the directory containing .portolan.

        Returns:
            ValidationResult indicating overall metadata health.
        """
        from portolan_cli.metadata.scan import scan_catalog_metadata

        if not (catalog_path / "catalog.json").exists():
            return self._pass("No catalog.json found")

        report = scan_catalog_metadata(catalog_path)

        if not report.results:
            return self._pass("No geo-asset files found in collections")

        if report.passed:
            return self._pass(f"All {report.total_count} geo-assets have fresh metadata")

        issues = []
        if report.missing_count > 0:
            issues.append(f"{report.missing_count} missing")
        if report.stale_count > 0:
            issues.append(f"{report.stale_count} stale")
        if report.breaking_count > 0:
            issues.append(f"{report.breaking_count} breaking")
        if report.orphaned_count > 0:
            issues.append(f"{report.orphaned_count} orphaned")

        message = f"Metadata issues found: {', '.join(issues)}"
        has_errors = report.missing_count > 0 or report.breaking_count > 0
        fix_hint = (
            "Run 'portolan check --metadata --fix' to update STAC metadata"
            if has_errors or report.stale_count > 0
            else "Register orphan files in collection.json/item.json or delete them"
        )

        return ValidationResult(
            rule_name=self.name,
            passed=False,
            severity=Severity.ERROR if has_errors else Severity.WARNING,
            message=message,
            fix_hint=fix_hint,
        )


class ProvisionalDatetimeRule(ValidationRule):
    """Check for items with provisional (unknown) datetime.

    Per ADR-0035, items added without --datetime have null temporal extent
    and are marked with portolan:datetime_provisional=true. This rule warns
    about such items so users can enrich the metadata later.

    This is a WARNING, not an error - items are still valid STAC, just
    missing explicit temporal metadata.
    """

    name = "provisional_datetime"
    severity = Severity.WARNING
    description = "Check for items missing explicit datetime"

    def check(self, catalog_path: Path) -> ValidationResult:
        """Find items with provisional datetime marker.

        Args:
            catalog_path: Path to the directory containing .portolan.

        Returns:
            ValidationResult with list of provisional items.
        """
        # Find all item JSON files in collections
        catalog_json = catalog_path / "catalog.json"
        if not catalog_json.exists():
            return self._pass("No catalog.json found")

        provisional_items: list[str] = []

        # Collections can be nested (per ADR-0032), find all collection.json files
        for collection_json in catalog_path.rglob("collection.json"):
            collection_dir = collection_json.parent

            # Skip hidden directories
            if any(part.startswith(".") for part in collection_dir.parts):
                continue

            # Compute collection_id as relative path from catalog root
            # e.g., catalog_path/environment/air-quality/collection.json -> "environment/air-quality"
            collection_id = str(collection_dir.relative_to(catalog_path)).replace("\\", "/")

            # Find item JSON files (not collection.json, not versions.json, not catalog.json)
            for item_json in collection_dir.rglob("*.json"):
                if item_json.name in ("collection.json", "versions.json", "catalog.json"):
                    continue

                try:
                    data = json.loads(item_json.read_text(encoding="utf-8"))
                    properties = data.get("properties", {})
                    if properties.get("portolan:datetime_provisional"):
                        item_id = data.get("id", item_json.stem)
                        provisional_items.append(f"{collection_id}/{item_id}")
                except (json.JSONDecodeError, OSError):
                    # Skip files we can't read
                    continue

        if not provisional_items:
            return self._pass("All items have explicit datetime")

        # Build message with list of provisional items
        item_list = ", ".join(provisional_items[:5])  # Show first 5
        if len(provisional_items) > 5:
            item_list += f" (+{len(provisional_items) - 5} more)"

        return self._fail(
            f"{len(provisional_items)} item(s) have provisional datetime: {item_list}",
            fix_hint="Use 'portolan add --datetime YYYY-MM-DD' to set explicit datetime",
        )


# --- Thorough rules (expensive, run with --thorough) ---


class PartitionStructureRule(ValidationRule):
    """Check that partitioned collections have consistent Hive-style structure.

    Validates:
    - All partition directories follow same key=value pattern
    - No orphan files outside partition structure
    - partition:* extension fields are present when partitions detected
    """

    name = "partition_structure"
    severity = Severity.WARNING
    description = "Verify partition directory structure consistency"

    def check(self, catalog_path: Path) -> ValidationResult:
        """Check partition structure for all collections."""
        issues: list[str] = []

        # Find all collection.json files
        for coll_json in catalog_path.rglob("collection.json"):
            coll_dir = coll_json.parent

            # Look for Hive-style partition directories (key=value pattern)
            partition_dirs = [
                d
                for d in coll_dir.iterdir()
                if d.is_dir() and "=" in d.name and not d.name.startswith(".")
            ]

            if not partition_dirs:
                continue

            # Extract partition key from first directory
            first_key = partition_dirs[0].name.split("=")[0]

            # Check all partition dirs use same key
            for pdir in partition_dirs:
                if "=" not in pdir.name:
                    issues.append(f"{coll_dir.name}: orphan dir '{pdir.name}'")
                    continue
                key = pdir.name.split("=")[0]
                if key != first_key:
                    issues.append(f"{coll_dir.name}: mixed keys '{first_key}' and '{key}'")

            # Check for orphan parquet files at collection level
            orphan_files = list(coll_dir.glob("*.parquet"))
            if orphan_files and partition_dirs:
                issues.append(f"{coll_dir.name}: {len(orphan_files)} orphan .parquet at root")

            # Check collection.json has partition:* fields
            # STAC Collections store extension fields at top level, not in "properties"
            try:
                with open(coll_json) as f:
                    coll_data = json.load(f)
                if "partition:scheme" not in coll_data:
                    issues.append(f"{coll_dir.name}: missing partition:scheme in collection.json")
            except (json.JSONDecodeError, OSError):
                pass

        if not issues:
            return self._pass("All partitioned collections have consistent structure")

        issue_list = "; ".join(issues[:5])
        if len(issues) > 5:
            issue_list += f" (+{len(issues) - 5} more)"

        return self._fail(
            f"Partition structure issues: {issue_list}",
            fix_hint="Ensure all partition directories use same key pattern",
        )


class PartitionSchemaConsistencyRule(ValidationRule):
    """Check that all parquet files in a partition have consistent schema.

    Reads parquet metadata (footer only) from all partition files to verify
    they share the same schema.
    """

    name = "partition_schema_consistency"
    severity = Severity.ERROR
    description = "Verify all partition files have same Parquet schema"

    def check(self, catalog_path: Path) -> ValidationResult:
        """Check schema consistency across partition files."""
        try:
            import pyarrow.parquet as pq
            from pyarrow import ArrowInvalid
        except ImportError:
            return ValidationResult(
                rule_name=self.name,
                passed=True,
                severity=Severity.WARNING,
                message="PyArrow not available, schema consistency not checked",
            )

        issues: list[str] = []

        for coll_json in catalog_path.rglob("collection.json"):
            coll_dir = coll_json.parent

            # Find Hive-style partition directories
            partition_dirs = [
                d
                for d in coll_dir.iterdir()
                if d.is_dir() and "=" in d.name and not d.name.startswith(".")
            ]

            if not partition_dirs:
                continue

            # Collect schemas from all parquet files
            schemas: dict[str, list[str]] = {}
            for pdir in partition_dirs:
                for pq_file in pdir.glob("*.parquet"):
                    try:
                        schema = pq.read_schema(pq_file)
                        schema_key = str(sorted(schema.names))
                        if schema_key not in schemas:
                            schemas[schema_key] = []
                        schemas[schema_key].append(str(pq_file.relative_to(coll_dir)))
                    except (OSError, ArrowInvalid) as e:
                        issues.append(f"{coll_dir.name}: unreadable {pq_file.name} ({e})")

            if len(schemas) > 1:
                issues.append(
                    f"{coll_dir.name}: {len(schemas)} different schemas across partitions"
                )

        if not issues:
            return self._pass("All partition files have consistent schema")

        issue_list = "; ".join(issues[:3])
        if len(issues) > 3:
            issue_list += f" (+{len(issues) - 3} more)"

        return self._fail(
            f"Schema inconsistency: {issue_list}",
            fix_hint="Re-partition with consistent source data",
        )


class BboxValidRule(ValidationRule):
    """Check that all collection and item bboxes are valid (issue #516).

    Validates that bboxes:
    - Do not contain inf or nan values
    - Have coordinates within WGS84 bounds (lon: -180 to 180, lat: -90 to 90)
    - Have south <= north

    This catches garbage coordinates that can poison catalog-level extent unions
    and break map UI browsing (discovered in IGN Argentina with sentinel -1.79e308 values).
    """

    name = "bbox_valid"
    severity = Severity.ERROR
    description = "Check for invalid bbox coordinates (inf/nan/out of range)"

    def check(self, catalog_path: Path) -> ValidationResult:
        """Find collections and items with invalid bboxes."""
        catalog_json = catalog_path / "catalog.json"
        if not catalog_json.exists():
            return self._pass("No catalog.json found")

        invalid = self._check_catalog_bbox(catalog_json)
        invalid.extend(self._check_collections(catalog_path))

        if not invalid:
            return self._pass("All bboxes are valid")

        summary = ", ".join(invalid[:5])
        if len(invalid) > 5:
            summary += f" (+{len(invalid) - 5} more)"

        return self._fail(
            f"{len(invalid)} invalid bbox(es): {summary}",
            fix_hint="Re-add the affected collections to regenerate with valid source data",
        )

    def _check_catalog_bbox(self, catalog_json: Path) -> list[str]:
        """Check catalog-level extent if present (all bbox entries)."""
        from portolan_cli.bbox import get_bbox_validation_reason

        invalid: list[str] = []
        try:
            data = json.loads(catalog_json.read_text(encoding="utf-8"))
            bbox_list = data.get("extent", {}).get("spatial", {}).get("bbox", [])
            for i, bbox in enumerate(bbox_list):
                if not bbox:
                    continue
                reason = get_bbox_validation_reason(bbox)
                if reason:
                    label = f"catalog bbox[{i}]" if len(bbox_list) > 1 else "catalog"
                    invalid.append(f"{label}: {reason}")
        except (json.JSONDecodeError, OSError):
            pass
        return invalid

    def _check_collections(self, catalog_path: Path) -> list[str]:
        """Check all collection and item bboxes."""

        invalid: list[str] = []

        for collection_json in catalog_path.rglob("collection.json"):
            collection_dir = collection_json.parent
            if any(part.startswith(".") for part in collection_dir.parts):
                continue

            coll_id = str(collection_dir.relative_to(catalog_path)).replace("\\", "/")
            invalid.extend(self._check_collection_file(collection_json, coll_id))
            invalid.extend(self._check_items(collection_dir, coll_id))

        return invalid

    def _check_collection_file(self, path: Path, coll_id: str) -> list[str]:
        """Check a single collection.json for invalid bboxes."""
        from portolan_cli.bbox import get_bbox_validation_reason

        invalid: list[str] = []
        try:
            data = json.loads(path.read_text(encoding="utf-8"))
            bbox_list = data.get("extent", {}).get("spatial", {}).get("bbox", [])
            for i, bbox in enumerate(bbox_list):
                if not bbox:
                    continue
                reason = get_bbox_validation_reason(bbox)
                if reason:
                    label = f"{coll_id} bbox[{i}]" if len(bbox_list) > 1 else coll_id
                    invalid.append(f"{label}: {reason}")
        except (json.JSONDecodeError, OSError):
            pass
        return invalid

    def _check_items(self, collection_dir: Path, coll_id: str) -> list[str]:
        """Check item bboxes in a collection."""
        from portolan_cli.bbox import get_bbox_validation_reason

        invalid: list[str] = []
        skip_names = {"collection.json", "versions.json", "catalog.json"}

        for item_json in collection_dir.rglob("*.json"):
            if item_json.name in skip_names:
                continue
            try:
                data = json.loads(item_json.read_text(encoding="utf-8"))
                if data.get("type") != "Feature":
                    continue
                bbox = data.get("bbox")
                if bbox:
                    reason = get_bbox_validation_reason(bbox)
                    if reason:
                        item_id = data.get("id", item_json.stem)
                        invalid.append(f"{coll_id}/{item_id}: {reason}")
            except (json.JSONDecodeError, OSError):
                continue
        return invalid


# --- Tabular collection rules (ADR-0047, RULE-0090 through RULE-0094) ---


def _iter_collections(catalog_path: Path) -> list[tuple[Path, dict[str, Any]]]:
    """Yield (collection_dir, parsed-json) for every non-hidden collection.json.

    Shared by the tabular rules below. Unparsable files are skipped silently,
    matching the resilience of the other rules in this module.
    """
    found: list[tuple[Path, dict[str, Any]]] = []
    for collection_json in catalog_path.rglob("collection.json"):
        collection_dir = collection_json.parent
        # Filter hidden dirs relative to the catalog root, not by absolute path
        # parts — otherwise a catalog living under a dotted directory (e.g.
        # ~/.local/share/catalog) would skip every collection.
        rel_parts = collection_dir.relative_to(catalog_path).parts
        if any(part.startswith(".") for part in rel_parts):
            continue
        try:
            data = json.loads(collection_json.read_text(encoding="utf-8"))
        except (json.JSONDecodeError, OSError):
            continue
        found.append((collection_dir, data))
    return found


def _summarize(issues: list[str]) -> str:
    """Render an issue list as a capped preview (first 5, then `(+N more)`)."""
    preview = "; ".join(issues[:5])
    if len(issues) > 5:
        preview += f" (+{len(issues) - 5} more)"
    return preview


class TabularGeospatialFlagRule(ValidationRule):
    """RULE-0090: tabular collections MUST set ``portolan:geospatial: false``.

    The flag distinguishes intentionally non-spatial collections from spatial
    collections with a missing extent, so federation agents can route queries.
    Fires for collections whose assets are tabular (CSV/XLSX or plain Parquet)
    when the flag is not explicitly ``false``.
    """

    name = "tabular_geospatial_flag"
    severity = Severity.ERROR
    description = "Verify tabular collections set portolan:geospatial: false"

    def check(self, catalog_path: Path) -> ValidationResult:
        issues: list[str] = []
        for collection_dir, data in _iter_collections(catalog_path):
            if classify_collection_data(collection_dir, data) != "tabular":
                continue
            if data.get("portolan:geospatial") is not False:
                issues.append(f"{collection_dir.name}: missing portolan:geospatial: false")

        if issues:
            return self._fail(
                f"{len(issues)} tabular collection(s) not marked non-spatial: {_summarize(issues)}",
                fix_hint="Run 'portolan check --fix' to add portolan:geospatial: false",
            )
        return self._pass("All tabular collections are marked non-spatial")


class TabularTableExtensionRule(ValidationRule):
    """RULE-0091: tabular collections SHOULD use the STAC Table extension.

    With no geometry to hint at meaning, the schema is the primary semantic
    handle for consumers. Warns when a non-spatial collection lacks
    ``table:columns`` or does not declare the Table extension.
    """

    name = "tabular_table_extension"
    severity = Severity.WARNING
    description = "Recommend STAC Table extension for tabular collections"

    def check(self, catalog_path: Path) -> ValidationResult:
        issues: list[str] = []
        for collection_dir, data in _iter_collections(catalog_path):
            if not _is_tabular_collection(collection_dir, data):
                continue
            has_columns = bool(data.get("table:columns"))
            extensions = data.get("stac_extensions", [])
            has_extension = any(_TABLE_EXTENSION_MARKER in str(ext) for ext in extensions)
            if not has_columns or not has_extension:
                issues.append(f"{collection_dir.name}: no table:columns / Table extension")

        if issues:
            return self._fail(
                f"{len(issues)} tabular collection(s) missing Table extension: "
                f"{_summarize(issues)}",
                fix_hint="Add the STAC Table extension and table:columns to the collection",
            )
        return self._pass("All tabular collections document their schema")


class TabularTemporalExtentRule(ValidationRule):
    """RULE-0093: tabular collections SHOULD have a temporal extent.

    Most tabular data is time-dimensioned (time-series, reporting periods), and
    a temporal extent enables time-based queries. Warns when a non-spatial
    collection omits ``extent.temporal`` entirely (a present-but-null interval
    is the intentional open-interval default of ADR-0035 and is not flagged).
    """

    name = "tabular_temporal_extent"
    severity = Severity.WARNING
    description = "Recommend temporal extent for tabular collections"

    def check(self, catalog_path: Path) -> ValidationResult:
        issues: list[str] = []
        for collection_dir, data in _iter_collections(catalog_path):
            if not _is_tabular_collection(collection_dir, data):
                continue
            extent = data.get("extent", {})
            if not isinstance(extent, dict) or extent.get("temporal") is None:
                issues.append(f"{collection_dir.name}: no extent.temporal")

        if issues:
            return self._fail(
                f"{len(issues)} tabular collection(s) missing temporal extent: "
                f"{_summarize(issues)}",
                fix_hint="Set extent.temporal in metadata.yaml when the data is time-dimensioned",
            )
        return self._pass("All tabular collections declare a temporal extent")


class TabularCollectionLevelAssetsRule(ValidationRule):
    """RULE-0094: tabular collections MUST use collection-level assets.

    Single-file tabular data should live in ``collection.assets``, not be
    wrapped in STAC items. Fires for a non-spatial collection that contains
    ``item.json`` files. Partitioned collections (``partition:scheme`` present)
    are exempt — ADR-0047 lets Hive-partitioned tabular data use items.
    """

    name = "tabular_collection_level_assets"
    severity = Severity.ERROR
    description = "Verify tabular collections use collection-level assets, not items"

    def check(self, catalog_path: Path) -> ValidationResult:
        issues: list[str] = []
        for collection_dir, data in _iter_collections(catalog_path):
            if not _is_tabular_collection(collection_dir, data):
                continue
            if data.get("partition:scheme"):
                continue
            item_files = [
                p
                for p in collection_dir.rglob("item.json")
                if not any(part.startswith(".") for part in p.relative_to(collection_dir).parts)
            ]
            if item_files:
                issues.append(f"{collection_dir.name}: data wrapped in {len(item_files)} item(s)")

        if issues:
            return self._fail(
                f"{len(issues)} tabular collection(s) wrap data in items: {_summarize(issues)}",
                fix_hint="Store single-file tabular data as collection-level assets (ADR-0031)",
            )
        return self._pass("All tabular collections use collection-level assets")
