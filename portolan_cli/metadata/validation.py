"""Metadata validation functions.

Provides validation for STAC catalog structures:
- check_directory_metadata(): Scan a directory tree for metadata issues
- validate_collection_extent(): Verify collection bbox contains all item bboxes
- validate_catalog_links(): Verify catalog links point to existing files
"""

from __future__ import annotations

import json
from dataclasses import dataclass, field
from pathlib import Path

from portolan_cli.metadata.models import MetadataCheckResult, MetadataReport, MetadataStatus
from portolan_cli.models.catalog import CatalogModel
from portolan_cli.models.collection import CollectionModel
from portolan_cli.models.item import ItemModel


@dataclass
class ValidationMessage:
    """A validation message (error or warning).

    Attributes:
        message: Human-readable description.
        path: Optional path to the problematic file/link.
    """

    message: str
    path: str | None = None


@dataclass
class ValidationResult:
    """Result of a validation check.

    Attributes:
        passed: Whether validation passed without errors.
        errors: List of error messages (causes failure).
        warnings: List of warning messages (informational, doesn't fail).
    """

    passed: bool = True
    errors: list[ValidationMessage] = field(default_factory=list)
    warnings: list[ValidationMessage] = field(default_factory=list)


def validate_collection_extent(
    collection: CollectionModel,
    items: list[ItemModel],
) -> ValidationResult:
    """Validate that collection extent contains all item bboxes.

    The collection's spatial extent should be a union of all item bboxes.
    Each item's bbox must be fully contained within the collection extent.

    Args:
        collection: The collection to validate.
        items: List of items belonging to this collection.

    Returns:
        ValidationResult with any items outside the collection extent.
    """
    result = ValidationResult()

    # Empty items is valid
    if not items:
        return result

    # Get collection bbox (first bbox in list)
    if not collection.extent.spatial.bbox:
        result.passed = False
        result.errors.append(
            ValidationMessage(
                message="Collection has no spatial extent defined",
            )
        )
        return result

    coll_bbox = collection.extent.spatial.bbox[0]
    coll_west, coll_south, coll_east, coll_north = coll_bbox[:4]

    # Check each item's bbox is within collection extent
    for item in items:
        if item.bbox is None:
            result.warnings.append(
                ValidationMessage(
                    message=f"Item '{item.id}' has no bbox",
                )
            )
            continue

        item_west, item_south, item_east, item_north = item.bbox[:4]

        # Check if item bbox is fully contained in collection bbox
        outside = False
        if item_west < coll_west:
            outside = True
        if item_east > coll_east:
            outside = True
        if item_south < coll_south:
            outside = True
        if item_north > coll_north:
            outside = True

        if outside:
            result.passed = False
            result.errors.append(
                ValidationMessage(
                    message=(
                        f"Item '{item.id}' bbox [{item_west}, {item_south}, {item_east}, {item_north}] "
                        f"is outside collection extent [{coll_west}, {coll_south}, {coll_east}, {coll_north}]"
                    ),
                )
            )

    return result


def validate_catalog_links(
    catalog: CatalogModel,
    catalog_path: Path,
) -> ValidationResult:
    """Validate that catalog links point to existing files.

    Checks:
    - 'self' link exists (warning if missing)
    - 'root' link exists (warning if missing)
    - 'child' links point to existing collection.json files
    - 'item' links point to existing item.json files

    Args:
        catalog: The catalog to validate.
        catalog_path: Path to the catalog.json file for resolving relative links.

    Returns:
        ValidationResult with any broken links.
    """
    result = ValidationResult()
    catalog_dir = catalog_path.parent

    # Track which required links are present
    has_self = False
    has_root = False

    for link in catalog.links:
        if link.rel == "self":
            has_self = True
        elif link.rel == "root":
            has_root = True
        elif link.rel in ("child", "item"):
            # Resolve relative path
            target_path = (catalog_dir / link.href).resolve()

            if not target_path.exists():
                result.passed = False
                result.errors.append(
                    ValidationMessage(
                        message=f"Link '{link.rel}' points to missing file: {link.href}",
                        path=str(target_path),
                    )
                )

    # Warn about missing recommended links
    if not has_self:
        result.warnings.append(
            ValidationMessage(
                message="Catalog is missing 'self' link (recommended by STAC)",
            )
        )

    if not has_root:
        result.warnings.append(
            ValidationMessage(
                message="Catalog is missing 'root' link (recommended by STAC)",
            )
        )

    return result


def check_directory_metadata(directory: Path) -> MetadataReport:
    """Scan a directory tree for metadata issues.

    Finds all STAC JSON files and geo-asset files (.parquet, .tif),
    validates STAC structure, and reports any issues.

    Args:
        directory: Root directory to scan.

    Returns:
        MetadataReport with results for all files found.
    """
    report = MetadataReport()

    if not directory.exists():
        return report

    # Find all catalog.json files
    catalog_files = list(directory.glob("**/catalog.json"))
    for catalog_path in catalog_files:
        result = _check_catalog(catalog_path)
        if result:
            report.results.append(result)

    # Find all collection.json files
    collection_files = list(directory.glob("**/collection.json"))
    for collection_path in collection_files:
        result = _check_collection(collection_path)
        if result:
            report.results.append(result)

    # Find all item JSON files (excluding catalog/collection)
    json_files = list(directory.glob("**/*.json"))
    for json_path in json_files:
        if json_path.name in ("catalog.json", "collection.json", "versions.json"):
            continue
        result = _check_item_json(json_path)
        if result:
            report.results.append(result)

    return report


def _check_catalog(catalog_path: Path) -> MetadataCheckResult | None:
    """Check a catalog.json file.

    Args:
        catalog_path: Path to catalog.json.

    Returns:
        MetadataCheckResult or None if not a valid catalog.
    """
    try:
        with open(catalog_path) as f:
            data = json.load(f)

        if data.get("type") != "Catalog":
            return None

        catalog = CatalogModel.from_dict(data)
        validation = validate_catalog_links(catalog, catalog_path)

        if validation.passed:
            return MetadataCheckResult(
                file_path=catalog_path,
                status=MetadataStatus.FRESH,
                message="Catalog metadata is valid",
            )
        else:
            return MetadataCheckResult(
                file_path=catalog_path,
                status=MetadataStatus.STALE,
                message=validation.errors[0].message if validation.errors else "Catalog has issues",
                fix_hint="Run 'portolan fix' to repair catalog links",
            )

    except (json.JSONDecodeError, KeyError, ValueError) as e:
        return MetadataCheckResult(
            file_path=catalog_path,
            status=MetadataStatus.BREAKING,
            message=f"Invalid catalog format: {e}",
            fix_hint="Check catalog.json syntax",
        )


def _check_collection(collection_path: Path) -> MetadataCheckResult | None:
    """Check a collection.json file.

    Args:
        collection_path: Path to collection.json.

    Returns:
        MetadataCheckResult or None if not a valid collection.
    """
    try:
        with open(collection_path) as f:
            data = json.load(f)

        if data.get("type") != "Collection":
            return None

        CollectionModel.from_dict(data)

        # Collection is valid if it parses
        return MetadataCheckResult(
            file_path=collection_path,
            status=MetadataStatus.FRESH,
            message="Collection metadata is valid",
        )

    except (json.JSONDecodeError, KeyError, ValueError) as e:
        return MetadataCheckResult(
            file_path=collection_path,
            status=MetadataStatus.BREAKING,
            message=f"Invalid collection format: {e}",
            fix_hint="Check collection.json syntax",
        )


def _check_item_json(json_path: Path) -> MetadataCheckResult | None:
    """Check if a JSON file is a valid STAC item.

    Args:
        json_path: Path to JSON file.

    Returns:
        MetadataCheckResult or None if not a STAC item.
    """
    try:
        with open(json_path) as f:
            data = json.load(f)

        if data.get("type") != "Feature":
            return None

        ItemModel.from_dict(data)

        # Item is valid if it parses
        return MetadataCheckResult(
            file_path=json_path,
            status=MetadataStatus.FRESH,
            message="Item metadata is valid",
        )

    except (json.JSONDecodeError, KeyError, ValueError) as e:
        return MetadataCheckResult(
            file_path=json_path,
            status=MetadataStatus.STALE,
            message=f"Invalid item format: {e}",
            fix_hint="Run 'portolan fix' to regenerate item metadata",
        )
