"""Sync module - orchestrates catalog synchronization with remote storage.

The sync command sequences: Pull -> Init -> Scan -> Check -> Push.
All primitives already exist - this module is orchestration glue.

Workflow:
1. Pull: Fetch remote state, detect conflicts
2. Init: Initialize catalog if needed (idempotent for already-managed catalogs)
3. Scan: Discover files in catalog
4. Check: Validate cloud-native status, optionally convert with --fix
5. Push: Upload changes to remote storage

See ADR-0005 for versions.json as single source of truth.
See ADR-0007 for CLI wraps Python API (all logic in library layer).
"""

from __future__ import annotations

import asyncio
import json
import tempfile
from dataclasses import dataclass, field
from pathlib import Path
from typing import TYPE_CHECKING, Any

from portolan_cli.catalog import CatalogState, detect_state, init_catalog
from portolan_cli.check import CheckReport, check_directory
from portolan_cli.download import download_file
from portolan_cli.output import detail, error, info, success, warn
from portolan_cli.pull import PullError, PullResult, pull
from portolan_cli.push import PushConflictError, PushResult, push_async
from portolan_cli.scan import ScanResult, scan_directory

if TYPE_CHECKING:
    pass


# =============================================================================
# Exceptions
# =============================================================================


class SyncError(Exception):
    """Base exception for sync operations."""

    pass


# =============================================================================
# URL Parsing Utilities
# =============================================================================


def infer_local_path_from_url(remote_url: str) -> Path:
    """Infer local directory name from a remote catalog URL.

    Extracts the last path component from the URL as the directory name,
    similar to how `git clone` infers directory name from repo URL.

    Args:
        remote_url: Remote catalog URL (e.g., s3://mybucket/my-catalog).

    Returns:
        Path object with the inferred directory name.

    Raises:
        ValueError: If cannot infer a name (e.g., bucket-only URL).

    Examples:
        >>> infer_local_path_from_url("s3://bucket/my-catalog")
        Path('my-catalog')
        >>> infer_local_path_from_url("s3://bucket/path/to/catalog/")
        Path('catalog')
    """
    # Strip trailing slashes
    url = remote_url.rstrip("/")

    # Split by '/' and get last non-empty component
    parts = url.split("/")

    # Filter out empty parts (from multiple slashes)
    parts = [p for p in parts if p]

    # We need at least scheme://bucket/catalog (3+ parts after split)
    # e.g., ['s3:', 'bucket', 'catalog'] or ['s3:', '', 'bucket', 'catalog']
    # After removing scheme part, we need the catalog name
    if len(parts) < 3:
        raise ValueError(
            f"Cannot infer local path from URL: {remote_url}. "
            "URL must include a catalog name (e.g., s3://bucket/catalog-name)."
        )

    catalog_name = parts[-1]

    # The last part should not be empty or just the bucket
    if not catalog_name or catalog_name.endswith(":"):
        raise ValueError(
            f"Cannot infer local path from URL: {remote_url}. "
            "URL must include a catalog name (e.g., s3://bucket/catalog-name)."
        )

    return Path(catalog_name)


# =============================================================================
# Remote Catalog Fetching
# =============================================================================


def _fetch_remote_catalog_json(
    remote_url: str,
    *,
    profile: str | None = None,
) -> dict[str, Any]:
    """Fetch and parse catalog.json from a remote catalog.

    Args:
        remote_url: Remote catalog URL (e.g., s3://bucket/catalog).
        profile: AWS profile name (for S3).

    Returns:
        Parsed catalog.json as a dictionary.

    Raises:
        CloneError: If fetch or parse fails.
    """
    # Build remote catalog.json path
    catalog_url = f"{remote_url.rstrip('/')}/catalog.json"

    # Download to temp file
    with tempfile.NamedTemporaryFile(suffix=".json", delete=False) as tmp:
        tmp_path = Path(tmp.name)

    try:
        result = download_file(
            source=catalog_url,
            destination=tmp_path,
            profile=profile,
        )

        if not result.success:
            error_msgs = (
                ", ".join(f"{path}: {exc}" for path, exc in result.errors)
                if result.errors
                else "Unknown error"
            )
            raise CloneError(f"Failed to fetch remote catalog.json: {error_msgs}")

        # Parse the downloaded file
        with open(tmp_path, encoding="utf-8") as f:
            data: dict[str, Any] = json.load(f)
            return data

    except json.JSONDecodeError as e:
        raise CloneError(f"Failed to parse remote catalog.json: {e}") from e

    finally:
        # Clean up temp file
        if tmp_path.exists():
            tmp_path.unlink()


def list_remote_collections(
    remote_url: str,
    *,
    profile: str | None = None,
    max_depth: int = 10,
) -> list[str]:
    """List all collections available in a remote catalog.

    Recursively fetches catalog.json files and parses STAC child links
    to discover collection names. Handles nested catalog structures
    per ADR-0032 (nested catalogs with flat collections).

    Args:
        remote_url: Remote catalog URL (e.g., s3://bucket/catalog).
        profile: AWS profile name (for S3).
        max_depth: Maximum recursion depth to prevent infinite loops.
            Default is 10, which should handle most reasonable hierarchies.

    Returns:
        List of collection names found in the catalog (including nested).

    Raises:
        CloneError: If unable to fetch or parse the catalog.
    """
    # Track visited URLs to detect circular references
    visited_urls: set[str] = set()

    return _list_remote_collections_recursive(
        remote_url=remote_url,
        profile=profile,
        max_depth=max_depth,
        current_depth=0,
        visited_urls=visited_urls,
        path_prefix="",  # Start with empty prefix at root
    )


def _resolve_href(base_url: str, href: str) -> str:
    """Resolve a potentially relative href against a base URL.

    Args:
        base_url: The base catalog URL (e.g., s3://bucket/catalog).
        href: The href to resolve (relative or absolute).

    Returns:
        Absolute URL for the href.
    """
    # If href is already absolute (has scheme), return as-is
    if "://" in href:
        return href

    # Handle relative paths
    # Strip trailing slash from base
    base = base_url.rstrip("/")

    # Handle "./" prefix
    if href.startswith("./"):
        href = href[2:]

    # Handle "../" prefix
    while href.startswith("../"):
        href = href[3:]
        # Go up one directory in base
        if "/" in base:
            base = base.rsplit("/", 1)[0]

    return f"{base}/{href}"


def _extract_catalog_url_from_href(base_url: str, href: str) -> str:
    """Extract the catalog directory URL from a catalog.json href.

    Args:
        base_url: The base catalog URL.
        href: The href pointing to a catalog.json file.

    Returns:
        The catalog directory URL (without catalog.json suffix).
    """
    full_url = _resolve_href(base_url, href)

    # Remove the catalog.json suffix to get the catalog directory
    if full_url.endswith("/catalog.json"):
        return full_url[: -len("/catalog.json")]
    elif full_url.endswith("catalog.json"):
        return full_url[: -len("catalog.json")].rstrip("/")

    return full_url


def _list_remote_collections_recursive(
    remote_url: str,
    *,
    profile: str | None,
    max_depth: int,
    current_depth: int,
    visited_urls: set[str],
    path_prefix: str = "",
) -> list[str]:
    """Recursively list collections from a catalog and its subcatalogs.

    Internal recursive helper for list_remote_collections.

    Args:
        path_prefix: The accumulated path from the root catalog to this point.
            Used to construct full collection paths like "climate/hittekaart".
    """
    # Normalize URL for visited check
    normalized_url = remote_url.rstrip("/")

    # Check for circular reference
    if normalized_url in visited_urls:
        return []

    # Check depth limit
    if current_depth >= max_depth:
        warn(f"Maximum catalog depth ({max_depth}) reached at {remote_url}")
        return []

    # Mark as visited
    visited_urls.add(normalized_url)

    # Fetch this catalog
    catalog_data = _fetch_remote_catalog_json(remote_url, profile=profile)

    collections: list[str] = []

    # Parse STAC links to find child collections and subcatalogs
    links = catalog_data.get("links", [])
    for link in links:
        if link.get("rel") != "child":
            continue

        href = link.get("href", "")
        if not href:
            continue

        # Determine if this is a subcatalog or a collection
        if href.endswith("catalog.json"):
            # This is a subcatalog - recurse into it
            subcatalog_url = _extract_catalog_url_from_href(remote_url, href)

            # Extract the subcatalog directory name to build the path prefix
            # e.g., "./climate/catalog.json" -> "climate"
            subcatalog_dir = _extract_subcatalog_dir_from_href(href)
            new_prefix = f"{path_prefix}{subcatalog_dir}/" if subcatalog_dir else path_prefix

            nested_collections = _list_remote_collections_recursive(
                remote_url=subcatalog_url,
                profile=profile,
                max_depth=max_depth,
                current_depth=current_depth + 1,
                visited_urls=visited_urls,
                path_prefix=new_prefix,
            )
            collections.extend(nested_collections)

        elif href.endswith("collection.json"):
            # This is an actual collection - extract the name with full path
            collection_name = _extract_collection_name_from_href(href)
            if collection_name:
                # Prepend path prefix for nested collections
                full_path = f"{path_prefix}{collection_name}" if path_prefix else collection_name
                collections.append(full_path)

        else:
            # Unknown link type - could be a collection without .json suffix
            # Try to extract a name anyway (backwards compatibility)
            collection_name = _extract_collection_name_from_href(href)
            if collection_name:
                full_path = f"{path_prefix}{collection_name}" if path_prefix else collection_name
                collections.append(full_path)

    return collections


def _extract_subcatalog_dir_from_href(href: str) -> str:
    """Extract the subcatalog directory name from an href.

    Used to build path prefixes for nested collections.

    Args:
        href: The href pointing to a catalog.json file.
            e.g., "./climate/catalog.json" or "s3://bucket/catalog/climate/catalog.json"

    Returns:
        The subcatalog directory name (e.g., "climate").
    """
    # Remove catalog.json suffix
    if href.endswith("/catalog.json"):
        href = href[: -len("/catalog.json")]
    elif href.endswith("catalog.json"):
        href = href[: -len("catalog.json")]

    # Remove any trailing slashes
    href = href.rstrip("/")

    # Handle relative paths
    if href.startswith("./"):
        href = href[2:]

    # Get the last path component (subcatalog directory name)
    if "/" in href:
        return href.split("/")[-1]

    return href


def _extract_collection_name_from_href(href: str) -> str:
    """Extract collection path from an href.

    Handles both relative (./environment/collection-name/collection.json)
    and absolute (s3://bucket/catalog/collection-name/collection.json) paths.

    For relative paths, preserves the full path structure (e.g., "environment/bodemkwaliteit").
    For absolute paths, extracts only the leaf directory name.

    Args:
        href: The href pointing to a collection.json file.

    Returns:
        The collection path relative to the catalog root.
    """
    # Remove collection.json suffix if present
    if href.endswith("/collection.json"):
        href = href[: -len("/collection.json")]
    elif href.endswith("collection.json"):
        href = href[: -len("collection.json")]

    # Remove any trailing slashes
    href = href.rstrip("/")

    # Handle relative paths - preserve full structure
    if href.startswith("./"):
        return href[2:]  # Remove "./" prefix, keep rest (e.g., "environment/bodemkwaliteit")

    # Handle absolute URLs - extract only leaf directory
    # (absolute URLs are already resolved to a specific location)
    if "://" in href:
        # Get the last path component for absolute URLs
        if "/" in href:
            return href.split("/")[-1]
        return href

    # For other cases (bare relative paths), return as-is
    return href


# =============================================================================
# Data Classes
# =============================================================================


@dataclass
class SyncResult:
    """Result of a sync operation.

    Aggregates results from all sync steps: pull, init, scan, check, push.

    Attributes:
        success: True if sync completed without errors.
        pull_result: Result from the pull step, or None if not executed.
        init_performed: True if init was called (False if already managed).
        scan_result: Result from the scan step, or None if not executed.
        check_result: Result from the check step, or None if not executed.
        push_result: Result from the push step, or None if not executed.
        errors: List of error messages from any step.
    """

    success: bool
    pull_result: PullResult | None
    init_performed: bool
    scan_result: ScanResult | None
    check_result: CheckReport | None
    push_result: PushResult | None
    errors: list[str] = field(default_factory=list)


# =============================================================================
# Sync Step Helpers
# =============================================================================


def _step_pull(
    destination: str,
    catalog_root: Path,
    collection: str,
    force: bool,
    dry_run: bool,
    profile: str | None,
) -> tuple[PullResult | None, str | None]:
    """Execute pull step. Returns (pull_result, error_msg)."""
    info(f"Pulling from {destination}...")
    try:
        pull_result = pull(
            remote_url=destination,
            local_root=catalog_root,
            collection=collection,
            force=force,
            dry_run=dry_run,
            profile=profile,
        )

        if not pull_result.success:
            # Check if this is a "first sync" case (remote doesn't exist yet)
            is_first_sync = (
                pull_result.remote_version is None and not pull_result.uncommitted_changes
            )

            if is_first_sync:
                info("No remote catalog found (first sync)")
            else:
                error_msg = "Pull failed"
                if pull_result.uncommitted_changes:
                    files = ", ".join(pull_result.uncommitted_changes)
                    error_msg += f": uncommitted changes in {files}"
                return pull_result, error_msg

        if pull_result.up_to_date:
            info("Already up to date with remote")
        else:
            success(f"Pulled {pull_result.files_downloaded} file(s)")

        return pull_result, None

    except PullError as e:
        return None, f"Pull failed: {e}"


def _step_init(
    catalog_root: Path,
    force: bool,
) -> tuple[bool, str | None]:
    """Execute init step. Returns (init_performed, error_msg)."""
    state = detect_state(catalog_root)

    if state == CatalogState.FRESH:
        info("Initializing catalog...")
        try:
            init_catalog(catalog_root)
            success("Initialized catalog")
            return True, None
        except Exception as e:
            return False, f"Init failed: {e}"

    if state == CatalogState.MANAGED:
        return False, None

    # UNMANAGED_STAC - refuse without --force
    if force:
        warn("Catalog is an unmanaged STAC catalog, proceeding with --force")
        return False, None

    return False, (
        "Catalog appears to be an unmanaged STAC catalog. "
        "Use 'portolan adopt' to bring it under management, or --force to sync anyway."
    )


def _step_scan(catalog_root: Path) -> tuple[ScanResult | None, str | None]:
    """Execute scan step. Returns (scan_result, error_msg)."""
    info(f"Scanning {catalog_root}...")
    try:
        scan_result = scan_directory(catalog_root)

        if scan_result.has_errors:
            warn(f"Scan found {scan_result.error_count} issue(s)")
        else:
            info(f"Found {len(scan_result.ready)} file(s) ready for import")

        return scan_result, None
    except Exception as e:
        return None, f"Scan failed: {e}"


def _step_check(
    catalog_root: Path,
    fix: bool,
    dry_run: bool,
) -> tuple[CheckReport | None, str | None]:
    """Execute check step. Returns (check_result, error_msg)."""
    info("Checking cloud-native status...")
    try:
        check_result = check_directory(
            catalog_root, fix=fix, dry_run=dry_run, catalog_path=catalog_root
        )

        if check_result.convertible_count > 0:
            if fix:
                success(
                    f"Converted {check_result.convertible_count} file(s) to cloud-native format"
                )
            else:
                warn(
                    f"{check_result.convertible_count} file(s) can be converted "
                    "(use --fix to convert)"
                )

        if check_result.unsupported_count > 0:
            warn(f"{check_result.unsupported_count} unsupported file(s) found")

        return check_result, None
    except Exception as e:
        return None, f"Check failed: {e}"


def _step_push(
    catalog_root: Path,
    collection: str,
    destination: str,
    force: bool,
    dry_run: bool,
    profile: str | None,
    region: str | None,
) -> tuple[PushResult | None, str | None]:
    """Execute push step. Returns (push_result, error_msg)."""
    info(f"Pushing to {destination}...")
    try:
        push_result = asyncio.run(
            push_async(
                catalog_root=catalog_root,
                collection=collection,
                destination=destination,
                force=force,
                dry_run=dry_run,
                profile=profile,
                region=region,
            )
        )

        if not push_result.success:
            error_msg = "Push failed"
            if push_result.errors:
                details = ", ".join(push_result.errors)
                error_msg += f": {details}"
            return push_result, error_msg

        if push_result.versions_pushed > 0:
            success(
                f"Pushed {push_result.versions_pushed} version(s), "
                f"{push_result.files_uploaded} file(s)"
            )
        elif not dry_run:
            # Only show "Nothing to push" in non-dry-run mode
            # In dry-run mode, push() already printed the appropriate message
            info("Nothing to push - local and remote are in sync")

        return push_result, None

    except PushConflictError as e:
        return None, f"Push conflict: {e}"
    except Exception as e:
        return None, f"Push failed: {e}"


# =============================================================================
# Main Sync Function
# =============================================================================


def sync(
    catalog_root: Path,
    collection: str,
    destination: str,
    *,
    force: bool = False,
    dry_run: bool = False,
    fix: bool = False,
    profile: str | None = None,
    region: str | None = None,
) -> SyncResult:
    """Sync local catalog with remote storage.

    Orchestrates the full sync workflow:
    1. Pull: Fetch remote state, detect conflicts
    2. Init: Initialize catalog if needed (skipped if already managed)
    3. Scan: Discover files in catalog
    4. Check: Validate cloud-native status (with --fix: convert non-cloud-native)
    5. Push: Upload changes to remote storage

    Args:
        catalog_root: Path to catalog root directory.
        collection: Collection identifier to sync.
        destination: Object store URL (e.g., s3://bucket/prefix).
        force: If True, overwrite conflicts (passes to pull and push).
        dry_run: If True, show what would happen without making changes.
        fix: If True, convert non-cloud-native formats during check.
        profile: AWS profile name (for S3 destinations).
        region: AWS region (for S3 destinations). Overrides profile/env config.

    Returns:
        SyncResult with aggregated results from all steps.
    """
    # Validate catalog root exists
    if not catalog_root.exists():
        error_msg = f"Catalog root not found: {catalog_root}"
        error(error_msg)
        return SyncResult(
            success=False,
            pull_result=None,
            init_performed=False,
            scan_result=None,
            check_result=None,
            push_result=None,
            errors=[error_msg],
        )

    # Step 1: Pull
    pull_result, pull_error = _step_pull(
        destination, catalog_root, collection, force, dry_run, profile
    )
    if pull_error:
        error(pull_error)
        return SyncResult(
            success=False,
            pull_result=pull_result,
            init_performed=False,
            scan_result=None,
            check_result=None,
            push_result=None,
            errors=[pull_error],
        )

    # Step 2: Init
    init_performed, init_error = _step_init(catalog_root, force)
    if init_error:
        error(init_error)
        return SyncResult(
            success=False,
            pull_result=pull_result,
            init_performed=False,
            scan_result=None,
            check_result=None,
            push_result=None,
            errors=[init_error],
        )

    # Step 3: Scan
    scan_result, scan_error = _step_scan(catalog_root)
    if scan_error:
        error(scan_error)
        return SyncResult(
            success=False,
            pull_result=pull_result,
            init_performed=init_performed,
            scan_result=None,
            check_result=None,
            push_result=None,
            errors=[scan_error],
        )

    # Step 4: Check
    check_result, check_error = _step_check(catalog_root, fix, dry_run)
    if check_error:
        error(check_error)
        return SyncResult(
            success=False,
            pull_result=pull_result,
            init_performed=init_performed,
            scan_result=scan_result,
            check_result=None,
            push_result=None,
            errors=[check_error],
        )

    # Step 5: Push
    push_result, push_error = _step_push(
        catalog_root, collection, destination, force, dry_run, profile, region
    )
    if push_error:
        error(push_error)
        return SyncResult(
            success=False,
            pull_result=pull_result,
            init_performed=init_performed,
            scan_result=scan_result,
            check_result=check_result,
            push_result=push_result,
            errors=[push_error],
        )

    # Success
    return SyncResult(
        success=True,
        pull_result=pull_result,
        init_performed=init_performed,
        scan_result=scan_result,
        check_result=check_result,
        push_result=push_result,
        errors=[],
    )


# =============================================================================
# Clone Function
# =============================================================================


@dataclass
class CloneResult:
    """Result of a clone operation.

    Attributes:
        success: True if clone completed successfully.
        pull_result: Result from the pull operation (single collection).
        local_path: Path where the catalog was cloned to.
        errors: List of error messages if any step failed.
        collections_cloned: List of collection names that were cloned (multi-collection).
        total_files_downloaded: Total files downloaded across all collections.
    """

    success: bool
    pull_result: PullResult | None
    local_path: Path
    errors: list[str] = field(default_factory=list)
    collections_cloned: list[str] = field(default_factory=list)
    total_files_downloaded: int = 0


class CloneError(Exception):
    """Exception raised when clone fails."""

    pass


def clone(
    remote_url: str,
    local_path: Path,
    collection: str | None = None,
    *,
    profile: str | None = None,
) -> CloneResult:
    """Clone a remote catalog to a local directory.

    This is essentially "pull to an empty directory" with guardrails.
    Creates the target directory, initializes a Portolan catalog, and pulls
    collections from remote storage.

    When collection is None, all collections from the remote catalog are cloned.
    When collection is specified, only that collection is cloned.

    Args:
        remote_url: Remote catalog URL (e.g., s3://bucket/catalog).
        local_path: Local directory to clone into (will be created).
        collection: Collection name to clone, or None to clone all collections.
        profile: AWS profile name (for S3).

    Returns:
        CloneResult with operation details.

    Raises:
        CloneError: If the target directory already exists and is not empty,
                    or if the clone operation fails.
    """
    errors: list[str] = []
    collections_cloned: list[str] = []
    total_files: int = 0

    # Check if target already exists
    if local_path.exists():
        contents = list(local_path.iterdir())
        if contents:
            error_msg = f"Target directory is not empty: {local_path}"
            error(error_msg)
            return CloneResult(
                success=False,
                pull_result=None,
                local_path=local_path,
                errors=[error_msg],
            )

    # Determine which collections to clone
    if collection is None:
        # Clone all collections - fetch remote catalog to discover them
        info("Discovering remote collections...")
        try:
            collections_to_clone = list_remote_collections(remote_url, profile=profile)
        except CloneError as e:
            error_msg = str(e)
            error(error_msg)
            return CloneResult(
                success=False,
                pull_result=None,
                local_path=local_path,
                errors=[error_msg],
            )

        if not collections_to_clone:
            error_msg = f"Remote catalog has no collections to clone: {remote_url}"
            error(error_msg)
            return CloneResult(
                success=False,
                pull_result=None,
                local_path=local_path,
                errors=[error_msg],
            )

        info(f"Found {len(collections_to_clone)} collection(s): {', '.join(collections_to_clone)}")
    else:
        collections_to_clone = [collection]

    # Create target directory
    info(f"Cloning to {local_path}...")
    local_path.mkdir(parents=True, exist_ok=True)

    # Initialize catalog
    info("Initializing catalog...")
    try:
        init_catalog(
            local_path,
            title=f"Clone of {remote_url}",
            description=f"Cloned from {remote_url}",
        )
    except Exception as e:
        error_msg = f"Failed to initialize catalog: {e}"
        errors.append(error_msg)
        error(error_msg)
        return CloneResult(
            success=False,
            pull_result=None,
            local_path=local_path,
            errors=errors,
        )

    # Pull each collection
    last_pull_result: PullResult | None = None

    for coll in collections_to_clone:
        info(f"Pulling collection '{coll}' from {remote_url}...")
        try:
            pull_result = pull(
                remote_url=remote_url,
                local_root=local_path,
                collection=coll,
                force=False,  # No local changes to force-overwrite
                dry_run=False,
                profile=profile,
            )

            if not pull_result.success:
                # Check if remote doesn't exist
                if pull_result.remote_version is None:
                    error_msg = f"Remote collection '{coll}' not found at {remote_url}"
                else:
                    error_msg = f"Pull failed for collection '{coll}'"
                errors.append(error_msg)
                error(error_msg)
                # Continue with other collections instead of failing immediately
                continue

            collections_cloned.append(coll)
            total_files += pull_result.files_downloaded
            last_pull_result = pull_result
            detail(f"Cloned {pull_result.files_downloaded} file(s) from '{coll}'")

        except PullError as e:
            error_msg = f"Clone failed for collection '{coll}': {e}"
            errors.append(error_msg)
            error(error_msg)
            # Continue with other collections
            continue

    # Determine overall success
    if not collections_cloned:
        # No collections were successfully cloned
        return CloneResult(
            success=False,
            pull_result=last_pull_result,
            local_path=local_path,
            errors=errors,
            collections_cloned=[],
            total_files_downloaded=0,
        )

    # At least some collections succeeded
    if errors:
        # Partial success
        warn(
            f"Cloned {len(collections_cloned)}/{len(collections_to_clone)} collections with errors"
        )
        return CloneResult(
            success=False,  # Partial failure is still failure
            pull_result=last_pull_result,
            local_path=local_path,
            errors=errors,
            collections_cloned=collections_cloned,
            total_files_downloaded=total_files,
        )

    # Full success
    success(
        f"Cloned {total_files} file(s) from {len(collections_cloned)} collection(s) to {local_path}"
    )

    return CloneResult(
        success=True,
        pull_result=last_pull_result,
        local_path=local_path,
        errors=[],
        collections_cloned=collections_cloned,
        total_files_downloaded=total_files,
    )
