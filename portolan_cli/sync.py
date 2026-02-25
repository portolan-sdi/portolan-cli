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

from dataclasses import dataclass, field
from pathlib import Path
from typing import TYPE_CHECKING

from portolan_cli.catalog import CatalogState, detect_state, init_catalog
from portolan_cli.check import CheckReport, check_directory
from portolan_cli.output import error, info, success, warn
from portolan_cli.pull import PullError, PullResult, pull
from portolan_cli.push import PushConflictError, PushResult, push
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
        check_result = check_directory(catalog_root, fix=fix, dry_run=dry_run)

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
) -> tuple[PushResult | None, str | None]:
    """Execute push step. Returns (push_result, error_msg)."""
    info(f"Pushing to {destination}...")
    try:
        push_result = push(
            catalog_root=catalog_root,
            collection=collection,
            destination=destination,
            force=force,
            dry_run=dry_run,
            profile=profile,
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
        else:
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
        catalog_root, collection, destination, force, dry_run, profile
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
        pull_result: Result from the pull operation.
        local_path: Path where the catalog was cloned to.
        errors: List of error messages if any step failed.
    """

    success: bool
    pull_result: PullResult | None
    local_path: Path
    errors: list[str] = field(default_factory=list)


class CloneError(Exception):
    """Exception raised when clone fails."""

    pass


def clone(
    remote_url: str,
    local_path: Path,
    collection: str,
    *,
    profile: str | None = None,
) -> CloneResult:
    """Clone a remote catalog to a local directory.

    This is essentially "pull to an empty directory" with guardrails.
    Creates the target directory, initializes a Portolan catalog, and pulls
    the specified collection from remote storage.

    Args:
        remote_url: Remote catalog URL (e.g., s3://bucket/catalog).
        local_path: Local directory to clone into (will be created).
        collection: Collection name to clone.
        profile: AWS profile name (for S3).

    Returns:
        CloneResult with operation details.

    Raises:
        CloneError: If the target directory already exists and is not empty,
                    or if the clone operation fails.
    """
    errors: list[str] = []

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

    # Pull from remote
    info(f"Pulling collection '{collection}' from {remote_url}...")
    try:
        pull_result = pull(
            remote_url=remote_url,
            local_root=local_path,
            collection=collection,
            force=False,  # No local changes to force-overwrite
            dry_run=False,
            profile=profile,
        )

        if not pull_result.success:
            # Check if remote doesn't exist
            if pull_result.remote_version is None:
                error_msg = f"Remote collection '{collection}' not found at {remote_url}"
            else:
                error_msg = "Pull failed during clone"
            errors.append(error_msg)
            error(error_msg)
            return CloneResult(
                success=False,
                pull_result=pull_result,
                local_path=local_path,
                errors=errors,
            )

        success(f"Cloned {pull_result.files_downloaded} file(s) to {local_path}")

    except PullError as e:
        error_msg = f"Clone failed: {e}"
        errors.append(error_msg)
        error(error_msg)
        return CloneResult(
            success=False,
            pull_result=None,
            local_path=local_path,
            errors=errors,
        )

    return CloneResult(
        success=True,
        pull_result=pull_result,
        local_path=local_path,
        errors=[],
    )
