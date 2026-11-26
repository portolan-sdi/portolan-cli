"""Sync module - orchestrates catalog synchronization with remote storage.

The sync command sequences: Pull -> Init -> Scan -> Check -> Push.
All primitives already exist - this module is orchestration glue.

Workflow:
1. Pull: Fetch remote state, detect conflicts
2. Init: Initialize catalog if needed (idempotent for already-managed catalogs)
3. Scan: Discover files in catalog
4. Check: Validate cloud-native status, optionally convert with --fix
5. Push: Upload changes to remote storage

"""

from __future__ import annotations

import asyncio
from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Any

from portolan_cli.catalog import (
    CatalogState,
    detect_state,
    init_catalog,
)
from portolan_cli.scan.check import CheckReport, check_directory
from portolan_cli.scan.core import ScanResult, scan_directory
from portolan_cli.sync.pull import PullError, PullResult, pull
from portolan_cli.sync.push import PushConflictError, PushResult, push_async
from portolan_cli.sync.report import Report, silent
from portolan_cli.trace import span

if TYPE_CHECKING:
    from pathlib import Path

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

    def to_dict(self) -> dict[str, Any]:
        """Return the envelope ``data`` of ``portolan sync``."""
        data: dict[str, Any] = {"init_performed": self.init_performed, "errors": self.errors}
        if self.pull_result is not None:
            data["pull"] = {
                "files_downloaded": self.pull_result.files_downloaded,
                "files_skipped": self.pull_result.files_skipped,
                "up_to_date": self.pull_result.up_to_date,
                "local_version": self.pull_result.local_version,
                "remote_version": self.pull_result.remote_version,
            }
        if self.push_result is not None:
            data["push"] = {
                "files_uploaded": self.push_result.files_uploaded,
                "versions_pushed": self.push_result.versions_pushed,
                "conflicts": self.push_result.conflicts,
            }
        return data


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
    report: Report = silent,
) -> tuple[PullResult | None, str | None]:
    """Execute pull step. Returns (pull_result, error_msg)."""
    report("info", f"Pulling from {destination}...")
    try:
        pull_result = pull(
            remote_url=destination,
            local_root=catalog_root,
            collection=collection,
            force=force,
            dry_run=dry_run,
            profile=profile,
            report=report,
        )

        if not pull_result.success:
            # Check if this is a "first sync" case (remote doesn't exist yet)
            is_first_sync = (
                pull_result.remote_version is None and not pull_result.uncommitted_changes
            )

            if is_first_sync:
                report("info", "No remote catalog found (first sync)")
            else:
                error_msg = "Pull failed"
                if pull_result.uncommitted_changes:
                    files = ", ".join(pull_result.uncommitted_changes)
                    error_msg += f": uncommitted changes in {files}"
                return pull_result, error_msg

        if pull_result.up_to_date:
            report("info", "Already up to date with remote")
        else:
            report("success", f"Pulled {pull_result.files_downloaded} file(s)")

        return pull_result, None

    except PullError as e:
        return None, f"Pull failed: {e}"


def _step_init(
    catalog_root: Path,
    force: bool,
    report: Report = silent,
) -> tuple[bool, str | None]:
    """Execute init step. Returns (init_performed, error_msg)."""
    state = detect_state(catalog_root)

    if state == CatalogState.FRESH:
        report("info", "Initializing catalog...")
        try:
            # No license here: sync wraps a directory whose collections already
            # exist, and it never writes a collection.json of its own (issue #686).
            init_catalog(catalog_root, license_id=None)
            report("success", "Initialized catalog")
            return True, None
        except Exception as e:
            return False, f"Init failed: {e}"

    if state == CatalogState.MANAGED:
        return False, None

    # UNMANAGED_STAC - refuse without --force
    if force:
        report("warn", "Catalog is an unmanaged STAC catalog, proceeding with --force")
        return False, None

    return False, (
        "Catalog appears to be an unmanaged STAC catalog. "
        "Use 'portolan adopt' to bring it under management, or --force to sync anyway."
    )


def _step_scan(catalog_root: Path, report: Report = silent) -> tuple[ScanResult | None, str | None]:
    """Execute scan step. Returns (scan_result, error_msg)."""
    report("info", f"Scanning {catalog_root}...")
    try:
        scan_result = scan_directory(catalog_root)

        if scan_result.has_errors:
            report("warn", f"Scan found {scan_result.error_count} issue(s)")
        else:
            report("info", f"Found {len(scan_result.ready)} file(s) ready for import")

        return scan_result, None
    except Exception as e:
        return None, f"Scan failed: {e}"


def _step_check(
    catalog_root: Path,
    fix: bool,
    dry_run: bool,
    report: Report = silent,
) -> tuple[CheckReport | None, str | None]:
    """Execute check step. Returns (check_result, error_msg)."""
    report("info", "Checking cloud-native status...")
    try:
        check_result = check_directory(
            catalog_root, fix=fix, dry_run=dry_run, catalog_path=catalog_root
        )

        if check_result.convertible_count > 0:
            if fix:
                report(
                    "success",
                    f"Converted {check_result.convertible_count} file(s) to cloud-native format",
                )
            else:
                report(
                    "warn",
                    f"{check_result.convertible_count} file(s) can be converted "
                    "(use --fix to convert)",
                )

        if check_result.unsupported_count > 0:
            report("warn", f"{check_result.unsupported_count} unsupported file(s) found")

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
    report: Report = silent,
) -> tuple[PushResult | None, str | None]:
    """Execute push step. Returns (push_result, error_msg)."""
    report("info", f"Pushing to {destination}...")
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
            report(
                "success",
                f"Pushed {push_result.versions_pushed} version(s), "
                f"{push_result.files_uploaded} file(s)",
            )
        elif not dry_run and push_result.files_uploaded == 0:
            # Only show "Nothing to push" in non-dry-run mode
            # In dry-run mode, push() already printed the appropriate message
            # A metadata-only sync already reported its file count (#816).
            report("info", "Nothing to push - local and remote are in sync")

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
    report: Report = silent,
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
        report: Receives each progress line of the five steps.

    Returns:
        SyncResult with aggregated results from all steps.
    """
    # Validate catalog root exists
    if not catalog_root.exists():
        error_msg = f"Catalog root not found: {catalog_root}"
        report("error", error_msg)
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
    with span("sync.pull"):
        pull_result, pull_error = _step_pull(
            destination, catalog_root, collection, force, dry_run, profile, report
        )
    if pull_error:
        report("error", pull_error)
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
    with span("sync.init"):
        init_performed, init_error = _step_init(catalog_root, force, report)
    if init_error:
        report("error", init_error)
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
    with span("sync.scan"):
        scan_result, scan_error = _step_scan(catalog_root, report)
    if scan_error:
        report("error", scan_error)
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
    with span("sync.check"):
        check_result, check_error = _step_check(catalog_root, fix, dry_run, report)
    if check_error:
        report("error", check_error)
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
    with span("sync.push"):
        push_result, push_error = _step_push(
            catalog_root, collection, destination, force, dry_run, profile, region, report
        )
    if push_error:
        report("error", push_error)
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
