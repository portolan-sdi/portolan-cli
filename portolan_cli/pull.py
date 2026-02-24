"""Pull updates from remote cloud storage catalogs.

This module provides functionality to synchronize a local catalog with its
remote counterpart, similar to `git pull`. It:

1. Fetches remote versions.json
2. Diffs against local versions.json
3. Checks for uncommitted local changes
4. Downloads changed assets (unless --force is used to overwrite)
5. Updates local versions.json

Mental model: `portolan pull` is like `git pull`
- If local has uncommitted changes that would be overwritten, refuse (unless --force)
- Show what would change with --dry-run
- Update tracking file (versions.json) after successful sync

See ADR-0005 for versions.json as single source of truth.
See ADR-0017 for MTIME + heuristics change detection.
"""

from __future__ import annotations

import tempfile
from dataclasses import dataclass, field
from pathlib import Path
from typing import TYPE_CHECKING

from portolan_cli.dataset import compute_checksum
from portolan_cli.download import download_file
from portolan_cli.output import detail, error, info, success, warn
from portolan_cli.upload import parse_object_store_url
from portolan_cli.versions import (
    VersionsFile,
    read_versions,
    write_versions,
)

if TYPE_CHECKING:
    pass


# =============================================================================
# Exceptions
# =============================================================================


class PullError(Exception):
    """Base exception for pull operations."""

    pass


# =============================================================================
# Data Classes
# =============================================================================


@dataclass
class PullResult:
    """Result of a pull operation.

    Attributes:
        success: True if pull completed successfully.
        files_downloaded: Number of files downloaded.
        files_skipped: Number of files skipped (already up to date).
        local_version: Local version before pull.
        remote_version: Remote version.
        uncommitted_changes: List of files with uncommitted changes that blocked pull.
        up_to_date: True if already at remote version (nothing to pull).
    """

    success: bool
    files_downloaded: int
    files_skipped: int
    local_version: str | None
    remote_version: str | None
    uncommitted_changes: list[str] = field(default_factory=list)
    up_to_date: bool = False


@dataclass
class VersionDiff:
    """Diff between local and remote versions.

    Attributes:
        local_version: Current local version string.
        remote_version: Current remote version string.
        is_behind: True if local is behind remote.
        files_to_download: List of file names that need downloading.
        remote_assets: Dict mapping filename to remote asset metadata.
    """

    local_version: str | None
    remote_version: str | None
    is_behind: bool
    files_to_download: list[str]
    remote_assets: dict[str, dict[str, str | int]] = field(default_factory=dict)


# =============================================================================
# Change Detection
# =============================================================================


def detect_uncommitted_changes(
    catalog_root: Path,
    collection: str,
) -> list[str]:
    """Detect local files that differ from what versions.json says they should be.

    This is like `git status` - it compares the actual local files against
    what the local versions.json says they should be.

    Args:
        catalog_root: Root directory of the catalog.
        collection: Collection name/identifier.

    Returns:
        List of filenames with uncommitted changes.

    Raises:
        FileNotFoundError: If versions.json doesn't exist.
    """
    versions_path = catalog_root / ".portolan" / "collections" / collection / "versions.json"

    try:
        versions_file = read_versions(versions_path)
    except FileNotFoundError:
        # No versions.json means no tracked files, no uncommitted changes
        return []

    if not versions_file.versions:
        return []

    # Get the current version's expected asset checksums
    current_version = versions_file.versions[-1]
    uncommitted = []

    for asset_name, asset in current_version.assets.items():
        # Determine local file path (relative to catalog root)
        local_path = catalog_root / asset.href

        if not local_path.exists():
            # File is missing - this is an uncommitted change (deletion)
            uncommitted.append(asset_name)
            continue

        # Compare checksum
        actual_checksum = compute_checksum(local_path)
        if actual_checksum != asset.sha256:
            uncommitted.append(asset_name)

    return uncommitted


# =============================================================================
# Version Diffing
# =============================================================================


def diff_versions(
    local_versions: VersionsFile,
    remote_versions: VersionsFile,
) -> VersionDiff:
    """Diff local and remote versions to determine what needs downloading.

    Args:
        local_versions: Local versions.json content.
        remote_versions: Remote versions.json content.

    Returns:
        VersionDiff with files that need downloading.
    """
    local_version = local_versions.current_version
    remote_version = remote_versions.current_version

    # If versions match, nothing to do
    if local_version == remote_version:
        return VersionDiff(
            local_version=local_version,
            remote_version=remote_version,
            is_behind=False,
            files_to_download=[],
        )

    # Get remote's current assets
    remote_assets: dict[str, dict[str, str | int]] = {}
    if remote_versions.versions:
        current_remote = remote_versions.versions[-1]
        for name, asset in current_remote.assets.items():
            remote_assets[name] = {
                "sha256": asset.sha256,
                "size_bytes": asset.size_bytes,
                "href": asset.href,
            }

    # Get local's current assets (if any)
    local_assets: dict[str, str] = {}  # name -> sha256
    if local_versions.versions:
        current_local = local_versions.versions[-1]
        for name, asset in current_local.assets.items():
            local_assets[name] = asset.sha256

    # Find files that need downloading:
    # - New files not in local
    # - Files with different checksums
    files_to_download = []
    for name, remote_asset in remote_assets.items():
        if name not in local_assets:
            files_to_download.append(name)
        elif local_assets[name] != remote_asset["sha256"]:
            files_to_download.append(name)

    return VersionDiff(
        local_version=local_version,
        remote_version=remote_version,
        is_behind=True,
        files_to_download=files_to_download,
        remote_assets=remote_assets,
    )


# =============================================================================
# Remote Fetch
# =============================================================================


def _fetch_remote_versions(
    remote_url: str,
    collection: str,
    profile: str | None = None,
) -> VersionsFile:
    """Fetch remote versions.json.

    Args:
        remote_url: Remote catalog URL (e.g., s3://bucket/catalog).
        collection: Collection name.
        profile: AWS profile name (for S3).

    Returns:
        Parsed VersionsFile from remote.

    Raises:
        PullError: If fetch fails.
    """
    # Build remote versions.json path
    remote_versions_url = (
        f"{remote_url.rstrip('/')}/.portolan/collections/{collection}/versions.json"
    )

    # Download to temp file
    with tempfile.NamedTemporaryFile(suffix=".json", delete=False) as tmp:
        tmp_path = Path(tmp.name)

    try:
        result = download_file(
            source=remote_versions_url,
            destination=tmp_path,
            profile=profile,
        )

        if not result.success:
            raise PullError(f"Failed to fetch remote versions.json: {result.errors}")

        # Parse the downloaded file
        return read_versions(tmp_path)

    finally:
        # Clean up temp file
        if tmp_path.exists():
            tmp_path.unlink()


def _download_assets(
    remote_url: str,
    local_root: Path,
    files_to_download: list[str],
    remote_assets: dict[str, dict[str, str | int]],
    profile: str | None = None,
    dry_run: bool = False,
) -> tuple[int, int]:
    """Download assets from remote.

    Args:
        remote_url: Remote catalog base URL.
        local_root: Local catalog root directory.
        files_to_download: List of filenames to download.
        remote_assets: Dict mapping filename to asset metadata.
        profile: AWS profile name (for S3).
        dry_run: If True, don't actually download.

    Returns:
        Tuple of (files_downloaded, files_failed).
    """
    downloaded = 0
    failed = 0

    for filename in files_to_download:
        asset = remote_assets.get(filename)
        if not asset:
            continue

        # Build URLs
        href = str(asset["href"])
        remote_asset_url = f"{remote_url.rstrip('/')}/{href}"
        local_path = local_root / href

        if dry_run:
            info(f"[DRY RUN] Would download: {filename}")
            continue

        # Ensure parent directory exists
        local_path.parent.mkdir(parents=True, exist_ok=True)

        result = download_file(
            source=remote_asset_url,
            destination=local_path,
            profile=profile,
            overwrite=True,
        )

        if result.success:
            downloaded += 1
        else:
            failed += 1

    return downloaded, failed


# =============================================================================
# Main Pull Function
# =============================================================================


def pull(
    remote_url: str,
    local_root: Path,
    collection: str,
    *,
    force: bool = False,
    dry_run: bool = False,
    profile: str | None = None,
) -> PullResult:
    """Pull updates from a remote catalog.

    Similar to `git pull`, this:
    1. Fetches remote versions.json
    2. Compares with local versions.json
    3. Checks for uncommitted local changes
    4. Downloads changed files (unless blocked by uncommitted changes)
    5. Updates local versions.json

    Args:
        remote_url: Remote catalog URL (e.g., s3://bucket/catalog).
        local_root: Local catalog root directory.
        collection: Collection name to pull.
        force: If True, overwrite uncommitted local changes.
        dry_run: If True, show what would happen without downloading.
        profile: AWS profile name (for S3).

    Returns:
        PullResult with operation results.

    Raises:
        ValueError: If remote_url is invalid.
    """
    # Validate URL
    try:
        parse_object_store_url(remote_url)
    except ValueError as e:
        raise ValueError(f"Invalid remote URL: {e}") from e

    # Path to local versions.json
    versions_path = local_root / ".portolan" / "collections" / collection / "versions.json"

    # Load local versions (may not exist yet)
    try:
        local_versions = read_versions(versions_path)
    except FileNotFoundError:
        local_versions = VersionsFile(
            spec_version="1.0.0",
            current_version=None,
            versions=[],
        )

    # Fetch remote versions
    try:
        remote_versions = _fetch_remote_versions(remote_url, collection, profile)
    except PullError as e:
        error(f"Failed to fetch remote: {e}")
        return PullResult(
            success=False,
            files_downloaded=0,
            files_skipped=0,
            local_version=local_versions.current_version,
            remote_version=None,
        )

    # Diff versions
    diff = diff_versions(local_versions, remote_versions)

    # Check if up to date
    if not diff.is_behind:
        info("Already up to date")
        return PullResult(
            success=True,
            files_downloaded=0,
            files_skipped=0,
            local_version=diff.local_version,
            remote_version=diff.remote_version,
            up_to_date=True,
        )

    # Check for uncommitted changes (unless --force)
    uncommitted: list[str] = []
    if not force:
        uncommitted = detect_uncommitted_changes(local_root, collection)
        # Only care about uncommitted changes to files that would be overwritten
        conflicts = [f for f in uncommitted if f in diff.files_to_download]

        if conflicts:
            warn("Uncommitted local changes would be overwritten:")
            for filename in conflicts:
                detail(f"  {filename}")
            warn("Use --force to discard local changes")

            return PullResult(
                success=False,
                files_downloaded=0,
                files_skipped=0,
                local_version=diff.local_version,
                remote_version=diff.remote_version,
                uncommitted_changes=conflicts,
            )

    # Report what will be downloaded
    info(f"Pulling {len(diff.files_to_download)} file(s) from {remote_url}")
    for filename in diff.files_to_download:
        detail(f"  {filename}")

    # Download assets
    downloaded, failed = _download_assets(
        remote_url=remote_url,
        local_root=local_root,
        files_to_download=diff.files_to_download,
        remote_assets=diff.remote_assets,
        profile=profile,
        dry_run=dry_run,
    )

    # Handle failures
    if failed > 0:
        error(f"Failed to download {failed} file(s)")
        return PullResult(
            success=False,
            files_downloaded=downloaded,
            files_skipped=0,
            local_version=diff.local_version,
            remote_version=diff.remote_version,
        )

    # Update local versions.json (unless dry-run)
    if not dry_run:
        # Write remote versions.json to local
        versions_path.parent.mkdir(parents=True, exist_ok=True)
        write_versions(versions_path, remote_versions)
        success(f"Updated to version {diff.remote_version}")

    return PullResult(
        success=True,
        files_downloaded=downloaded if not dry_run else 0,
        files_skipped=len(diff.files_to_download) - downloaded if not dry_run else 0,
        local_version=diff.local_version,
        remote_version=diff.remote_version,
    )
