"""Pull updates from remote cloud storage catalogs."""

from __future__ import annotations

import asyncio
import json
import logging
import posixpath
import tempfile
from dataclasses import dataclass, field
from pathlib import Path
from typing import TYPE_CHECKING, Any

import obstore as obs

from portolan_cli.async_utils import (
    CircuitBreaker,
    CircuitBreakerError,
    get_default_concurrency,
)
from portolan_cli.catalog import intermediate_catalog_ids
from portolan_cli.sync.download import (
    download_file,
    get_remote_file_size_async,
)
from portolan_cli.sync.report import Report, silent
from portolan_cli.sync.upload import _setup_store_and_kwargs, parse_object_store_url
from portolan_core.checksums import compute_checksum
from portolan_core.json_io import write_json_atomic
from portolan_core.run_state import stat_matches
from portolan_core.stac_links import SIDE_FILES, published_files
from portolan_core.trace import count, span
from portolan_core.versions import (
    VersionsFile,
    _parse_versions_file,
    read_versions,
    write_versions,
)

logger = logging.getLogger(__name__)

if TYPE_CHECKING:
    from obstore.store import (
        AzureStore,
        GCSStore,
        HTTPStore,
        LocalStore,
        MemoryStore,
        S3Store,
    )

    ObjectStore = S3Store | GCSStore | AzureStore | HTTPStore | LocalStore | MemoryStore

    from portolan_cli.version_ops import BackendPullResult

_MAX_FILES_LISTED = 5


# Default concurrency for async downloads (imported from async_utils)
DEFAULT_CONCURRENCY = get_default_concurrency()


# Exceptions


class PullError(Exception):
    """Base exception for pull operations."""


# Data Classes


@dataclass
class PullResult:
    """Result of a pull operation.

    Attributes:
        success: True if pull completed successfully.
        files_downloaded: Number of files downloaded.
        files_skipped: Number of files skipped (already up to date).
        local_version: Local version before pull.
        remote_version: Remote version (None in dry-run mode since no network check).
        uncommitted_changes: List of files with uncommitted changes that blocked pull.
        up_to_date: True if already at remote version (nothing to pull).
        dry_run: True if this was a dry-run operation (no network calls made).
        files_restored: Number of files restored (re-downloaded because missing locally).
    """

    success: bool
    files_downloaded: int
    files_skipped: int
    local_version: str | None
    remote_version: str | None
    uncommitted_changes: list[str] = field(default_factory=list)
    up_to_date: bool = False
    dry_run: bool = False
    files_restored: int = 0

    def to_dict(self) -> dict[str, Any]:
        """Return the envelope ``data`` of ``portolan pull --collection``."""
        return {
            "files_downloaded": self.files_downloaded,
            "files_skipped": self.files_skipped,
            "files_restored": self.files_restored,
            "local_version": self.local_version,
            "remote_version": self.remote_version,
            "up_to_date": self.up_to_date,
        }


@dataclass(frozen=True)
class BackendPull:
    """A pull of one collection that a non-file versioning backend ran."""

    result: PullResult

    def to_dict(self) -> dict[str, Any]:
        """Return the envelope ``data`` of the backend pull."""
        data = self.result.to_dict()
        del data["files_restored"]
        return data


@dataclass
class VersionDiff:
    """Diff between local and remote versions."""

    local_version: str | None
    remote_version: str | None
    is_behind: bool
    files_to_download: list[str]
    remote_assets: dict[str, dict[str, str | int]] = field(default_factory=dict)
    is_local_ahead: bool = False
    is_diverged: bool = False
    local_only_versions: list[str] = field(default_factory=list)
    remote_only_versions: list[str] = field(default_factory=list)


@dataclass
class PullAllResult:
    """Result of a catalog-wide pull operation."""

    success: bool
    total_collections: int
    successful_collections: int
    failed_collections: int
    total_files_downloaded: int
    collection_errors: dict[str, list[str]] = field(default_factory=dict)

    def to_dict(self) -> dict[str, Any]:
        """Return the envelope ``data`` of ``portolan pull`` for the whole catalog."""
        return {
            "total_collections": self.total_collections,
            "successful_collections": self.successful_collections,
            "failed_collections": self.failed_collections,
            "total_files_downloaded": self.total_files_downloaded,
            "collection_errors": self.collection_errors,
        }


# Change Detection


def detect_uncommitted_changes(
    catalog_root: Path,
    collection: str,
) -> list[str]:
    """Detect local files that differ from what versions.json says they should be."""
    # versions.json at collection root
    versions_path = catalog_root / collection / "versions.json"

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

        # Fast path: the size and mtime match the record, so the file is unchanged.
        if stat_matches(local_path.stat(), asset.mtime, asset.size_bytes):
            continue

        # Slow path: compute checksum (either mtime not recorded or mtime/size changed)
        actual_checksum = compute_checksum(local_path)
        if actual_checksum != asset.sha256:
            uncommitted.append(asset_name)

    return uncommitted


def find_missing_files(
    catalog_root: Path,
    collection: str,
) -> list[str]:
    """Find files that are tracked in versions.json but missing from disk."""
    # versions.json at collection root
    versions_path = catalog_root / collection / "versions.json"

    try:
        versions_file = read_versions(versions_path)
    except FileNotFoundError:
        # No versions.json means no tracked files, nothing to restore
        return []

    if not versions_file.versions:
        return []

    # Get the current version's assets
    current_version = versions_file.versions[-1]
    missing = []

    for asset_name, asset in current_version.assets.items():
        # Determine local file path (href is relative to catalog root)
        local_path = catalog_root / asset.href

        if not local_path.exists():
            missing.append(asset_name)

    return missing


# Version Diffing


def diff_versions(
    local_versions: VersionsFile,
    remote_versions: VersionsFile,
) -> VersionDiff:
    """Diff local and remote versions to determine what needs downloading."""
    local_version = local_versions.current_version
    remote_version = remote_versions.current_version

    # Build version sets for comparison
    local_version_strs = {v.version for v in local_versions.versions}
    remote_version_strs = {v.version for v in remote_versions.versions}

    local_only = local_version_strs - remote_version_strs
    remote_only = remote_version_strs - local_version_strs

    # Determine sync state
    is_local_ahead = bool(local_only) and not remote_only
    is_diverged = bool(local_only) and bool(remote_only)
    is_behind = bool(remote_only) and not local_only

    # Get remote's current assets (always populate for restore support)
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

    # Download new files and files whose checksum differs.
    files_to_download = []
    for name, remote_asset in remote_assets.items():
        if name not in local_assets or local_assets[name] != remote_asset["sha256"]:
            files_to_download.append(name)

    return VersionDiff(
        local_version=local_version,
        remote_version=remote_version,
        is_behind=is_behind,
        files_to_download=files_to_download,
        remote_assets=remote_assets,
        is_local_ahead=is_local_ahead,
        is_diverged=is_diverged,
        local_only_versions=sorted(local_only),
        remote_only_versions=sorted(remote_only),
    )


# Remote Fetch


def fetch_remote_versions(
    remote_url: str,
    collection: str,
    profile: str | None = None,
    *,
    report: Report | None = None,
) -> VersionsFile:
    """Fetch remote versions.json."""
    # Build remote versions.json path
    remote_versions_url = f"{remote_url.rstrip('/')}/{collection}/versions.json"

    # Download to temp file
    with tempfile.NamedTemporaryFile(suffix=".json", delete=False) as tmp:
        tmp_path = Path(tmp.name)

    try:
        result = download_file(
            source=remote_versions_url,
            destination=tmp_path,
            profile=profile,
            report=report,
        )

        if not result.success:
            raise PullError(f"Failed to fetch remote versions.json: {result.errors}")

        # Parse the downloaded file
        return read_versions(tmp_path)

    finally:
        # Clean up temp file
        if tmp_path.exists():
            tmp_path.unlink()


def _validate_safe_path(local_root: Path, href: str) -> Path:
    """Validate that href resolves to a path within local_root."""
    import os

    # Reject absolute paths
    if href.startswith("/") or (len(href) > 1 and href[1] == ":"):  # Unix or Windows abs
        raise ValueError(f"Absolute href not allowed: {href}")

    # Normalize and check for path traversal sequences
    safe_href = Path(href).as_posix().lstrip("/")
    if ".." in safe_href.split("/"):
        raise ValueError(f"Invalid href with path traversal: {href}")

    # Resolve and verify containment
    resolved_path = (local_root / safe_href).resolve()
    root_resolved = local_root.resolve()

    # Check that resolved path is within root (or IS root for edge cases)
    if not (
        str(resolved_path).startswith(str(root_resolved) + os.sep) or resolved_path == root_resolved
    ):
        raise ValueError(f"href escapes catalog root: {href}")

    return resolved_path


def _download_assets(
    remote_url: str,
    local_root: Path,
    files_to_download: list[str],
    remote_assets: dict[str, dict[str, str | int]],
    profile: str | None = None,
    dry_run: bool = False,
    report: Report = silent,
) -> tuple[int, int]:
    """Download assets from remote."""
    downloaded = 0
    failed = 0
    total = len(files_to_download)

    for i, filename in enumerate(files_to_download, 1):
        asset = remote_assets.get(filename)
        if not asset:
            continue

        # Build URLs
        href = str(asset["href"])

        # Security: Validate path before any filesystem operations
        local_path = _validate_safe_path(local_root, href)

        remote_asset_url = f"{remote_url.rstrip('/')}/{href}"

        if dry_run:
            report("info", f"[DRY RUN] Would download ({i}/{total}): {filename}")
            continue

        # Progress reporting for large downloads
        report("info", f"Downloading ({i}/{total}): {filename}")

        # Ensure parent directory exists
        local_path.parent.mkdir(parents=True, exist_ok=True)

        result = download_file(
            source=remote_asset_url,
            destination=local_path,
            profile=profile,
            overwrite=True,
            report=report,
        )

        if result.success:
            downloaded += 1
        else:
            failed += 1

    return downloaded, failed


# Pre-Pull Validation


def _check_sync_state_conflicts(
    diff: VersionDiff,
    force: bool,
    report: Report = silent,
) -> PullResult | None:
    """Check for local-ahead or diverged states that would cause data loss."""
    if force:
        return None

    if diff.is_local_ahead:
        report("warn", "Local has unpushed versions that would be lost:")
        for v in diff.local_only_versions:
            report("detail", f" {v}")
        report("warn", "Push your changes first, or use --force to discard them")
        return PullResult(
            success=False,
            files_downloaded=0,
            files_skipped=0,
            local_version=diff.local_version,
            remote_version=diff.remote_version,
            uncommitted_changes=diff.local_only_versions,
        )

    if diff.is_diverged:
        report("warn", "Local and remote have diverged:")
        report("warn", f" Local-only versions: {diff.local_only_versions}")
        report("warn", f" Remote-only versions: {diff.remote_only_versions}")
        report("warn", "Resolve by pushing --force or pulling --force (will lose data)")
        return PullResult(
            success=False,
            files_downloaded=0,
            files_skipped=0,
            local_version=diff.local_version,
            remote_version=diff.remote_version,
            uncommitted_changes=diff.local_only_versions,
        )

    return None


def _check_uncommitted_conflicts(
    local_root: Path,
    collection: str,
    diff: VersionDiff,
    force: bool,
    report: Report = silent,
) -> PullResult | None:
    """Check for uncommitted local changes that would be overwritten."""
    if force:
        return None

    uncommitted = detect_uncommitted_changes(local_root, collection)
    conflicts = [f for f in uncommitted if f in diff.files_to_download]

    if conflicts:
        report("warn", "Uncommitted local changes would be overwritten:")
        for filename in conflicts:
            report("detail", f" {filename}")
        report("warn", "Use --force to discard local changes")

        return PullResult(
            success=False,
            files_downloaded=0,
            files_skipped=0,
            local_version=diff.local_version,
            remote_version=diff.remote_version,
            uncommitted_changes=conflicts,
        )

    return None


def _report_pull_plan(
    remote_url: str,
    update_files: list[str],
    restore_files: list[str],
    concurrency: int,
    report: Report = silent,
) -> None:
    """Report what will be downloaded during a pull operation."""
    update_count = len(update_files)
    restore_count = len(restore_files)
    all_files = update_files + restore_files

    if restore_count > 0 and update_count > 0:
        report("info", f"Pulling {update_count} updated file(s) + {restore_count} missing file(s)")
    elif restore_count > 0:
        report("info", f"Restoring {restore_count} missing file(s) from {remote_url}")
    else:
        report("info", f"Pulling {update_count} file(s) from {remote_url}")

    report("info", f"Using concurrency: {concurrency}")
    for filename in all_files[:_MAX_FILES_LISTED]:
        report("detail", f" {filename}")
    if len(all_files) > _MAX_FILES_LISTED:
        report("detail", f" ... and {len(all_files) - _MAX_FILES_LISTED} more")


# Async Download Primitives


async def _download_file_async(
    store: ObjectStore,
    remote_key: str,
    local_path: Path,
) -> tuple[bool, int]:
    """Download a single file asynchronously."""
    # Use a temp file to avoid corrupting existing files on failure
    temp_path = local_path.with_name(local_path.name + ".part")

    try:
        # Ensure parent directory exists
        local_path.parent.mkdir(parents=True, exist_ok=True)

        # Use obstore's async get
        response = await obs.get_async(store, remote_key)
        content = await response.bytes_async()

        # Write to temp file first
        temp_path.write_bytes(content)

        # Atomically replace target with temp file
        temp_path.replace(local_path)

        return True, len(content)
    except Exception:
        # Clean up temp file only (preserve original if it exists)
        if temp_path.exists():
            temp_path.unlink()
        raise


async def _fetch_remote_versions_async(
    remote_url: str,
    collection: str,
    profile: str | None = None,
    *,
    report: Report = silent,
) -> VersionsFile:
    """Fetch remote versions.json asynchronously."""
    # Build remote versions.json path
    import json

    remote_versions_url = f"{remote_url.rstrip('/')}/{collection}/versions.json"
    bucket_url, prefix = parse_object_store_url(remote_versions_url)

    # Setup store (chunk_concurrency=4 matches download.py default)
    store, _kwargs = _setup_store_and_kwargs(
        bucket_url, profile, chunk_concurrency=4, report=report
    )

    try:
        # Fetch using async get
        response = await obs.get_async(store, prefix)
        content = await response.bytes_async()

        # Parse JSON content using canonical parser for validation
        # content is obstore.Bytes, convert to bytes then decode
        count("versions_json.reads")
        data = json.loads(bytes(content).decode("utf-8"))

        # Use canonical parser (validates schema and required fields)
        return _parse_versions_file(data)
    except ValueError as e:
        # Re-raise validation errors from parser with better context
        raise PullError(f"Invalid remote versions.json: {e}") from e
    except Exception as e:
        raise PullError(f"Failed to fetch remote versions.json: {e}") from e


async def _download_assets_async(  # noqa: PLR0915 - one ordered transport sequence
    store: ObjectStore,
    prefix: str,
    local_root: Path,
    files_to_download: list[str],
    remote_assets: dict[str, dict[str, str | int]],
    concurrency: int = DEFAULT_CONCURRENCY,
    *,
    report: Report = silent,
    # The library draws the bar only when the caller asks. Moving the renderer
    # to the CLI is a follow-up.
    show_progress: bool = False,
) -> tuple[int, int]:
    """Download assets concurrently using asyncio."""
    from portolan_cli.async_utils import AsyncProgressReporter

    if not files_to_download:
        return 0, 0

    # Calculate total bytes for progress bar
    total_bytes = sum(int(remote_assets.get(f, {}).get("size_bytes", 0)) for f in files_to_download)

    # Semaphore for concurrency control
    semaphore = asyncio.Semaphore(concurrency)
    circuit_breaker = CircuitBreaker(failure_threshold=10)

    downloaded = 0
    failed = 0
    total = len(files_to_download)

    async with AsyncProgressReporter(
        total_files=total,
        total_bytes=total_bytes,
        json_mode=not show_progress,
        description="Downloading",
    ) as progress:

        async def download_one(filename: str) -> tuple[str, bool, str | None, int]:
            """Download a single file with semaphore control."""
            asset = remote_assets.get(filename)
            if not asset:
                return filename, False, "Asset metadata not found", 0

            href = str(asset["href"])
            file_size = int(asset.get("size_bytes", 0))

            # Security: Validate path before any filesystem operations
            try:
                local_path = _validate_safe_path(local_root, href)
            except ValueError as e:
                return filename, False, str(e), 0

            # Construct remote_key by joining prefix and href
            # Strip leading/trailing slashes for proper normalization
            remote_key = f"{prefix.strip('/')}/{href.lstrip('/')}" if prefix else href.lstrip("/")

            async with semaphore:
                # Check circuit breaker AFTER acquiring semaphore (execution time, not scheduling time)
                try:
                    circuit_breaker.check()
                except CircuitBreakerError:
                    return filename, False, "Circuit breaker open - too many failures", 0

                # Retry logic for rate limits
                max_retries = 3
                retry_delay = 1.0

                for attempt in range(max_retries):
                    try:
                        await _download_file_async(store, remote_key, local_path)
                        circuit_breaker.record_success()
                        logger.debug("Downloaded: %s", filename)
                        return filename, True, None, file_size
                    except Exception as e:
                        error_str = str(e)
                        if "429" in error_str and attempt < max_retries - 1:
                            # Rate limited - wait and retry
                            await asyncio.sleep(retry_delay * (attempt + 1))
                            continue
                        circuit_breaker.record_failure()
                        return filename, False, error_str, 0

                return filename, False, "Max retries exceeded", 0

        # Create tasks for all downloads
        tasks = [download_one(filename) for filename in files_to_download]

        # Execute all tasks concurrently
        results = await asyncio.gather(*tasks, return_exceptions=True)

        # Process results
        for result in results:
            if isinstance(result, BaseException):
                failed += 1
                report("error", f"Download error: {result}")
                progress.advance(0)
            elif isinstance(result, tuple):
                filename, success_flag, err_msg, bytes_downloaded = result
                if success_flag:
                    downloaded += 1
                    progress.advance(bytes_downloaded)
                else:
                    failed += 1
                    progress.advance(0)
                    if err_msg:
                        report("error", f"Failed {filename}: {err_msg}")

    return downloaded, failed


# Populate Missing File Sizes


def _resolve_asset_url(href: str, base_url: str, rel_path: str = "") -> str:
    """Resolve an asset href to a full URL for HTTP HEAD requests."""
    if href.startswith(("http://", "https://")):
        return href
    if href.startswith("../"):
        parent = "/".join(rel_path.split("/")[:-1]) if "/" in rel_path else ""
        return f"{base_url}/{parent}/{href[3:]}" if parent else f"{base_url}/{href[3:]}"
    if href.startswith("./"):
        return f"{base_url}/{rel_path}/{href[2:]}" if rel_path else f"{base_url}/{href[2:]}"
    return f"{base_url}/{rel_path}/{href}" if rel_path else f"{base_url}/{href}"


async def _fetch_and_set_file_size(
    asset: dict[str, Any],
    asset_url: str,
) -> bool:
    """Fetch file size via HTTP HEAD and set on asset if successful."""
    size = await get_remote_file_size_async(asset_url)
    if size is not None:
        asset["file:size"] = size
        return True
    return False


async def _populate_missing_file_sizes(
    local_root: Path,
    collection: str,
    remote_url: str,
) -> int:
    """Populate missing file:size values in STAC assets via HTTP HEAD."""
    collection_dir = local_root / collection
    collection_json = collection_dir / "collection.json"

    if not collection_json.exists():
        return 0

    populated = 0
    base_url = f"{remote_url}/{collection}"

    # Process collection.json assets
    populated += await _process_stac_file_sizes(collection_json, base_url, "")

    # Process item.json files
    for item_json in collection_dir.rglob("*.json"):
        if item_json.name in ("collection.json", "versions.json"):
            continue
        rel_path = item_json.parent.relative_to(local_root).as_posix()
        item_base = f"{remote_url}/{rel_path}"
        # item_base already includes rel_path, so pass "" to avoid URL duplication
        populated += await _process_stac_file_sizes(item_json, item_base, "")

    return populated


async def _enrich_file_sizes_safe(
    local_root: Path,
    collection: str,
    remote_url: str,
) -> None:
    """Safely enrich STAC assets with file:size metadata."""
    try:
        sizes_populated = await _populate_missing_file_sizes(
            local_root=local_root,
            collection=collection,
            remote_url=remote_url,
        )
        logger.debug("Populated file:size for %d asset(s)", sizes_populated)
    except Exception:
        # File sizes are optional. Record why the enrichment stopped.
        logger.debug("Could not populate file:size for %s", collection, exc_info=True)


async def _process_stac_file_sizes(
    stac_file: Path,
    base_url: str,
    rel_path: str,
) -> int:
    """Process a single STAC file and populate missing file:size values."""
    import json

    try:
        data = json.loads(stac_file.read_text(encoding="utf-8"))
    except (json.JSONDecodeError, OSError):
        return 0

    # Skip non-STAC files
    stac_type = data.get("type")
    if stac_type not in ("Collection", "Feature"):
        return 0

    assets = data.get("assets", {})
    if not assets:
        return 0

    populated = 0
    modified = False

    for asset_key, asset in assets.items():
        if "file:size" in asset:
            continue
        href = asset.get("href", "")
        asset_url = _resolve_asset_url(href, base_url, rel_path)

        if await _fetch_and_set_file_size(asset, asset_url):
            modified = True
            populated += 1
            logger.debug("Fetched file:size for %s:%s", stac_file.name, asset_key)

    if modified:
        write_json_atomic(stac_file, data)

    return populated


# Async Pull Function


async def pull_async(
    remote_url: str,
    local_root: Path,
    collection: str,
    *,
    force: bool = False,
    dry_run: bool = False,
    restore: bool = False,
    profile: str | None = None,
    concurrency: int = DEFAULT_CONCURRENCY,
    store: ObjectStore | None = None,
    report: Report = silent,
    show_progress: bool = False,
) -> PullResult:
    """Pull updates from a remote catalog asynchronously."""
    # Validate URL and parse components
    try:
        bucket_url, base_prefix = parse_object_store_url(remote_url)
    except ValueError as e:
        raise ValueError(f"Invalid remote URL: {e}") from e

    # Create store if not provided (connection pooling support)
    if store is None:
        store, _kwargs = _setup_store_and_kwargs(
            bucket_url, profile, chunk_concurrency=4, report=report
        )

    # Path to local versions.json
    versions_path = local_root / collection / "versions.json"

    # Load local versions (may not exist yet)
    try:
        local_versions = read_versions(versions_path)
    except FileNotFoundError:
        local_versions = VersionsFile(
            spec_version="1.0.0",
            current_version=None,
            versions=[],
        )

    # dry-run must not make any network calls.
    if dry_run:
        report("info", f"[DRY RUN] Would pull from {remote_url}/{collection}")
        report("info", f"[DRY RUN] Local version: {local_versions.current_version or '(none)'}")
        report("warn", "[DRY RUN] Remote conflict detection skipped (requires network)")
        return PullResult(
            success=True,
            files_downloaded=0,
            files_skipped=0,
            local_version=local_versions.current_version,
            remote_version=None,
            dry_run=True,
        )

    # Fetch remote versions asynchronously
    try:
        with span("pull.fetch_remote", collection=collection):
            remote_versions = await _fetch_remote_versions_async(
                remote_url, collection, profile, report=report
            )
    except PullError as e:
        report("error", f"Failed to fetch remote: {e}")
        return PullResult(
            success=False,
            files_downloaded=0,
            files_skipped=0,
            local_version=local_versions.current_version,
            remote_version=None,
        )

    # Diff versions
    diff = diff_versions(local_versions, remote_versions)

    # Handle restore mode: check for missing files even if versions match
    files_to_restore: list[str] = []
    if restore:
        missing_files = find_missing_files(local_root, collection)
        # Only restore files that aren't already in the diff's download list
        files_to_restore = [f for f in missing_files if f not in diff.files_to_download]

    # Check if up to date (considering restore mode)
    versions_match = not diff.is_behind and not diff.is_local_ahead and not diff.is_diverged
    if versions_match and not files_to_restore:
        report("info", "Already up to date")
        return PullResult(
            success=True,
            files_downloaded=0,
            files_skipped=0,
            local_version=diff.local_version,
            remote_version=diff.remote_version,
            up_to_date=True,
        )

    # Merge restore files into download list
    all_files_to_download = diff.files_to_download + files_to_restore

    # Data loss prevention checks
    sync_conflict = _check_sync_state_conflicts(diff, force, report)
    if sync_conflict is not None:
        return sync_conflict

    uncommitted_conflict = _check_uncommitted_conflicts(local_root, collection, diff, force, report)
    if uncommitted_conflict is not None:
        return uncommitted_conflict

    # Report what will be downloaded
    restore_count = len(files_to_restore)
    _report_pull_plan(remote_url, diff.files_to_download, files_to_restore, concurrency, report)

    # Download assets concurrently (reuses store connection)
    with span(
        "pull.download_assets", collection=collection, files=len(all_files_to_download)
    ) as stage:
        downloaded, failed = await _download_assets_async(
            store=store,
            prefix=base_prefix,
            local_root=local_root,
            files_to_download=all_files_to_download,
            remote_assets=diff.remote_assets,
            concurrency=concurrency,
            report=report,
            show_progress=show_progress,
        )
        stage.set(downloaded=downloaded, failed=failed)

    # Handle failures
    if failed > 0:
        report("error", f"Failed to download {failed} file(s)")
        return PullResult(
            success=False,
            files_downloaded=downloaded,
            files_skipped=0,
            local_version=diff.local_version,
            remote_version=diff.remote_version,
            files_restored=min(downloaded, restore_count),
        )

    # Populate missing file:size values via HTTP HEAD
    with span("pull.populate_sizes", collection=collection):
        await _enrich_file_sizes_safe(
            local_root=local_root,
            collection=collection,
            remote_url=remote_url,
        )

    # Update local versions.json (only if version actually changed)
    versions_path.parent.mkdir(parents=True, exist_ok=True)
    if diff.remote_version != diff.local_version:
        write_versions(versions_path, remote_versions)
        report("success", f"Updated to version {diff.remote_version}")
    elif restore_count > 0:
        report("success", f"Restored {restore_count} missing file(s)")

    return PullResult(
        success=True,
        files_downloaded=downloaded,
        files_skipped=len(all_files_to_download) - downloaded,
        local_version=diff.local_version,
        remote_version=diff.remote_version,
        files_restored=restore_count,
    )


# Main Pull Function (Sync Wrapper)


def pull(
    remote_url: str,
    local_root: Path,
    collection: str,
    *,
    force: bool = False,
    dry_run: bool = False,
    restore: bool = False,
    profile: str | None = None,
    concurrency: int = DEFAULT_CONCURRENCY,
    stac_tree: bool = False,
    report: Report = silent,
    show_progress: bool = False,
) -> PullResult:
    """Pull updates from a remote catalog (sync wrapper)."""
    # A push can land between the asset and tree downloads. A changed remote
    # version restarts the clone of the collection.
    attempt = 1
    while True:
        result = asyncio.run(
            pull_async(
                remote_url=remote_url,
                local_root=local_root,
                collection=collection,
                force=force,
                dry_run=dry_run,
                restore=restore,
                profile=profile,
                concurrency=concurrency,
                report=report,
                show_progress=show_progress,
            )
        )
        if not stac_tree or not result.success:
            return result
        written, failed, changed = asyncio.run(
            _download_stac_tree(
                remote_url,
                local_root,
                collection,
                profile,
                concurrency,
                expected_version=result.remote_version,
                report=report,
            )
        )
        if not changed:
            break
        report(
            "warn",
            f"The remote published a new version of '{collection}' during the clone. "
            f"Downloading it ({attempt}/{_CLONE_ATTEMPTS}).",
        )
        if attempt == _CLONE_ATTEMPTS:
            result.success = False
            report(
                "error",
                f"The remote of '{collection}' changed during each of {_CLONE_ATTEMPTS} attempts",
            )
            return result
        attempt += 1
    result.files_downloaded += written
    if failed:
        result.success = False
        report("error", f"Failed to download {failed} file(s) of '{collection}'")
    return result


# How many times clone starts a collection again when the remote changes under it.
_CLONE_ATTEMPTS = 3


async def _download_stac_tree(
    remote_url: str,
    local_root: Path,
    collection: str,
    profile: str | None,
    concurrency: int,
    *,
    expected_version: str | None = None,
    report: Report = silent,
) -> tuple[int, int, bool]:
    """Download the STAC tree of ``collection`` and the objects it names."""
    if expected_version is not None:
        remote = await _fetch_remote_versions_async(remote_url, collection, profile, report=report)
        if remote.current_version != expected_version:
            return 0, 0, True
    bucket_url, prefix = parse_object_store_url(remote_url)
    store, _ = _setup_store_and_kwargs(bucket_url, profile, chunk_concurrency=4, report=report)
    semaphore = asyncio.Semaphore(concurrency)
    counts = {"written": 0, "failed": 0}
    walked: set[str] = set()

    async def fetch(rel: str) -> bool:
        """Download one object. A missing object is skipped."""
        async with semaphore:
            try:
                target = _validate_safe_path(local_root, rel)
                await _download_file_async(store, f"{prefix}/{rel}".lstrip("/"), target)
            except FileNotFoundError:
                return False
            except Exception as e:
                report("error", f"Failed to download {rel}: {e}")
                counts["failed"] += 1
                return False
        counts["written"] += 1
        return True

    async def walk(document_rel: str) -> None:
        """Download one STAC document, the files it names, and its items."""
        if document_rel in walked:
            return
        walked.add(document_rel)
        if not await fetch(document_rel):
            return
        document = json.loads((local_root / document_rel).read_text(encoding="utf-8"))
        files, items = published_files(document, document_rel)
        # pull already wrote the assets that versions.json names. clone wrote
        # its own README.md and AGENTS.md, which the remote ones replace.
        files = {
            rel
            for rel in files
            if posixpath.basename(rel) in SIDE_FILES or not (local_root / rel).exists()
        }
        await asyncio.gather(
            *(fetch(rel) for rel in sorted(files)),
            *(walk(rel) for rel in sorted(items)),
        )

    documents = [
        posixpath.join(c, "catalog.json") for c in ["", *intermediate_catalog_ids(collection)]
    ]
    await asyncio.gather(
        fetch("versions.json"),
        fetch(f"{collection}/versions.json"),
        *(walk(rel) for rel in [*documents, f"{collection}/collection.json"]),
    )
    return counts["written"], counts["failed"], False


# Catalog-wide Pull (Async)


def _report_one_pull_result(
    result: object,
    coll: str,
    index: int,
    total: int,
    collection_errors: dict[str, list[str]],
    report: Report = silent,
) -> int | None:
    """Print the outcome of one collection pull and record any errors."""
    prefix = f"[{index + 1}/{total}]"

    if isinstance(result, BaseException):
        report("error", f"{prefix} Failed {coll}: {result}")
        collection_errors[coll] = [str(result)]
        return None

    if not isinstance(result, tuple):
        report("error", f"{prefix} Failed {coll}: Unknown error")
        collection_errors[coll] = ["Unknown error"]
        return None

    coll, pull_result, err_msg = result
    if err_msg:
        report("error", f"{prefix} Failed {coll}: {err_msg}")
        collection_errors[coll] = [err_msg]
        return None

    if pull_result is None:
        report("error", f"{prefix} Failed {coll}: Unknown error")
        collection_errors[coll] = ["Unknown error"]
        return None

    if pull_result.success:
        if pull_result.up_to_date:
            report("success", f"{prefix} {coll}: Already up to date")
        else:
            report("success", f"{prefix} Pulled {coll}: {pull_result.files_downloaded} file(s)")
        return int(pull_result.files_downloaded)

    if pull_result.uncommitted_changes:
        errors_list = [f"Uncommitted changes: {', '.join(pull_result.uncommitted_changes)}"]
    else:
        errors_list = ["Pull failed"]
    report("error", f"{prefix} Failed {coll}: {', '.join(errors_list)}")
    collection_errors[coll] = errors_list
    return None


async def pull_all_collections_async(  # noqa: PLR0915 - one ordered transport sequence
    remote_url: str,
    local_root: Path,
    *,
    force: bool = False,
    dry_run: bool = False,
    restore: bool = False,
    profile: str | None = None,
    concurrency: int | None = None,
    file_concurrency: int | None = None,
    report: Report = silent,
    show_progress: bool = False,
) -> PullAllResult:
    """Pull all collections in a catalog from cloud storage asynchronously."""
    # Import here to avoid circular dependency
    from portolan_cli.sync.push import discover_collections
    from portolan_cli.version_ops import try_backend_pull
    from portolan_core.run_state import RunState
    from portolan_core.stac_links import catalog_collections

    # Validate URL and create shared store (connection pooling)
    try:
        bucket_url, _prefix = parse_object_store_url(remote_url)
    except ValueError as e:
        raise ValueError(f"Invalid remote URL: {e}") from e

    shared_store: ObjectStore | None = None

    # discover_collections validates catalog and raises ValueError if invalid
    collections = discover_collections(local_root, report=report)
    state = RunState.open(local_root)
    # Plugin history need not have a local versions.json. Keep file discovery
    # intact and include plugin collections from the STAC tree as well.
    collections = sorted(
        set(collections)
        | {
            c
            for c in catalog_collections(local_root)
            if state.backend(state.collection_dir(c)) != "file"
        }
    )
    total = len(collections)

    if total == 0:
        report("warn", "No initialized collections found in catalog")
        report("warn", "Collections need a versions.json file to be pullable")
        return PullAllResult(
            success=True,  # Empty catalog is not a failure, just nothing to do
            total_collections=0,
            successful_collections=0,
            failed_collections=0,
            total_files_downloaded=0,
        )

    report("info", f"Found {total} collection(s) to pull")

    # Track aggregate stats
    successful = 0
    failed = 0
    total_files = 0
    collection_errors: dict[str, list[str]] = {}

    # Determine concurrency
    max_concurrent = concurrency if concurrency is not None else min(total, DEFAULT_CONCURRENCY)
    max_concurrent = min(max_concurrent, total)  # Don't exceed collection count

    report("info", f"Using concurrency: {max_concurrent}")

    # Semaphore for collection-level concurrency control
    semaphore = asyncio.Semaphore(max_concurrent)

    async def pull_one(collection: str) -> tuple[str, PullResult | None, str | None]:
        """Pull a single collection with semaphore control."""
        nonlocal shared_store
        async with semaphore:
            try:
                # A plugin pull is synchronous. A thread keeps the other
                # collections downloading while it runs.
                backend_result = await asyncio.to_thread(
                    try_backend_pull, local_root, remote_url, collection, dry_run, state=state
                )
                if backend_result.handled:
                    return collection, _from_backend(backend_result, dry_run), None
                if shared_store is None and not dry_run:
                    shared_store, _kwargs = _setup_store_and_kwargs(
                        bucket_url, profile, chunk_concurrency=4, report=report
                    )
                # Build kwargs, only including concurrency if explicitly set
                pull_kwargs: dict[str, Any] = {
                    "remote_url": remote_url,
                    "local_root": local_root,
                    "collection": collection,
                    "force": force,
                    "dry_run": dry_run,
                    "restore": restore,
                    "profile": profile,
                    "store": shared_store,  # Reuse connection across collections
                    "report": report,
                    "show_progress": show_progress,
                }
                if file_concurrency is not None:
                    pull_kwargs["concurrency"] = file_concurrency

                result = await pull_async(**pull_kwargs)
                return (collection, result, None)
            except Exception as e:
                return (collection, None, f"{type(e).__name__}: {e}")

    # Run all collection pulls concurrently
    tasks = [pull_one(coll) for coll in collections]
    results = await asyncio.gather(*tasks, return_exceptions=True)

    for i, result in enumerate(results):
        files = _report_one_pull_result(result, collections[i], i, total, collection_errors, report)
        if files is None:
            failed += 1
        else:
            successful += 1
            total_files += files

    # Summary report
    report("info", f"\n{'=' * 60}")
    if failed == 0:
        report("success", f"Pulled {successful} collection(s), {total_files} file(s) total")
    else:
        report("warn", f"Completed with errors: {successful} succeeded, {failed} failed")
        for collection, errors in collection_errors.items():
            report("warn", f" {collection}: {', '.join(errors)}")

    return PullAllResult(
        success=(failed == 0),
        total_collections=total,
        successful_collections=successful,
        failed_collections=failed,
        total_files_downloaded=total_files,
        collection_errors=collection_errors,
    )


def pull_all_collections(
    remote_url: str,
    local_root: Path,
    *,
    force: bool = False,
    dry_run: bool = False,
    restore: bool = False,
    profile: str | None = None,
    workers: int | None = None,
    file_concurrency: int | None = None,
    report: Report = silent,
    show_progress: bool = False,
) -> PullAllResult:
    """Pull all collections in a catalog from cloud storage (sync wrapper)."""
    return asyncio.run(
        pull_all_collections_async(
            remote_url=remote_url,
            local_root=local_root,
            force=force,
            dry_run=dry_run,
            restore=restore,
            profile=profile,
            concurrency=workers,
            file_concurrency=file_concurrency,
            report=report,
            show_progress=show_progress,
        )
    )


def _from_backend(backend_result: BackendPullResult, dry_run: bool) -> PullResult:
    """Return the PullResult of a pull that a non-file backend ran."""
    return PullResult(
        success=backend_result.success,
        files_downloaded=backend_result.files_downloaded,
        files_skipped=backend_result.files_skipped,
        local_version=backend_result.local_version,
        remote_version=backend_result.remote_version,
        up_to_date=backend_result.up_to_date,
        dry_run=dry_run,
    )


def pull_catalog(
    remote_url: str,
    local_root: Path,
    collection: str | None = None,
    *,
    force: bool = False,
    dry_run: bool = False,
    restore: bool = False,
    profile: str | None = None,
    workers: int | None = None,
    concurrency: int = DEFAULT_CONCURRENCY,
    report: Report = silent,
    show_progress: bool = False,
) -> PullResult | PullAllResult | BackendPull:
    """Run ``portolan pull``: pull one collection, or each collection of the catalog."""
    if collection is None:
        return pull_all_collections(
            remote_url=remote_url,
            local_root=local_root,
            force=force,
            dry_run=dry_run,
            restore=restore,
            profile=profile,
            workers=workers,
            file_concurrency=concurrency,
            report=report,
            show_progress=show_progress,
        )

    from portolan_cli.version_ops import try_backend_pull

    backend_result = try_backend_pull(local_root, remote_url, collection, dry_run)
    if backend_result.handled:
        return BackendPull(_from_backend(backend_result, dry_run))
    return pull(
        remote_url=remote_url,
        local_root=local_root,
        collection=collection,
        force=force,
        dry_run=dry_run,
        restore=restore,
        profile=profile,
        concurrency=concurrency,
        report=report,
        show_progress=show_progress,
    )
