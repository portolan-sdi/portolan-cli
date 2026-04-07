"""Push module - sync local catalog changes to cloud object storage.

This module provides the push functionality for Portolan catalogs:
- Read local versions.json
- Fetch remote versions.json (with etag for optimistic locking)
- Diff: find local-only, remote-only, and common versions
- Detect conflicts (remote-only versions indicate divergence)
- Upload changed assets (manifest-last: assets first, then versions.json)
- Use etag-based optimistic locking for atomic updates

Design Principles:
- Manifest-last atomicity: Upload assets first, then versions.json last
- Optimistic locking: Use etag to detect concurrent modifications
- Explicit conflict handling: Fail on conflicts unless --force

See ADR-0005 for versions.json as single source of truth.
See ADR-0007 for CLI wraps Python API (all logic in library layer).
"""

from __future__ import annotations

import asyncio
import json
import threading
import time
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

import obstore as obs

from portolan_cli.async_utils import (
    AsyncIOExecutor,
    AsyncProgressReporter,
    CircuitBreakerError,
    get_default_concurrency,
)
from portolan_cli.output import detail, error, info, output_section, success, warn
from portolan_cli.upload import ObjectStore, setup_store
from portolan_cli.upload_progress import UploadProgressReporter

__all__ = [
    "get_default_workers",
    "push",
    "push_async",
    "push_all_collections",
    "discover_collections",
    "UploadMetrics",
    "format_file_size",
    "format_speed",
    "PushVersionDiff",
    "VersionDiff",  # Deprecated alias
]


def get_default_workers() -> int:
    """Get default number of workers for parallel operations.

    This is a backward-compatible wrapper around get_default_concurrency().
    New code should use get_default_concurrency() from async_utils instead.

    Returns:
        Default number of workers (same as concurrency default).
    """
    return get_default_concurrency()


# =============================================================================
# Exceptions
# =============================================================================


class PushConflictError(Exception):
    """Raised when push detects conflict with remote state.

    This occurs when:
    - Remote has versions not present locally (remote diverged)
    - Remote versions.json changed during push (etag mismatch)
    """

    pass


# =============================================================================
# Data Classes
# =============================================================================


@dataclass
class PushResult:
    """Result of a push operation.

    Attributes:
        success: True if push completed without errors or conflicts.
        files_uploaded: Number of asset files uploaded.
        versions_pushed: Number of new versions pushed (from versions.json).
        conflicts: List of conflict descriptions.
        errors: List of error messages.
        dry_run: True if this was a dry-run operation (no network calls made).
        would_push_versions: In dry-run mode, max versions that would be pushed
            (upper bound; actual count depends on remote state).
        metrics: Upload performance metrics (bytes, duration, speed).
    """

    success: bool
    files_uploaded: int
    versions_pushed: int
    conflicts: list[str] = field(default_factory=list)
    errors: list[str] = field(default_factory=list)
    dry_run: bool = False
    would_push_versions: int = 0
    metrics: UploadMetrics | None = None


@dataclass
class PushVersionDiff:
    """Result of diffing local vs remote versions for push operations.

    Attributes:
        local_only: Versions that exist only locally (to be pushed).
        remote_only: Versions that exist only remotely (conflict!).
        common: Versions that exist in both local and remote.
    """

    local_only: list[str]
    remote_only: list[str]
    common: list[str]

    @property
    def has_conflict(self) -> bool:
        """True if remote has versions not present locally."""
        return len(self.remote_only) > 0


# Alias for backwards compatibility (deprecated, use PushVersionDiff)
VersionDiff = PushVersionDiff


@dataclass
class UploadMetrics:
    """Tracks upload performance metrics for summary display.

    Thread-safe accumulator for upload statistics.

    Attributes:
        total_bytes: Sum of all uploaded file sizes.
        total_duration: Sum of individual task durations (for per-file stats only).
        file_count: Number of files uploaded.
        elapsed_seconds: Wall-clock time for the batch (used for average_speed).
    """

    total_bytes: int = 0
    total_duration: float = 0.0
    file_count: int = 0
    elapsed_seconds: float = 0.0
    _lock: threading.Lock = field(default_factory=threading.Lock, repr=False)
    _start_time: float | None = field(default=None, repr=False)

    def start_timer(self) -> None:
        """Start the wall-clock timer for this batch."""
        self._start_time = time.perf_counter()

    def stop_timer(self) -> None:
        """Stop the wall-clock timer and record elapsed time."""
        if self._start_time is not None:
            self.elapsed_seconds = time.perf_counter() - self._start_time
            self._start_time = None

    def record_elapsed(self, elapsed: float) -> None:
        """Record wall-clock elapsed time directly (thread-safe).

        Args:
            elapsed: Elapsed wall-clock time in seconds.
        """
        with self._lock:
            self.elapsed_seconds += elapsed

    def record(self, size_bytes: int, duration_seconds: float) -> None:
        """Record metrics for a single upload (thread-safe).

        Note: duration_seconds is per-file timing, not used for average_speed
        when elapsed_seconds is available (parallel uploads overlap).
        """
        with self._lock:
            self.total_bytes += size_bytes
            self.total_duration += duration_seconds
            self.file_count += 1

    @property
    def average_speed(self) -> float:
        """Average upload speed in bytes per second.

        Uses wall-clock elapsed_seconds when available (correct for parallel uploads),
        falls back to total_duration only if elapsed_seconds is not set.
        """
        # Prefer wall-clock time for accurate speed calculation
        if self.elapsed_seconds > 0:
            return self.total_bytes / self.elapsed_seconds
        # Fallback to total_duration (sum of individual task times)
        if self.total_duration == 0:
            return 0.0
        return self.total_bytes / self.total_duration

    def merge(self, other: UploadMetrics) -> None:
        """Merge metrics from another instance (thread-safe).

        Used to aggregate metrics across multiple collections.
        """
        with self._lock:
            self.elapsed_seconds += other.elapsed_seconds
            self.total_bytes += other.total_bytes
            self.total_duration += other.total_duration
            self.file_count += other.file_count


# =============================================================================
# Formatting Utilities
# =============================================================================


def format_file_size(size_bytes: int) -> str:
    """Format file size in human-readable form.

    Args:
        size_bytes: Size in bytes.

    Returns:
        Human-readable size string (e.g., "54.2 MB").
    """
    if size_bytes < 1024:
        return f"{size_bytes} B"
    elif size_bytes < 1024 * 1024:
        return f"{size_bytes / 1024:.1f} KB"
    elif size_bytes < 1024 * 1024 * 1024:
        return f"{size_bytes / (1024 * 1024):.1f} MB"
    else:
        return f"{size_bytes / (1024 * 1024 * 1024):.1f} GB"


def format_speed(bytes_per_second: float) -> str:
    """Format upload speed in human-readable form.

    Uses binary units (KiB, MiB, GiB) which are standard for network transfer rates.

    Args:
        bytes_per_second: Speed in bytes per second.

    Returns:
        Human-readable speed string (e.g., "10.5 MiB/s").
    """
    if bytes_per_second < 1024:
        return f"{int(bytes_per_second)} B/s"
    elif bytes_per_second < 1024 * 1024:
        return f"{bytes_per_second / 1024:.1f} KiB/s"
    elif bytes_per_second < 1024 * 1024 * 1024:
        return f"{bytes_per_second / (1024 * 1024):.1f} MiB/s"
    else:
        return f"{bytes_per_second / (1024 * 1024 * 1024):.1f} GiB/s"


# =============================================================================
# Version Diffing
# =============================================================================


def diff_version_lists(local_versions: list[str], remote_versions: list[str]) -> PushVersionDiff:
    """Compute diff between local and remote version string lists.

    This is a simple set-based diff for push operations, comparing version
    strings to determine what needs to be pushed.

    Note: pull.py has a separate diff_versions() that works with VersionsFile
    objects and computes files to download.

    Args:
        local_versions: List of version strings from local versions.json.
        remote_versions: List of version strings from remote versions.json.

    Returns:
        PushVersionDiff with local_only, remote_only, and common versions.
    """
    local_set = set(local_versions)
    remote_set = set(remote_versions)

    # Preserve order from original lists
    local_only = [v for v in local_versions if v not in remote_set]
    remote_only = [v for v in remote_versions if v not in local_set]
    common = [v for v in local_versions if v in remote_set]

    return PushVersionDiff(
        local_only=local_only,
        remote_only=remote_only,
        common=common,
    )


# =============================================================================
# Local Versions Reading
# =============================================================================


def _read_local_versions(catalog_root: Path, collection: str) -> dict[str, Any]:
    """Read local versions.json for a collection.

    Args:
        catalog_root: Path to catalog root directory.
        collection: Collection identifier.

    Returns:
        Parsed versions.json data as dictionary.

    Raises:
        FileNotFoundError: If versions.json doesn't exist.
        ValueError: If versions.json is invalid JSON.
    """
    # versions.json at collection root (per ADR-0023)
    versions_path = catalog_root / collection / "versions.json"

    if not versions_path.exists():
        raise FileNotFoundError(f"versions.json not found: {versions_path}")

    try:
        data: dict[str, Any] = json.loads(versions_path.read_text(encoding="utf-8"))
        return data
    except json.JSONDecodeError as e:
        raise ValueError(f"Invalid JSON in versions.json: {e}") from e


# =============================================================================
# Remote Versions Fetching
# =============================================================================


def _fetch_remote_versions(
    store: ObjectStore,
    prefix: str,
    collection: str,
) -> tuple[dict[str, Any] | None, str | None]:
    """Fetch remote versions.json and its etag atomically.

    Uses a single get() call to avoid TOCTOU race conditions where the file
    could change between head() and get() calls.

    Args:
        store: Object store instance.
        prefix: Prefix within the bucket.
        collection: Collection identifier.

    Returns:
        Tuple of (versions_data, etag). Both are None if file doesn't exist.
    """
    # versions.json at collection root (per ADR-0023)
    key = f"{prefix}/{collection}/versions.json".lstrip("/")

    try:
        # Single atomic get() - includes metadata with e_tag
        result = obs.get(store, key)
        content_bytes: bytes = bytes(result.bytes())

        # Extract etag from result metadata (avoids TOCTOU race)
        etag = result.meta.get("e_tag") if result.meta else None

        versions_data: dict[str, Any] = json.loads(content_bytes)
        return versions_data, etag

    except FileNotFoundError:
        return None, None
    except Exception as e:
        # Check if it's a "not found" error (various cloud providers report differently)
        error_str = str(e).lower()
        error_type = type(e).__name__.lower()
        if any(
            x in error_str or x in error_type
            for x in ["notfound", "404", "nosuchkey", "does not exist"]
        ):
            return None, None
        raise


# =============================================================================
# Asset Upload
# =============================================================================


def _build_remote_asset_set(
    remote_versions_data: dict[str, Any] | None,
) -> set[tuple[str, str]]:
    """Build set of (href, sha256) pairs from all assets in all remote versions.

    Args:
        remote_versions_data: Remote versions.json data, or None if no remote.

    Returns:
        Set of (href, sha256) tuples from all remote assets.
        Both href and sha256 must match for an asset to be considered "already exists".
        This ensures renamed files (same sha256, different href) are still uploaded.
    """
    if remote_versions_data is None:
        return set()

    remote_assets: set[tuple[str, str]] = set()
    for version_entry in remote_versions_data.get("versions", []):
        for asset_name, asset_data in version_entry.get("assets", {}).items():
            href = asset_data.get("href", asset_name)
            sha256 = asset_data.get("sha256")
            if sha256 and href:
                remote_assets.add((href, sha256))

    return remote_assets


def _get_assets_to_upload(
    catalog_root: Path,
    versions_data: dict[str, Any],
    versions_to_push: list[str],
    remote_versions_data: dict[str, Any] | None = None,
) -> list[Path]:
    """Get list of asset files that need to be uploaded.

    Compares local assets against remote by sha256 hash. Only assets that are
    new or changed (different sha256) are included. This prevents re-uploading
    unchanged assets when adding a single file to a large catalog (Issue #329).

    Args:
        catalog_root: Path to catalog root.
        versions_data: Local versions.json data.
        versions_to_push: List of version strings to push.
        remote_versions_data: Remote versions.json data for sha256 comparison.
            If None, all assets from versions_to_push are uploaded (first push).

    Returns:
        List of absolute paths to asset files that need to be uploaded.

    Raises:
        FileNotFoundError: If a referenced asset file doesn't exist.
    """
    # Build set of (href, sha256) pairs from all remote versions
    remote_assets = _build_remote_asset_set(remote_versions_data)

    assets_to_upload: list[Path] = []
    seen_hrefs: set[str] = set()
    skipped_count = 0

    for version_entry in versions_data.get("versions", []):
        version_str = version_entry.get("version")
        if version_str not in versions_to_push:
            continue

        for asset_name, asset_data in version_entry.get("assets", {}).items():
            href = asset_data.get("href", asset_name)

            # Skip if we've already processed this asset path
            if href in seen_hrefs:
                continue
            seen_hrefs.add(href)

            # Get local sha256 (may be None if malformed)
            local_sha256 = asset_data.get("sha256")

            # Skip upload only if BOTH href AND sha256 match remote
            # This ensures renamed files (same sha256, different href) are uploaded
            if local_sha256 and (href, local_sha256) in remote_assets:
                skipped_count += 1
                continue

            # Resolve path relative to catalog root
            asset_path = catalog_root / href
            if not asset_path.exists():
                raise FileNotFoundError(
                    f"Asset referenced in version {version_str} not found: {href}"
                )
            assets_to_upload.append(asset_path.resolve())

    # Log diffing results for user feedback (Issue #329)
    new_count = len(assets_to_upload)
    if skipped_count > 0 or (remote_assets and new_count > 0):
        info(f"Uploading {new_count} new/changed asset(s), skipping {skipped_count} unchanged")

    return assets_to_upload


def _cleanup_uploaded_assets(store: ObjectStore, uploaded_keys: list[str]) -> None:
    """Clean up (delete) uploaded assets after a failed push.

    This is called when asset uploads succeed but versions.json upload fails,
    preventing orphaned assets in the bucket.

    Args:
        store: Object store instance.
        uploaded_keys: List of object keys to delete.
    """
    if not uploaded_keys:
        return

    warn(f"Rolling back {len(uploaded_keys)} uploaded asset(s)...")
    for key in uploaded_keys:
        try:
            obs.delete(store, key)
            detail(f"Deleted: {key}")
        except Exception as e:
            # Log but don't fail - best effort cleanup
            warn(f"Failed to delete {key} during rollback: {e}")


# =============================================================================
# STAC Metadata File Discovery and Upload (Issue #252)
# =============================================================================


def _discover_stac_files(
    catalog_root: Path,
    collection: str,
    *,
    include_catalog: bool = False,
) -> dict[str, list[Path]]:
    """Discover STAC metadata files that should be uploaded for a collection.

    Finds collection.json and all item STAC files within the collection's
    directory structure. Optionally includes catalog.json and README.md files.

    Note: Portolan creates item files as {item_id}.json (not item.json).
    The item_id matches the item directory name by convention.

    Args:
        catalog_root: Path to catalog root.
        collection: Collection identifier.
        include_catalog: If True, include catalog.json and root README.md in discovery.
            Default False because catalog.json is a shared resource that
            should be uploaded once after all collections, not per-collection.

    Returns:
        Dict with keys 'catalog', 'collection', 'items', 'readmes' mapping to lists of paths.
        - 'catalog': [catalog_root/catalog.json] if include_catalog and exists
        - 'collection': [collection/collection.json] if exists
        - 'items': [collection/item1/item1.json, ...] for each item found
        - 'readmes': [README.md files at catalog and collection level]

    Raises:
        FileNotFoundError: If collection.json doesn't exist (required for push).
    """
    stac_files: dict[str, list[Path]] = {
        "catalog": [],
        "collection": [],
        "items": [],
        "readmes": [],
    }

    # 1. Root catalog.json and README.md (only if requested)
    if include_catalog:
        catalog_json = catalog_root / "catalog.json"
        if catalog_json.exists():
            stac_files["catalog"].append(catalog_json)
        # Root README.md
        root_readme = catalog_root / "README.md"
        if root_readme.exists():
            stac_files["readmes"].append(root_readme)

    # 2. Collection's collection.json (required) and README.md (optional)
    collection_dir = catalog_root / collection
    collection_json = collection_dir / "collection.json"
    if not collection_json.exists():
        raise FileNotFoundError(
            f"collection.json not found for '{collection}': {collection_json}. "
            "Run 'portolan add' to create STAC metadata before pushing."
        )
    stac_files["collection"].append(collection_json)
    # Collection-level README.md
    collection_readme = collection_dir / "README.md"
    if collection_readme.exists():
        stac_files["readmes"].append(collection_readme)

    # 3. All item STAC files within the collection
    # Portolan naming convention: items are in subdirectories named {item_id}
    # and the STAC file is {item_id}.json (not item.json)
    visited_paths: set[Path] = set()

    for item_dir in collection_dir.iterdir():
        # Skip non-directories and hidden directories
        if not item_dir.is_dir() or item_dir.name.startswith("."):
            continue

        # Symlink safety: resolve and detect cycles (matches discover_collections)
        try:
            resolved = item_dir.resolve()
        except OSError:
            warn(f"Cannot resolve path {item_dir}, skipping")
            continue

        if resolved in visited_paths:
            warn(f"Symlink cycle detected at {item_dir}, skipping")
            continue
        visited_paths.add(resolved)

        # Look for {item_id}.json where item_id = directory name
        item_id = item_dir.name
        item_json = item_dir / f"{item_id}.json"
        if item_json.exists():
            stac_files["items"].append(item_json)

    return stac_files


def _stat_files_safely(files: list[Path], errors: list[str]) -> tuple[dict[Path, int], list[Path]]:
    """Stat files safely, recording errors for files that can't be stat'd.

    This handles the case where files are deleted between discovery and upload.
    Returns both the file sizes dict and the list of successfully stat'd files.

    Args:
        files: List of file paths to stat.
        errors: List to append error messages to.

    Returns:
        Tuple of (file_sizes dict, list of files that were successfully stat'd).
    """
    file_sizes: dict[Path, int] = {}
    for f in files:
        try:
            file_sizes[f] = f.stat().st_size
        except OSError as e:
            errors.append(f"Failed to stat {f}: {e}")
    uploadable = [f for f in files if f in file_sizes]
    return file_sizes, uploadable


def _upload_stac_files(
    store: ObjectStore,
    catalog_root: Path,
    prefix: str,
    stac_files: dict[str, list[Path]],
    *,
    dry_run: bool = False,
    json_mode: bool = False,
    verbose: bool = False,
) -> tuple[int, list[str], list[str]]:
    """Upload STAC metadata files in manifest-last order.

    Upload order (manifest-last pattern for atomicity):
    1. Item STAC files (leaf manifests) - {item_id}.json
    2. collection.json (intermediate manifest)
    3. catalog.json (root manifest) - only if included in stac_files

    Note: READMEs are uploaded separately AFTER versions.json since they
    are derived from STAC + versions.json + metadata.yaml.

    Note: STAC files are NOT rolled back on failure. They are idempotent
    (re-uploading is safe) and the manifest-last pattern ensures consistency:
    versions.json is uploaded last, so incomplete pushes aren't "visible".

    Args:
        store: Object store instance.
        catalog_root: Path to catalog root (for relative path calculation).
        prefix: Prefix in object storage.
        stac_files: Dict of STAC files from _discover_stac_files().
        dry_run: If True, don't actually upload.
        json_mode: If True, suppress progress bar (for --json output).
        verbose: If True, show per-file output instead of progress bar.

    Returns:
        Tuple of (files_uploaded, errors, uploaded_keys).
    """
    import sys

    files_uploaded = 0
    errors: list[str] = []
    uploaded_keys: list[str] = []

    # Build ordered list: items first, then collection, then catalog
    # READMEs are uploaded separately after versions.json
    ordered_files: list[Path] = []
    ordered_files.extend(stac_files.get("items", []))
    ordered_files.extend(stac_files.get("collection", []))
    ordered_files.extend(stac_files.get("catalog", []))

    if not ordered_files:
        return 0, [], []

    # Pre-cache file sizes, handling stat errors for files deleted between discovery and upload
    file_sizes, uploadable_files = _stat_files_safely(ordered_files, errors)
    total = len(uploadable_files)
    total_bytes = sum(file_sizes.values())

    if total == 0:
        return 0, errors, []

    # Use progress bar for STAC uploads (ADR-0040: unified progress output)
    # Suppress in json_mode or when verbose (verbose shows per-file output)
    suppress_progress = json_mode or verbose or not sys.stderr.isatty()

    with UploadProgressReporter(
        total_files=total,
        total_bytes=total_bytes,
        json_mode=suppress_progress,
    ) as reporter:
        for file_path in uploadable_files:
            try:
                rel_path = file_path.relative_to(catalog_root)
                target_key = f"{prefix}/{rel_path.as_posix()}".lstrip("/")
                file_size = file_sizes[file_path]  # Use cached size

                if dry_run:
                    if verbose:
                        info(f"[DRY RUN] Would upload STAC: {rel_path}")
                    reporter.advance(bytes_uploaded=file_size)
                else:
                    if verbose:
                        detail(f"Uploading STAC: {rel_path}")
                    content = file_path.read_bytes()
                    obs.put(store, target_key, content)
                    files_uploaded += 1
                    uploaded_keys.append(target_key)
                    reporter.advance(bytes_uploaded=file_size)

            except Exception as e:
                error_msg = f"Failed to upload {file_path}: {e}"
                errors.append(error_msg)
                error(error_msg)

    return files_uploaded, errors, uploaded_keys


def _upload_readmes(
    store: ObjectStore,
    catalog_root: Path,
    prefix: str,
    stac_files: dict[str, list[Path]],
    *,
    dry_run: bool = False,
) -> tuple[int, list[str]]:
    """Upload README.md files after all other metadata.

    READMEs are derived from STAC + versions.json + metadata.yaml, so they
    must be uploaded last. They are not rolled back on failure since they
    are purely documentation.

    Args:
        store: Object store instance.
        catalog_root: Path to catalog root.
        prefix: Prefix in object storage.
        stac_files: Dict from _discover_stac_files() containing 'readmes' key.
        dry_run: If True, don't actually upload.

    Returns:
        Tuple of (files_uploaded, errors).
    """
    readmes = stac_files.get("readmes", [])
    if not readmes:
        return 0, []

    files_uploaded = 0
    errors: list[str] = []

    info(f"Uploading {len(readmes)} README file(s)...")

    for readme_path in readmes:
        try:
            rel_path = readme_path.relative_to(catalog_root)
            target_key = f"{prefix}/{rel_path.as_posix()}".lstrip("/")

            if dry_run:
                info(f"[DRY RUN] Would upload README: {rel_path}")
            else:
                detail(f"Uploading README: {rel_path}")
                content = readme_path.read_bytes()
                obs.put(store, target_key, content)
                files_uploaded += 1

        except Exception as e:
            error_msg = f"Failed to upload {readme_path}: {e}"
            errors.append(error_msg)
            error(error_msg)

    return files_uploaded, errors


def _upload_versions_json(
    store: ObjectStore,
    prefix: str,
    collection: str,
    versions_data: dict[str, Any],
    etag: str | None,
    *,
    force: bool = False,
) -> None:
    """Upload versions.json with optimistic locking.

    Args:
        store: Object store instance.
        prefix: Prefix in object storage.
        collection: Collection identifier.
        versions_data: The versions.json data to upload.
        etag: Expected etag for conditional put (None for first push).
        force: If True, use overwrite mode instead of conditional put.

    Raises:
        PushConflictError: If etag mismatch (remote changed during push).
    """
    # versions.json at collection root (per ADR-0023)
    key = f"{prefix}/{collection}/versions.json".lstrip("/")
    content = json.dumps(versions_data, indent=2).encode("utf-8")

    try:
        if force or etag is None:
            # Force mode or first push: use overwrite
            obs.put(store, key, content)
        else:
            # Conditional put with etag (optimistic locking)
            # obstore uses UpdateVersion dict as the mode parameter
            obs.put(store, key, content, mode={"e_tag": etag})
    except Exception as e:
        if "Precondition" in str(e) or "PreconditionError" in str(type(e).__name__):
            raise PushConflictError("Remote changed during push, re-run push to try again") from e
        raise


# =============================================================================
# Dry-Run Handling
# =============================================================================


def _handle_push_dry_run(
    catalog_root: Path,
    local_data: dict[str, Any],
    local_versions: list[str],
) -> PushResult:
    """Handle push dry-run mode: show what would be pushed without network I/O.

    This is extracted from push() to keep cyclomatic complexity manageable.

    Args:
        catalog_root: Resolved catalog root path.
        local_data: Parsed versions.json data.
        local_versions: List of version strings from local data.

    Returns:
        PushResult with dry_run=True and simulated counts.
    """
    # Try to get assets, but don't fail if some are missing (dry-run should be forgiving)
    try:
        assets = _get_assets_to_upload(catalog_root, local_data, local_versions)
        asset_count = len(assets)
        asset_paths = [asset.relative_to(catalog_root) for asset in assets]
        missing_assets: list[str] = []
    except FileNotFoundError as e:
        # Asset file is missing - warn but continue with dry-run
        warn(f"[DRY RUN] Warning: {e}")
        asset_count = 0
        asset_paths = []
        missing_assets = [str(e)]

    info(f"[DRY RUN] Would push up to {len(local_versions)} version(s): {local_versions}")
    info(f"[DRY RUN] Would upload up to {asset_count} asset file(s)")
    for rel_path in asset_paths:
        detail(f"  {rel_path}")
    warn("[DRY RUN] Remote conflict detection skipped (requires network)")
    warn("[DRY RUN] Actual versions pushed may be fewer if remote already has some")

    return PushResult(
        success=True,
        files_uploaded=0,
        versions_pushed=0,
        conflicts=[],
        errors=missing_assets,
        dry_run=True,
        would_push_versions=len(local_versions),
    )


# =============================================================================
# Main Push Function
# =============================================================================


def push(
    catalog_root: Path,
    collection: str,
    destination: str,
    *,
    force: bool = False,
    dry_run: bool = False,
    profile: str | None = None,
    region: str | None = None,
    workers: int | None = None,
    verbose: bool = False,
    json_mode: bool = False,
    suppress_progress: bool = False,
) -> PushResult:
    """Push local catalog changes to cloud object storage (sync wrapper).

    This is a thin wrapper around `push_async()` for backward compatibility.
    All logic is in the async implementation.

    Args:
        catalog_root: Path to the local catalog root.
        collection: Collection identifier to push.
        destination: Object store URL (e.g., s3://bucket/prefix).
        force: If True, overwrite remote even if diverged.
        dry_run: If True, show what would be uploaded without uploading.
        profile: AWS profile name (for S3 only).
        region: AWS region (for S3 only). Overrides profile/env config.
        workers: Number of parallel upload workers (maps to concurrency).
        verbose: If True, show per-file upload details (ADR-0040).
        json_mode: If True, suppress progress bar (for --json output).
        suppress_progress: If True, suppress progress bar (for nested calls).

    Returns:
        PushResult with upload statistics.

    Raises:
        FileNotFoundError: If catalog or versions.json doesn't exist.
        ValueError: If destination URL is invalid.
        PushConflictError: If remote diverged and force=False.
    """
    # Map workers to concurrency (same concept, different naming)
    concurrency = workers if workers is not None else None

    return asyncio.run(
        push_async(
            catalog_root=catalog_root,
            collection=collection,
            destination=destination,
            force=force,
            dry_run=dry_run,
            profile=profile,
            region=region,
            concurrency=concurrency,
            json_mode=json_mode,
            suppress_progress=suppress_progress,
            verbose=verbose,
        )
    )


# =============================================================================
# Async Push Implementation
# =============================================================================


async def _fetch_remote_versions_async(
    store: ObjectStore,
    prefix: str,
    collection: str,
) -> tuple[dict[str, Any] | None, str | None]:
    """Async version of _fetch_remote_versions.

    Uses obstore's async API for non-blocking I/O.

    Args:
        store: Object store instance.
        prefix: Prefix within the bucket.
        collection: Collection identifier.

    Returns:
        Tuple of (versions_data, etag). Both are None if file doesn't exist.
    """
    key = f"{prefix}/{collection}/versions.json".lstrip("/")

    try:
        # Use obstore's async get
        result = await obs.get_async(store, key)
        content_bytes: bytes = bytes(await result.bytes_async())

        # Extract etag from result metadata
        etag = result.meta.get("e_tag") if result.meta else None

        versions_data: dict[str, Any] = json.loads(content_bytes)
        return versions_data, etag

    except FileNotFoundError:
        return None, None
    except Exception as e:
        error_str = str(e).lower()
        error_type = type(e).__name__.lower()
        if any(
            x in error_str or x in error_type
            for x in ["notfound", "404", "nosuchkey", "does not exist"]
        ):
            return None, None
        raise


async def _upload_assets_async(
    store: ObjectStore,
    catalog_root: Path,
    prefix: str,
    assets: list[Path],
    *,
    concurrency: int = 50,
    json_mode: bool = False,
    suppress_progress: bool = False,
    verbose: bool = False,
) -> tuple[int, list[str], list[str], UploadMetrics]:
    """Upload asset files to object storage with async concurrent uploads.

    Uses AsyncIOExecutor for bounded concurrency with circuit breaker.

    Args:
        store: Object store instance.
        catalog_root: Path to catalog root (for relative path calculation).
        prefix: Prefix in object storage.
        assets: List of asset file paths to upload.
        concurrency: Maximum concurrent uploads (default 50).
        json_mode: If True, suppress progress bar.
        suppress_progress: If True, suppress progress bar.
        verbose: If True, print per-file upload details (ADR-0040).

    Returns:
        Tuple of (files_uploaded, errors, uploaded_keys, metrics).
    """
    metrics = UploadMetrics()

    if not assets:
        return 0, [], [], metrics

    total = len(assets)
    total_bytes = sum(p.stat().st_size for p in assets)

    # Pre-cache file sizes to avoid double stat() calls
    file_sizes: dict[Path, int] = {p: p.stat().st_size for p in assets}

    uploaded_keys: list[str] = []
    errors_list: list[str] = []

    async def upload_one(asset_path_str: str) -> tuple[str, int, float]:
        """Upload a single asset asynchronously."""
        asset_path = Path(asset_path_str)
        rel_path = asset_path.relative_to(catalog_root)
        target_key = f"{prefix}/{rel_path.as_posix()}".lstrip("/")

        size_bytes = file_sizes[asset_path]

        start = time.perf_counter()
        content = asset_path.read_bytes()
        await obs.put_async(store, target_key, content)
        duration = time.perf_counter() - start

        return target_key, size_bytes, duration

    asset_strs = [str(p) for p in assets]

    # Suppress progress bar when verbose (verbose replaces progress bar per ADR-0040)
    async with AsyncProgressReporter(
        total_files=total,
        total_bytes=total_bytes,
        json_mode=json_mode or suppress_progress or verbose,
    ) as reporter:

        def on_complete(
            item: str,
            result: tuple[str, int, float] | None,
            err: str | None,
            completed: int,
            total_count: int,
        ) -> None:
            """Track results (called from executor)."""
            rel_path = Path(item).relative_to(catalog_root)
            if err:
                errors_list.append(f"Failed to upload {rel_path}: {err}")
                error(f"Failed: {rel_path} - {err}")
            elif result:
                target_key, size_bytes, duration = result
                uploaded_keys.append(target_key)
                metrics.record(size_bytes, duration)
                reporter.advance(bytes_uploaded=size_bytes)
                # Verbose mode: print per-file details (ADR-0040)
                if verbose:
                    speed = size_bytes / duration if duration > 0 else 0
                    detail(
                        f"Uploaded ({completed}/{total_count}): {rel_path} ({format_file_size(size_bytes)}, {format_speed(speed)})"
                    )

        executor = AsyncIOExecutor[tuple[str, int, float]](
            concurrency=concurrency,
            circuit_breaker_threshold=5,
        )

        try:
            await executor.execute(
                items=asset_strs,
                operation=upload_one,
                on_complete=on_complete,
            )
        except CircuitBreakerError as e:
            errors_list.append(f"Circuit breaker tripped: {e}")
            error(f"Too many consecutive failures, aborting: {e}")

        metrics.record_elapsed(reporter.elapsed_seconds)

    return len(uploaded_keys), errors_list, uploaded_keys, metrics


async def _upload_stac_files_async(
    store: ObjectStore,
    catalog_root: Path,
    prefix: str,
    stac_files: dict[str, list[Path]],
    *,
    concurrency: int = 50,
    json_mode: bool = False,
    verbose: bool = False,
) -> tuple[int, list[str], list[str]]:
    """Upload STAC metadata files asynchronously.

    Upload order (manifest-last pattern for atomicity):
    1. Item STAC files (parallel)
    2. collection.json (sequential, after items)
    3. catalog.json (last)

    Args:
        store: Object store instance.
        catalog_root: Path to catalog root.
        prefix: Prefix in object storage.
        stac_files: Dict of STAC files from _discover_stac_files().
        concurrency: Maximum concurrent uploads.
        json_mode: If True, suppress progress bar.
        verbose: If True, print per-file upload details (ADR-0040).

    Returns:
        Tuple of (files_uploaded, errors, uploaded_keys).
    """
    errors: list[str] = []
    uploaded_keys: list[str] = []

    # Get file lists by type
    item_files = stac_files.get("items", [])
    collection_files = stac_files.get("collection", [])
    catalog_files = stac_files.get("catalog", [])

    all_files = item_files + collection_files + catalog_files
    if not all_files:
        return 0, [], []

    # Pre-cache file sizes, handling stat errors for files deleted between discovery and upload
    file_sizes, uploadable_files = _stat_files_safely(all_files, errors)
    if not uploadable_files:
        return 0, errors, []

    total_bytes = sum(file_sizes.values())

    # Filter file lists to only include uploadable files
    item_files = [f for f in item_files if f in file_sizes]
    collection_files = [f for f in collection_files if f in file_sizes]
    catalog_files = [f for f in catalog_files if f in file_sizes]

    # Semaphore for bounded concurrency
    semaphore = asyncio.Semaphore(concurrency)

    async def upload_one(file_path: Path) -> tuple[str, int] | None:
        """Upload a single STAC file with semaphore control."""
        async with semaphore:
            try:
                rel_path = file_path.relative_to(catalog_root)
                target_key = f"{prefix}/{rel_path.as_posix()}".lstrip("/")
                file_size = file_sizes[file_path]

                content = file_path.read_bytes()
                await obs.put_async(store, target_key, content)
                return target_key, file_size
            except Exception as e:
                error_msg = f"Failed to upload {file_path}: {e}"
                errors.append(error_msg)
                error(error_msg)
                return None

    # Suppress progress bar when verbose (verbose replaces progress bar per ADR-0040)
    async with AsyncProgressReporter(
        total_files=len(uploadable_files),
        total_bytes=total_bytes,
        json_mode=json_mode or verbose,
    ) as reporter:
        completed = 0
        total_count = len(uploadable_files)

        def log_verbose(file_path: Path, size: int) -> None:
            """Log verbose output for a file upload."""
            nonlocal completed
            completed += 1
            if verbose:
                rel_path = file_path.relative_to(catalog_root)
                detail(
                    f"Uploaded STAC ({completed}/{total_count}): {rel_path} ({format_file_size(size)})"
                )

        # Wave 1: Upload all item STAC files in parallel
        if item_files:
            tasks = [upload_one(f) for f in item_files]
            results = await asyncio.gather(*tasks)
            for i, result in enumerate(results):
                if result:
                    key, size = result
                    uploaded_keys.append(key)
                    reporter.advance(bytes_uploaded=size)
                    log_verbose(item_files[i], size)

        # Wave 2: Upload collection.json files in parallel (after items complete)
        if collection_files:
            tasks = [upload_one(f) for f in collection_files]
            results = await asyncio.gather(*tasks)
            for i, result in enumerate(results):
                if result:
                    key, size = result
                    uploaded_keys.append(key)
                    reporter.advance(bytes_uploaded=size)
                    log_verbose(collection_files[i], size)

        # Wave 3: Upload catalog.json last (manifest-last pattern)
        for file_path in catalog_files:
            result = await upload_one(file_path)
            if result:
                key, size = result
                uploaded_keys.append(key)
                reporter.advance(bytes_uploaded=size)
                log_verbose(file_path, size)

    return len(uploaded_keys), errors, uploaded_keys


async def _upload_versions_json_async(
    store: ObjectStore,
    prefix: str,
    collection: str,
    versions_data: dict[str, Any],
    etag: str | None,
    *,
    force: bool = False,
) -> None:
    """Upload versions.json with optimistic locking (async version).

    Args:
        store: Object store instance.
        prefix: Prefix in object storage.
        collection: Collection identifier.
        versions_data: The versions.json data to upload.
        etag: Expected etag for conditional put (None for first push).
        force: If True, use overwrite mode instead of conditional put.

    Raises:
        PushConflictError: If etag mismatch.
    """
    key = f"{prefix}/{collection}/versions.json".lstrip("/")
    # Use compact JSON for remote STAC files (saves bandwidth)
    content = json.dumps(versions_data, separators=(",", ":")).encode("utf-8")

    try:
        if force or etag is None:
            await obs.put_async(store, key, content)
        else:
            await obs.put_async(store, key, content, mode={"e_tag": etag})
    except Exception as e:
        if "Precondition" in str(e) or "PreconditionError" in str(type(e).__name__):
            raise PushConflictError("Remote changed during push, re-run push to try again") from e
        raise


async def _upload_readmes_async(
    store: ObjectStore,
    catalog_root: Path,
    prefix: str,
    stac_files: dict[str, list[Path]],
    *,
    concurrency: int = 50,
) -> tuple[int, list[str]]:
    """Upload README.md files asynchronously in parallel.

    Args:
        store: Object store instance.
        catalog_root: Path to catalog root.
        prefix: Prefix in object storage.
        stac_files: Dict from _discover_stac_files() containing 'readmes' key.
        concurrency: Maximum concurrent uploads.

    Returns:
        Tuple of (files_uploaded, errors).
    """
    readmes = stac_files.get("readmes", [])
    if not readmes:
        return 0, []

    errors: list[str] = []
    uploaded_count = 0

    info(f"Uploading {len(readmes)} README file(s)...")

    # Semaphore for bounded concurrency
    semaphore = asyncio.Semaphore(concurrency)

    async def upload_one(readme_path: Path) -> bool:
        """Upload a single README file with semaphore control."""
        async with semaphore:
            try:
                rel_path = readme_path.relative_to(catalog_root)
                target_key = f"{prefix}/{rel_path.as_posix()}".lstrip("/")

                detail(f"Uploading README: {rel_path}")
                content = readme_path.read_bytes()
                await obs.put_async(store, target_key, content)
                return True
            except Exception as e:
                error_msg = f"Failed to upload {readme_path}: {e}"
                errors.append(error_msg)
                error(error_msg)
                return False

    # Upload all READMEs in parallel (no ordering constraint)
    tasks = [upload_one(readme) for readme in readmes]
    results = await asyncio.gather(*tasks)
    uploaded_count = sum(1 for r in results if r)

    return uploaded_count, errors


async def _execute_push_uploads_async(
    store: ObjectStore,
    catalog_root: Path,
    prefix: str,
    collection: str,
    local_data: dict[str, Any],
    diff: PushVersionDiff,
    etag: str | None,
    *,
    concurrency: int,
    json_mode: bool,
    suppress_progress: bool,
    verbose: bool,
    force: bool,
    include_catalog: bool = True,
    remote_data: dict[str, Any] | None = None,
) -> PushResult:
    """Execute the upload phase of push_async.

    Handles assets, STAC files, versions.json, and READMEs in order.
    This is extracted from push_async to reduce cyclomatic complexity.

    Args:
        include_catalog: If True, upload catalog.json and root README.md.
        verbose: If True, print per-file upload details (ADR-0040).
        remote_data: Remote versions.json for sha256 diffing (Issue #329).

    Returns:
        PushResult with success or failure status and metrics.
    """
    # Get assets to upload (only new/changed, per Issue #329)
    assets = _get_assets_to_upload(
        catalog_root, local_data, diff.local_only, remote_versions_data=remote_data
    )

    # Upload assets first (async, manifest-last pattern)
    files_uploaded, upload_errors, uploaded_keys, metrics = await _upload_assets_async(
        store,
        catalog_root,
        prefix,
        assets,
        concurrency=concurrency,
        json_mode=json_mode,
        suppress_progress=suppress_progress,
        verbose=verbose,
    )

    if upload_errors:
        error("Asset upload failed, aborting push")
        _cleanup_uploaded_assets(store, uploaded_keys)
        return PushResult(
            success=False,
            files_uploaded=files_uploaded,
            versions_pushed=0,
            conflicts=[],
            errors=upload_errors,
            metrics=metrics,
        )

    # Upload STAC metadata files (async)
    # include_catalog controls whether catalog.json and root README.md are included.
    # - True for standalone push() so remote is a complete clonable catalog
    # - False when called from push_all_collections() (uploads once at end)
    try:
        stac_files = _discover_stac_files(catalog_root, collection, include_catalog=include_catalog)
    except FileNotFoundError as e:
        error(str(e))
        _cleanup_uploaded_assets(store, uploaded_keys)
        return PushResult(
            success=False,
            files_uploaded=files_uploaded,
            versions_pushed=0,
            conflicts=[],
            errors=[str(e)],
            metrics=metrics,
        )

    stac_uploaded, stac_errors, stac_keys = await _upload_stac_files_async(
        store,
        catalog_root,
        prefix,
        stac_files,
        concurrency=concurrency,
        json_mode=json_mode,
        verbose=verbose,
    )
    uploaded_keys.extend(stac_keys)
    files_uploaded += stac_uploaded

    if stac_errors:
        error("STAC metadata upload failed, aborting push")
        _cleanup_uploaded_assets(store, uploaded_keys)
        return PushResult(
            success=False,
            files_uploaded=files_uploaded,
            versions_pushed=0,
            conflicts=[],
            errors=stac_errors,
            metrics=metrics,
        )

    # Upload versions.json (async, manifest-last)
    info("Uploading versions.json...")
    try:
        await _upload_versions_json_async(store, prefix, collection, local_data, etag, force=force)
        msg = f"Pushed {len(diff.local_only)} version(s): {diff.local_only}"
        if metrics.total_bytes > 0:
            msg += (
                f" ({format_file_size(metrics.total_bytes)}, {format_speed(metrics.average_speed)})"
            )
        success(msg)
    except PushConflictError as e:
        _cleanup_uploaded_assets(store, uploaded_keys)
        raise PushConflictError("Remote changed during push, re-run push to try again") from e
    except Exception as e:
        _cleanup_uploaded_assets(store, uploaded_keys)
        error(f"Failed to upload versions.json: {e}")
        return PushResult(
            success=False,
            files_uploaded=files_uploaded,
            versions_pushed=0,
            conflicts=[],
            errors=[f"Failed to upload versions.json: {e}"],
            metrics=metrics,
        )

    # Upload READMEs last (async, parallel)
    readme_uploaded, readme_errors = await _upload_readmes_async(
        store, catalog_root, prefix, stac_files, concurrency=concurrency
    )
    files_uploaded += readme_uploaded
    for err in readme_errors:
        warn(err)

    return PushResult(
        success=True,
        files_uploaded=files_uploaded,
        versions_pushed=len(diff.local_only),
        conflicts=[],
        errors=[],
        metrics=metrics,
    )


async def push_async(
    catalog_root: Path,
    collection: str,
    destination: str,
    *,
    force: bool = False,
    dry_run: bool = False,
    profile: str | None = None,
    region: str | None = None,
    concurrency: int | None = None,
    json_mode: bool = False,
    suppress_progress: bool = False,
    verbose: bool = False,
    include_catalog: bool = True,
) -> PushResult:
    """Push local catalog changes to cloud object storage (async version).

    This is the async implementation of push() that uses native asyncio
    for concurrent uploads instead of ThreadPoolExecutor.

    Args:
        catalog_root: Path to the local catalog root.
        collection: Collection identifier to push.
        destination: Object store URL (e.g., s3://bucket/prefix).
        force: If True, overwrite remote even if diverged.
        dry_run: If True, show what would be uploaded without uploading.
        profile: AWS profile name (for S3 only).
        region: AWS region (for S3 only).
        concurrency: Maximum concurrent uploads (default 50).
        json_mode: If True, suppress progress bar.
        suppress_progress: If True, suppress progress bar.
        verbose: If True, print per-file upload details (ADR-0040).
        include_catalog: If True, upload catalog.json and root README.md.
            Set to False when called from push_all_collections (uploads them once at end).

    Returns:
        PushResult with upload statistics.

    Raises:
        FileNotFoundError: If catalog or versions.json doesn't exist.
        ValueError: If destination URL is invalid.
        PushConflictError: If remote diverged and force=False.
    """
    concurrency = concurrency or get_default_concurrency()

    # Validate catalog exists
    if not catalog_root.exists():
        raise FileNotFoundError(f"Catalog root not found: {catalog_root}")

    catalog_root = catalog_root.resolve()

    # Read local versions
    local_data = _read_local_versions(catalog_root, collection)
    local_versions: list[str] = [
        v.get("version") for v in local_data.get("versions", []) if v.get("version") is not None
    ]

    # Dry-run: return early without network I/O
    if dry_run:
        return _handle_push_dry_run(catalog_root, local_data, local_versions)

    # Setup store and fetch remote versions
    store, prefix = setup_store(destination, profile=profile, region=region)
    info(f"Checking remote state: {destination}")
    remote_data, etag = await _fetch_remote_versions_async(store, prefix, collection)

    # Extract remote versions
    if remote_data is None:
        info("No remote versions.json found (first push)")
        remote_versions: list[str] = []
    else:
        remote_versions = [v.get("version") for v in remote_data.get("versions", [])]
        detail(f"Remote version: {remote_data.get('current_version')}")

    # Diff versions and check for conflicts
    diff = diff_version_lists(local_versions, remote_versions)
    if diff.has_conflict and not force:
        raise PushConflictError(
            f"Remote has changes not present locally: {diff.remote_only}. "
            "Pull changes first or use --force to overwrite."
        )

    # Nothing to push?
    if not diff.local_only and not (force and diff.remote_only):
        info("Nothing to push - local and remote are in sync")
        return PushResult(
            success=True, files_uploaded=0, versions_pushed=0, conflicts=[], errors=[]
        )

    # Execute uploads (with remote data for sha256 diffing, Issue #329)
    return await _execute_push_uploads_async(
        store,
        catalog_root,
        prefix,
        collection,
        local_data,
        diff,
        etag,
        concurrency=concurrency,
        json_mode=json_mode,
        suppress_progress=suppress_progress,
        verbose=verbose,
        force=force,
        include_catalog=include_catalog,
        remote_data=remote_data,
    )


# =============================================================================
# Catalog-Wide Push (Issue #224)
# =============================================================================


@dataclass
class PushAllResult:
    """Result of pushing all collections in a catalog.

    Attributes:
        success: True if all collections pushed without errors.
        total_collections: Total number of collections found.
        successful_collections: Number of collections successfully pushed.
        failed_collections: Number of collections that failed to push.
        total_files_uploaded: Aggregate count of files uploaded across all collections.
        total_versions_pushed: Aggregate count of versions pushed across all collections.
        collection_errors: Dict mapping collection name to error messages.
    """

    success: bool
    total_collections: int
    successful_collections: int
    failed_collections: int
    total_files_uploaded: int
    total_versions_pushed: int
    collection_errors: dict[str, list[str]] = field(default_factory=dict)


def discover_collections(catalog_root: Path) -> list[str]:
    """Recursively discover all collections by finding directories with versions.json.

    Per ADR-0032 (Nested Catalogs with Flat Collections), collections can exist at any
    depth within the catalog structure. This function recursively searches for
    versions.json files and returns the relative paths to their parent directories.

    Args:
        catalog_root: Path to the catalog root directory.

    Returns:
        Sorted list of collection paths relative to catalog_root (POSIX format).
        Examples: ["collection", "sub-catalog/collection", "a/b/c/collection"]

    Raises:
        ValueError: If catalog_root is not a valid catalog directory.
    """
    if not catalog_root.exists():
        raise ValueError(f"Catalog root does not exist: {catalog_root}")

    # Validate this is actually a catalog (has sentinel file per ADR-0029)
    portolan_dir = catalog_root / ".portolan"
    config_yaml = portolan_dir / "config.yaml"
    if not config_yaml.exists():
        raise ValueError(f"Not a portolan catalog (missing .portolan/config.yaml): {catalog_root}")

    collections: list[str] = []
    visited_paths: set[Path] = set()

    # Use rglob to find all versions.json files recursively
    # Wrap in try/except to handle permission errors gracefully
    try:
        versions_files = list(catalog_root.rglob("versions.json"))
    except PermissionError as e:
        warn(f"Permission denied during catalog scan: {e}")
        versions_files = []

    for versions_file in versions_files:
        # Get the collection directory (parent of versions.json)
        collection_dir = versions_file.parent

        # Get path relative to catalog root for checking
        rel_path = collection_dir.relative_to(catalog_root)

        # Skip versions.json at catalog root (not a valid collection location)
        if not rel_path.parts:
            continue

        # Skip hidden directories (starting with '.') at any level in relative path
        # This includes .portolan, .git, .hidden, etc.
        if any(part.startswith(".") for part in rel_path.parts):
            continue

        # Resolve symlinks and detect cycles
        try:
            resolved = collection_dir.resolve()
        except OSError:
            # Cannot resolve (broken symlink or permission error)
            warn(f"Cannot resolve path {collection_dir}, skipping")
            continue

        # Skip if we've already seen this resolved path (symlink cycle)
        if resolved in visited_paths:
            warn(f"Symlink cycle detected at {collection_dir}, skipping")
            continue

        visited_paths.add(resolved)
        collections.append(rel_path.as_posix())

    return sorted(collections)


def _push_all_process_result(
    coll: str,
    result: PushResult | None,
    err_msg: str | None,
    current_completed: int,
    total: int,
    stats: dict[str, Any],
) -> None:
    """Process result of a single collection push (helper for push_all_collections)."""
    with output_section():
        if err_msg:
            error(f"[{current_completed}/{total}] Failed {coll}: {err_msg}")
            stats["failed"] += 1
            stats["errors"][coll] = [err_msg]
        elif result and result.success:
            v = result.versions_pushed
            f = result.files_uploaded
            success(f"[{current_completed}/{total}] {coll}: {v} version(s), {f} file(s)")
            stats["successful"] += 1
            stats["total_files"] += f
            stats["total_versions"] += v
            if result.metrics:
                stats["metrics"].merge(result.metrics)
        elif result:
            errors_list = result.errors + result.conflicts
            error(f"[{current_completed}/{total}] Failed {coll}: {', '.join(errors_list)}")
            stats["failed"] += 1
            stats["errors"][coll] = errors_list
        else:
            error(f"[{current_completed}/{total}] Failed {coll}: Unknown error")
            stats["failed"] += 1
            stats["errors"][coll] = ["Unknown error"]


def _push_all_upload_catalog(
    catalog_root: Path,
    destination: str,
    profile: str | None,
    region: str | None,
    dry_run: bool,
    stats: dict[str, Any],
) -> bool:
    """Upload catalog.json after all collections (helper for push_all_collections).

    Returns True if upload succeeded or was skipped, False if failed.
    """
    catalog_json = catalog_root / "catalog.json"

    if stats["successful"] > 0 and stats["failed"] == 0 and catalog_json.exists():
        if dry_run:
            info("[DRY RUN] Would upload catalog.json")
            return True
        try:
            store, prefix = setup_store(destination, profile=profile, region=region)
            target_key = f"{prefix}/catalog.json".lstrip("/")
            obs.put(store, target_key, catalog_json.read_bytes())
            success("Uploaded catalog.json")
            stats["total_files"] += 1
            return True
        except Exception as e:
            error(f"Failed to upload catalog.json: {e}")
            stats["errors"]["catalog.json"] = [str(e)]
            return False
    elif stats["failed"] > 0:
        warn("Skipping catalog.json upload because some collections failed")
    elif not catalog_json.exists():
        warn(f"catalog.json not found at {catalog_json} - remote catalog may be incomplete")
    return True


async def push_all_collections_async(
    catalog_root: Path,
    destination: str,
    *,
    force: bool = False,
    dry_run: bool = False,
    profile: str | None = None,
    region: str | None = None,
    concurrency: int | None = None,
    file_concurrency: int | None = None,
    verbose: bool = False,
    json_mode: bool = False,
) -> PushAllResult:
    """Push all collections in a catalog to cloud storage asynchronously.

    Primary async implementation. Uses asyncio.gather() for concurrent
    collection pushes within a single event loop.

    Args:
        catalog_root: Path to the catalog root directory.
        destination: Object store URL (e.g., s3://bucket/prefix).
        force: If True, overwrite remote even if diverged.
        dry_run: If True, show what would be uploaded without uploading.
        profile: AWS profile name (for S3 only).
        region: AWS region (for S3 only). Overrides profile/env config.
        concurrency: Maximum concurrent collection pushes. None = auto-detect.
        file_concurrency: Maximum concurrent file uploads within each collection.
            None = use push_async default. (Maps to --concurrency CLI flag.)
        verbose: If True, show per-file upload details.
        json_mode: If True, suppress progress bar (for --json output).

    Returns:
        PushAllResult with aggregate statistics and per-collection errors.

    Raises:
        ValueError: If catalog_root is not a valid catalog.
    """
    collections = discover_collections(catalog_root)
    total = len(collections)

    if total == 0:
        warn("No initialized collections found in catalog")
        warn("Collections need a versions.json file to be pushable")
        return PushAllResult(
            success=True,
            total_collections=0,
            successful_collections=0,
            failed_collections=0,
            total_files_uploaded=0,
            total_versions_pushed=0,
        )

    info(f"Found {total} collection(s) to push")

    # Track aggregate stats in a dict for helper function access
    stats: dict[str, Any] = {
        "successful": 0,
        "failed": 0,
        "total_files": 0,
        "total_versions": 0,
        "metrics": UploadMetrics(),
        "errors": {},
    }

    max_concurrent = (
        concurrency if concurrency is not None else min(total, get_default_concurrency())
    )
    max_concurrent = min(max_concurrent, total)

    info(f"Using concurrency: {max_concurrent}")

    # Semaphore for collection-level concurrency control
    semaphore = asyncio.Semaphore(max_concurrent)

    async def push_one(collection: str) -> tuple[str, PushResult | None, str | None]:
        """Push a single collection with semaphore control."""
        async with semaphore:
            try:
                result = await push_async(
                    catalog_root=catalog_root,
                    collection=collection,
                    destination=destination,
                    force=force,
                    dry_run=dry_run,
                    profile=profile,
                    region=region,
                    concurrency=file_concurrency,  # Pass file-level concurrency
                    json_mode=json_mode,
                    suppress_progress=True,
                    verbose=verbose,
                    include_catalog=False,  # Uploaded once at end by _push_all_upload_catalog
                )
                return (collection, result, None)
            except Exception as e:
                return (collection, None, f"{type(e).__name__}: {e}")

    # Run all collection pushes concurrently
    tasks = [push_one(coll) for coll in collections]
    results = await asyncio.gather(*tasks, return_exceptions=True)

    # Process results
    for i, result in enumerate(results):
        if isinstance(result, BaseException):
            coll = collections[i]
            error(f"[{i + 1}/{total}] Failed {coll}: {result}")
            stats["failed"] += 1
            stats["errors"][coll] = [str(result)]
        elif isinstance(result, tuple):
            coll, push_result, err_msg = result
            _push_all_process_result(coll, push_result, err_msg, i + 1, total, stats)

    catalog_ok = _push_all_upload_catalog(
        catalog_root, destination, profile, region, dry_run, stats
    )

    overall_success = stats["failed"] == 0 and catalog_ok
    with output_section():
        info(f"\n{'=' * 60}")
        if overall_success:
            msg = f"Pushed {stats['successful']} collection(s), "
            msg += f"{stats['total_versions']} version(s), {stats['total_files']} file(s)"
            if stats["metrics"].total_bytes > 0:
                size = format_file_size(stats["metrics"].total_bytes)
                speed = format_speed(stats["metrics"].average_speed)
                msg += f" ({size}, avg {speed})"
            success(msg)
        else:
            warn(
                f"Completed with errors: {stats['successful']} succeeded, {stats['failed']} failed"
            )
            for coll_name, errs in stats["errors"].items():
                warn(f"  {coll_name}: {', '.join(errs)}")

    return PushAllResult(
        success=overall_success,
        total_collections=total,
        successful_collections=stats["successful"],
        failed_collections=stats["failed"],
        total_files_uploaded=stats["total_files"],
        total_versions_pushed=stats["total_versions"],
        collection_errors=stats["errors"],
    )


def push_all_collections(
    catalog_root: Path,
    destination: str,
    *,
    force: bool = False,
    dry_run: bool = False,
    profile: str | None = None,
    region: str | None = None,
    workers: int | None = None,
    file_concurrency: int | None = None,
    verbose: bool = False,
    json_mode: bool = False,
) -> PushAllResult:
    """Push all collections in a catalog to cloud storage (sync wrapper).

    This is a thin wrapper around `push_all_collections_async()` for
    backward compatibility. All logic is in the async implementation.

    Args:
        catalog_root: Path to the catalog root directory.
        destination: Object store URL (e.g., s3://bucket/prefix).
        force: If True, overwrite remote even if diverged.
        dry_run: If True, show what would be uploaded without uploading.
        profile: AWS profile name (for S3 only).
        region: AWS region (for S3 only). Overrides profile/env config.
        workers: Number of parallel workers. None = auto-detect, 1 = sequential.
            (Maps to 'concurrency' in async implementation.)
        file_concurrency: Maximum concurrent file uploads within each collection.
            None = use push_async default. (Maps to --concurrency CLI flag.)
        verbose: If True, show per-file upload details.
        json_mode: If True, suppress progress bar (for --json output).

    Returns:
        PushAllResult with aggregate statistics and per-collection errors.

    Raises:
        ValueError: If catalog_root is not a valid catalog.
    """
    return asyncio.run(
        push_all_collections_async(
            catalog_root=catalog_root,
            destination=destination,
            force=force,
            dry_run=dry_run,
            profile=profile,
            region=region,
            concurrency=workers,
            file_concurrency=file_concurrency,
            verbose=verbose,
            json_mode=json_mode,
        )
    )
