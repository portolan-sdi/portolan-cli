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

import json
import threading
import time
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

import obstore as obs
from obstore.store import S3Store

from portolan_cli.output import detail, error, info, output_section, success, warn
from portolan_cli.parallel import (
    execute_parallel,
    get_default_workers,  # Re-export for backwards compatibility
)
from portolan_cli.upload import ObjectStore, parse_object_store_url
from portolan_cli.upload_progress import UploadProgressReporter

# Re-export get_default_workers for backwards compatibility
__all__ = [
    "get_default_workers",
    "push",
    "push_all_collections",
    "discover_collections",
    "UploadMetrics",
    "format_file_size",
    "format_speed",
]


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
class VersionDiff:
    """Result of diffing local vs remote versions.

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


def diff_version_lists(local_versions: list[str], remote_versions: list[str]) -> VersionDiff:
    """Compute diff between local and remote version string lists.

    This is a simple set-based diff for push operations, comparing version
    strings to determine what needs to be pushed.

    Note: pull.py has a separate diff_versions() that works with VersionsFile
    objects and computes files to download.

    Args:
        local_versions: List of version strings from local versions.json.
        remote_versions: List of version strings from remote versions.json.

    Returns:
        VersionDiff with local_only, remote_only, and common versions.
    """
    local_set = set(local_versions)
    remote_set = set(remote_versions)

    # Preserve order from original lists
    local_only = [v for v in local_versions if v not in remote_set]
    remote_only = [v for v in remote_versions if v not in local_set]
    common = [v for v in local_versions if v in remote_set]

    return VersionDiff(
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


def _setup_store(
    destination: str,
    *,
    profile: str | None = None,
    region: str | None = None,
) -> tuple[ObjectStore, str]:
    """Setup object store and extract prefix from destination URL.

    Supports:
    - S3 (s3://): Uses AWS credentials from profile or environment
    - GCS (gs://): Uses GOOGLE_APPLICATION_CREDENTIALS or service account path
    - Azure (az://): Uses AZURE_STORAGE_ACCOUNT + key/SAS token

    Args:
        destination: Object store URL (e.g., s3://bucket/prefix, gs://bucket/prefix,
            az://container/prefix).
        profile: AWS profile name (for S3 only).
        region: AWS region (for S3 only). Takes precedence over profile/env config.

    Returns:
        Tuple of (store, prefix).
    """
    import os

    from obstore.store import AzureStore, GCSStore

    bucket_url, prefix = parse_object_store_url(destination)

    if bucket_url.startswith("s3://"):
        bucket = bucket_url.replace("s3://", "")

        # Load credentials
        access_key: str | None = None
        secret_key: str | None = None
        profile_region: str | None = None

        if profile:
            from portolan_cli.upload import _load_aws_credentials_from_profile

            access_key, secret_key, profile_region = _load_aws_credentials_from_profile(profile)
        else:
            access_key = os.environ.get("AWS_ACCESS_KEY_ID")
            secret_key = os.environ.get("AWS_SECRET_ACCESS_KEY")

        # Region precedence: explicit param > env var > profile config
        resolved_region = region
        if not resolved_region:
            resolved_region = os.environ.get("AWS_REGION") or os.environ.get("AWS_DEFAULT_REGION")
        if not resolved_region:
            resolved_region = profile_region

        store_kwargs: dict[str, Any] = {}
        if resolved_region:
            store_kwargs["region"] = resolved_region
        if access_key and secret_key:
            store_kwargs["access_key_id"] = access_key
            store_kwargs["secret_access_key"] = secret_key

        # Bucket names with dots (e.g., us-west-2.opendata.source.coop) require
        # path-style requests because virtual-hosted style would create invalid
        # DNS names (bucket.s3.region.amazonaws.com doesn't work with dots)
        if "." in bucket:
            store_kwargs["virtual_hosted_style_request"] = False

        store: ObjectStore = S3Store(bucket, **store_kwargs)

    elif bucket_url.startswith("gs://"):
        bucket = bucket_url.replace("gs://", "")

        # GCS credentials from environment
        service_account_path = os.environ.get("GOOGLE_APPLICATION_CREDENTIALS")

        gcs_kwargs: dict[str, str] = {}
        if service_account_path:
            gcs_kwargs["service_account_path"] = service_account_path

        store = GCSStore(bucket, **gcs_kwargs)  # type: ignore[arg-type]

    elif bucket_url.startswith("az://"):
        container = bucket_url.replace("az://", "")

        # Azure credentials from environment
        account = os.environ.get("AZURE_STORAGE_ACCOUNT")
        access_key_azure = os.environ.get("AZURE_STORAGE_KEY")
        sas_token = os.environ.get("AZURE_STORAGE_SAS_TOKEN")

        azure_kwargs: dict[str, str] = {}
        if account:
            azure_kwargs["account"] = account
        if access_key_azure:
            azure_kwargs["access_key"] = access_key_azure
        elif sas_token:
            azure_kwargs["sas_token"] = sas_token

        store = AzureStore(container, **azure_kwargs)  # type: ignore[arg-type]

    else:
        # Fallback to generic URL parsing (for local/memory stores or unknown schemes)
        store = obs.store.from_url(bucket_url)

    return store, prefix


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


def _get_assets_to_upload(
    catalog_root: Path,
    versions_data: dict[str, Any],
    versions_to_push: list[str],
) -> list[Path]:
    """Get list of asset files that need to be uploaded.

    Args:
        catalog_root: Path to catalog root.
        versions_data: Local versions.json data.
        versions_to_push: List of version strings to push.

    Returns:
        List of absolute paths to asset files.

    Raises:
        FileNotFoundError: If a referenced asset file doesn't exist.
    """
    assets_to_upload: list[Path] = []
    seen_hrefs: set[str] = set()

    for version_entry in versions_data.get("versions", []):
        version_str = version_entry.get("version")
        if version_str not in versions_to_push:
            continue

        for asset_name, asset_data in version_entry.get("assets", {}).items():
            href = asset_data.get("href", asset_name)

            # Skip if we've already added this asset
            if href in seen_hrefs:
                continue
            seen_hrefs.add(href)

            # Resolve path relative to catalog root
            asset_path = catalog_root / href
            if not asset_path.exists():
                raise FileNotFoundError(
                    f"Asset referenced in version {version_str} not found: {href}"
                )
            assets_to_upload.append(asset_path.resolve())

    return assets_to_upload


def _upload_assets(
    store: ObjectStore,
    catalog_root: Path,
    prefix: str,
    assets: list[Path],
    *,
    dry_run: bool = False,
    workers: int | None = None,
    verbose: bool = False,
    json_mode: bool = False,
    suppress_progress: bool = False,
) -> tuple[int, list[str], list[str], UploadMetrics]:
    """Upload asset files to object storage with parallel workers.

    Args:
        store: Object store instance.
        catalog_root: Path to catalog root (for relative path calculation).
        prefix: Prefix in object storage.
        assets: List of asset file paths to upload.
        dry_run: If True, don't actually upload.
        workers: Number of parallel upload workers. None = auto-detect.
        verbose: If True, show per-file upload details. Default quiet mode
            only shows failures.
        json_mode: If True, suppress progress bar (for --json output).
        suppress_progress: If True, suppress progress bar (for nested calls
            where parent owns the progress surface).

    Returns:
        Tuple of (files_uploaded, errors, uploaded_keys, metrics).
        uploaded_keys contains the object keys that were successfully uploaded,
        useful for rollback on subsequent failures.
        metrics contains upload performance data for summary display.
    """
    metrics = UploadMetrics()

    if not assets:
        return 0, [], [], metrics

    total = len(assets)

    if dry_run:
        # Dry run stays sequential for readable output
        for i, asset_path in enumerate(assets, 1):
            rel_path = asset_path.relative_to(catalog_root)
            target_key = f"{prefix}/{rel_path.as_posix()}".lstrip("/")
            info(f"[DRY RUN] Would upload ({i}/{total}): {rel_path} -> {target_key}")
        return 0, [], [], metrics

    # Calculate total bytes for progress bar
    total_bytes = sum(p.stat().st_size for p in assets)

    # Thread-safe containers for parallel execution
    uploaded_keys: list[str] = []
    errors_list: list[str] = []
    keys_lock = threading.Lock()
    # Hold reference to progress reporter for on_complete callback
    progress_reporter: UploadProgressReporter | None = None

    def upload_one(asset_path_str: str) -> tuple[str, int, float]:
        """Upload a single asset. Returns (target_key, size_bytes, duration_seconds)."""
        asset_path = Path(asset_path_str)
        rel_path = asset_path.relative_to(catalog_root)
        target_key = f"{prefix}/{rel_path.as_posix()}".lstrip("/")

        # Get file size before upload
        size_bytes = asset_path.stat().st_size

        # Time the upload
        start = time.perf_counter()
        obs.put(store, target_key, asset_path)
        duration = time.perf_counter() - start

        return target_key, size_bytes, duration

    # Convert paths to strings for execute_parallel
    asset_strs = [str(p) for p in assets]

    def on_complete(
        item: str,
        result: tuple[str, int, float] | None,
        err: str | None,
        completed: int,
        total_count: int,
    ) -> None:
        """Track results thread-safely."""
        rel_path = Path(item).relative_to(catalog_root)
        with keys_lock:
            if err:
                errors_list.append(f"Failed to upload {rel_path}: {err}")
                # Always show failures
                error(f"Failed: {rel_path} - {err}")
            else:
                target_key, size_bytes, duration = result  # type: ignore[misc]
                uploaded_keys.append(target_key)
                metrics.record(size_bytes, duration)

                # Update progress bar
                if progress_reporter is not None:
                    progress_reporter.advance(bytes_uploaded=size_bytes)

                # Only show per-file details in verbose mode (without progress bar)
                if verbose and json_mode:
                    size_str = format_file_size(size_bytes)
                    speed = size_bytes / duration if duration > 0 else 0
                    speed_str = format_speed(speed)
                    detail(f"[{completed}/{total_count}] {rel_path} ({size_str}, {speed_str})")

    # Use progress bar for live feedback (unless json_mode or suppress_progress)
    with UploadProgressReporter(
        total_files=total,
        total_bytes=total_bytes,
        json_mode=json_mode or suppress_progress,
    ) as reporter:
        progress_reporter = reporter
        execute_parallel(
            items=asset_strs,
            operation=upload_one,
            workers=workers,
            on_complete=on_complete,
            verbose=False,  # Progress bar handles display, not execute_parallel
        )
        # Record wall-clock elapsed time for accurate average_speed calculation
        metrics.record_elapsed(reporter.elapsed_seconds)

    return len(uploaded_keys), errors_list, uploaded_keys, metrics


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


def _upload_stac_files(
    store: ObjectStore,
    catalog_root: Path,
    prefix: str,
    stac_files: dict[str, list[Path]],
    *,
    dry_run: bool = False,
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

    Returns:
        Tuple of (files_uploaded, errors, uploaded_keys).
    """
    files_uploaded = 0
    errors: list[str] = []
    uploaded_keys: list[str] = []

    # Build ordered list: items first, then collection, then catalog
    # READMEs are uploaded separately after versions.json
    ordered_files: list[Path] = []
    ordered_files.extend(stac_files.get("items", []))
    ordered_files.extend(stac_files.get("collection", []))
    ordered_files.extend(stac_files.get("catalog", []))

    total = len(ordered_files)
    if total == 0:
        return 0, [], []

    info(f"Uploading {total} STAC metadata file(s)...")

    for i, file_path in enumerate(ordered_files, 1):
        try:
            rel_path = file_path.relative_to(catalog_root)
            # Use as_posix() to ensure forward slashes on Windows (cloud keys are always /)
            target_key = f"{prefix}/{rel_path.as_posix()}".lstrip("/")

            if dry_run:
                info(f"[DRY RUN] Would upload STAC ({i}/{total}): {rel_path}")
            else:
                detail(f"Uploading STAC ({i}/{total}): {rel_path}")
                # Read and upload the JSON file
                content = file_path.read_bytes()
                obs.put(store, target_key, content)
                files_uploaded += 1
                uploaded_keys.append(target_key)

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
    """Push local catalog changes to cloud object storage.

    This function:
    1. Reads local versions.json
    2. Fetches remote versions.json (with etag)
    3. Diffs local vs remote to find changes
    4. Detects conflicts (unless --force)
    5. Uploads changed assets (manifest-last)
    6. Uploads versions.json with etag check

    Args:
        catalog_root: Path to the local catalog root.
        collection: Collection identifier to push.
        destination: Object store URL (e.g., s3://bucket/prefix).
        force: If True, overwrite remote even if diverged.
        dry_run: If True, show what would be uploaded without uploading.
        profile: AWS profile name (for S3 only).
        region: AWS region (for S3 only). Overrides profile/env config.
        workers: Number of parallel upload workers. None = auto-detect.
        verbose: If True, show per-file upload details with size and speed.
            Default is quiet mode (only shows failures).
        json_mode: If True, suppress progress bar (for --json output).
        suppress_progress: If True, suppress progress bar (for nested calls
            where parent owns the progress surface, e.g., catalog-wide push).

    Returns:
        PushResult with upload statistics.

    Raises:
        FileNotFoundError: If catalog or versions.json doesn't exist.
        ValueError: If destination URL is invalid.
        PushConflictError: If remote diverged and force=False.
    """
    # Validate catalog exists
    if not catalog_root.exists():
        raise FileNotFoundError(f"Catalog root not found: {catalog_root}")

    # Resolve catalog_root to absolute path for consistent path operations
    catalog_root = catalog_root.resolve()

    # Read local versions
    local_data = _read_local_versions(catalog_root, collection)
    # Filter out None values (malformed version entries)
    local_versions: list[str] = [
        v.get("version") for v in local_data.get("versions", []) if v.get("version") is not None
    ]

    # Bug #137: dry-run must not make any network calls.
    # Return early with a simulated "would push" result before any I/O.
    if dry_run:
        return _handle_push_dry_run(catalog_root, local_data, local_versions)

    # Setup store
    store, prefix = _setup_store(destination, profile=profile, region=region)

    # Fetch remote versions
    info(f"Checking remote state: {destination}")
    remote_data, etag = _fetch_remote_versions(store, prefix, collection)

    if remote_data is None:
        info("No remote versions.json found (first push)")
        remote_versions: list[str] = []
    else:
        remote_versions = [v.get("version") for v in remote_data.get("versions", [])]
        detail(f"Remote version: {remote_data.get('current_version')}")

    # Diff versions
    diff = diff_version_lists(local_versions, remote_versions)

    # Check for conflicts (dry_run already returned above, so we're always in live mode here)
    if diff.has_conflict and not force:
        conflict_msg = (
            f"Remote has changes not present locally: {diff.remote_only}. "
            "Pull changes first or use --force to overwrite."
        )
        raise PushConflictError(conflict_msg)

    # Nothing to push?
    # With --force, we still push if remote has versions we don't have (to overwrite remote state)
    if not diff.local_only and not (force and diff.remote_only):
        info("Nothing to push - local and remote are in sync")
        return PushResult(
            success=True,
            files_uploaded=0,
            versions_pushed=0,
            conflicts=[],
            errors=[],
        )

    # Get assets to upload
    assets = _get_assets_to_upload(catalog_root, local_data, diff.local_only)

    # Upload assets first (manifest-last pattern)
    files_uploaded, upload_errors, uploaded_keys, metrics = _upload_assets(
        store,
        catalog_root,
        prefix,
        assets,
        dry_run=False,
        workers=workers,
        verbose=verbose,
        json_mode=json_mode,
        suppress_progress=suppress_progress,
    )

    if upload_errors:
        error("Asset upload failed, aborting push")
        # Clean up any assets that were uploaded before the failure
        _cleanup_uploaded_assets(store, uploaded_keys)
        return PushResult(
            success=False,
            files_uploaded=files_uploaded,
            versions_pushed=0,
            conflicts=[],
            errors=upload_errors,
            metrics=metrics,
        )

    # Upload STAC metadata files (Issue #252)
    # Order: item STAC -> collection.json -> catalog.json (leaf to root)
    # Include catalog.json so standalone push() creates a clonable remote catalog
    try:
        stac_files = _discover_stac_files(catalog_root, collection, include_catalog=True)
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

    stac_uploaded, stac_errors, stac_keys = _upload_stac_files(
        store, catalog_root, prefix, stac_files, dry_run=False
    )
    # Track STAC keys for rollback - if versions.json fails, STAC files should
    # also be rolled back to avoid broken references to rolled-back assets
    uploaded_keys.extend(stac_keys)
    files_uploaded += stac_uploaded

    if stac_errors:
        error("STAC metadata upload failed, aborting push")
        # Rollback both assets and STAC files
        _cleanup_uploaded_assets(store, uploaded_keys)
        return PushResult(
            success=False,
            files_uploaded=files_uploaded,
            versions_pushed=0,
            conflicts=[],
            errors=stac_errors,
            metrics=metrics,
        )

    # Upload versions.json (manifest-last pattern for data integrity)
    info("Uploading versions.json...")
    try:
        _upload_versions_json(store, prefix, collection, local_data, etag, force=force)
        # Build success message with optional metrics
        msg = f"Pushed {len(diff.local_only)} version(s): {diff.local_only}"
        if metrics.total_bytes > 0:
            size = format_file_size(metrics.total_bytes)
            speed = format_speed(metrics.average_speed)
            msg += f" ({size}, {speed})"
        success(msg)
    except PushConflictError as e:
        # Clean up uploaded assets on versions.json failure (orphan prevention)
        _cleanup_uploaded_assets(store, uploaded_keys)
        raise PushConflictError("Remote changed during push, re-run push to try again") from e
    except Exception as e:
        # Clean up uploaded assets on any versions.json upload failure
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

    # Upload READMEs last (derived from STAC + versions.json + metadata.yaml)
    # README errors are warnings, not failures - the data is already pushed
    readme_uploaded, readme_errors = _upload_readmes(
        store, catalog_root, prefix, stac_files, dry_run=False
    )
    files_uploaded += readme_uploaded
    if readme_errors:
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


def push_all_collections(
    catalog_root: Path,
    destination: str,
    *,
    force: bool = False,
    dry_run: bool = False,
    profile: str | None = None,
    region: str | None = None,
    workers: int | None = None,
    verbose: bool = False,
    json_mode: bool = False,
) -> PushAllResult:
    """Push all collections in a catalog to cloud storage.

    Processes collections with configurable parallelism.
    Continues on individual failures and reports all errors at the end.

    Args:
        catalog_root: Path to the catalog root directory.
        destination: Object store URL (e.g., s3://bucket/prefix).
        force: If True, overwrite remote even if diverged.
        dry_run: If True, show what would be uploaded without uploading.
        profile: AWS profile name (for S3 only).
        region: AWS region (for S3 only). Overrides profile/env config.
        workers: Number of parallel workers. None = auto-detect, 1 = sequential.
        verbose: If True, show per-file upload details.
        json_mode: If True, suppress progress bar (for --json output).

    Returns:
        PushAllResult with aggregate statistics and per-collection errors.

    Raises:
        ValueError: If catalog_root is not a valid catalog.
    """
    # discover_collections validates catalog and raises ValueError if invalid
    collections = discover_collections(catalog_root)
    total = len(collections)

    if total == 0:
        warn("No initialized collections found in catalog")
        warn("Collections need a versions.json file to be pushable")
        return PushAllResult(
            success=True,  # Empty catalog is not a failure, just nothing to do
            total_collections=0,
            successful_collections=0,
            failed_collections=0,
            total_files_uploaded=0,
            total_versions_pushed=0,
        )

    info(f"Found {total} collection(s) to push")

    # Track aggregate stats (thread-safe via output_section for reporting)
    successful = 0
    failed = 0
    total_files = 0
    total_versions = 0
    total_metrics = UploadMetrics()  # Aggregate upload metrics across collections
    collection_errors: dict[str, list[str]] = {}

    def push_one(collection: str) -> PushResult:
        """Push a single collection."""
        # Use workers=1 here since collection-level parallelism is already handled
        # by execute_parallel. File-level parallelism is controlled by the workers
        # param when pushing a single collection directly via CLI.
        # suppress_progress=True prevents nested progress bars from interfering
        # with the catalog-level progress output.
        return push(
            catalog_root=catalog_root,
            collection=collection,
            destination=destination,
            force=force,
            dry_run=dry_run,
            profile=profile,
            region=region,
            workers=1,
            verbose=verbose,
            json_mode=json_mode,
            suppress_progress=True,  # Catalog-level owns the progress surface
        )

    def on_complete(
        coll: str,
        result: PushResult | None,
        err_msg: str | None,
        completed: int,
        total_count: int,
    ) -> None:
        """Process completion of a single collection (called with thread-safe progress)."""
        nonlocal successful, failed, total_files, total_versions

        # Use output_section to keep multi-line output together
        with output_section():
            if err_msg:
                error(f"[{completed}/{total_count}] Failed {coll}: {err_msg}")
                failed += 1
                collection_errors[coll] = [err_msg]
            elif result and result.success:
                v = result.versions_pushed
                f = result.files_uploaded
                success(f"[{completed}/{total_count}] {coll}: {v} version(s), {f} file(s)")
                successful += 1
                total_files += f
                total_versions += v
                # Aggregate metrics from successful pushes
                if result.metrics:
                    total_metrics.merge(result.metrics)
            elif result:
                errors_list = result.errors + result.conflicts
                error(f"[{completed}/{total_count}] Failed {coll}: {', '.join(errors_list)}")
                failed += 1
                collection_errors[coll] = errors_list
            else:
                error(f"[{completed}/{total_count}] Failed {coll}: Unknown error")
                failed += 1
                collection_errors[coll] = ["Unknown error"]

    # Execute with common parallel infrastructure
    execute_parallel(
        items=collections,
        operation=push_one,
        workers=workers,
        on_complete=on_complete,
    )

    # Upload catalog.json once after all collections (Issue #252)
    # Only upload when ALL collections succeeded to avoid publishing incomplete catalog
    # Note: Individual push() calls also upload catalog.json for standalone use
    catalog_json = catalog_root / "catalog.json"
    catalog_upload_failed = False

    if successful > 0 and failed == 0 and catalog_json.exists():
        if dry_run:
            info("[DRY RUN] Would upload catalog.json")
        else:
            try:
                store, prefix = _setup_store(destination, profile=profile, region=region)
                target_key = f"{prefix}/catalog.json".lstrip("/")
                content = catalog_json.read_bytes()
                obs.put(store, target_key, content)
                success("Uploaded catalog.json")
                total_files += 1
            except Exception as e:
                error(f"Failed to upload catalog.json: {e}")
                catalog_upload_failed = True
                collection_errors["catalog.json"] = [str(e)]
    elif failed > 0:
        warn("Skipping catalog.json upload because some collections failed")
    elif not catalog_json.exists():
        warn(f"catalog.json not found at {catalog_json} - remote catalog may be incomplete")

    # Summary report (use output_section to keep summary together)
    overall_success = failed == 0 and not catalog_upload_failed
    with output_section():
        info(f"\n{'=' * 60}")
        if overall_success:
            msg = f"Pushed {successful} collection(s), "
            msg += f"{total_versions} version(s), {total_files} file(s)"
            # Add throughput summary if we have metrics
            if total_metrics.total_bytes > 0:
                size = format_file_size(total_metrics.total_bytes)
                speed = format_speed(total_metrics.average_speed)
                msg += f" ({size}, avg {speed})"
            success(msg)
        else:
            warn(f"Completed with errors: {successful} succeeded, {failed} failed")
            for coll_name, errs in collection_errors.items():
                warn(f"  {coll_name}: {', '.join(errs)}")

    return PushAllResult(
        success=overall_success,
        total_collections=total,
        successful_collections=successful,
        failed_collections=failed,
        total_files_uploaded=total_files,
        total_versions_pushed=total_versions,
        collection_errors=collection_errors,
    )
