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
import os
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

import obstore as obs
from obstore.store import S3Store

from portolan_cli.output import detail, error, info, success, warn
from portolan_cli.upload import ObjectStore, parse_object_store_url

# =============================================================================
# Worker Configuration (Issue #229)
# =============================================================================


def get_default_workers() -> int:
    """Auto-detect number of workers for parallel push.

    Uses CPU count with a sensible cap to prevent overwhelming the system
    or hitting API rate limits.

    Returns:
        Number of workers to use (1-8).
    """
    cpu_count = os.cpu_count()
    if cpu_count is None:
        return 4  # Sensible fallback
    return min(cpu_count, 8)  # Cap at 8


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
    """

    success: bool
    files_uploaded: int
    versions_pushed: int
    conflicts: list[str] = field(default_factory=list)
    errors: list[str] = field(default_factory=list)
    dry_run: bool = False
    would_push_versions: int = 0


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
        region: str | None = None

        if profile:
            from portolan_cli.upload import _load_aws_credentials_from_profile

            access_key, secret_key, region = _load_aws_credentials_from_profile(profile)
        else:
            access_key = os.environ.get("AWS_ACCESS_KEY_ID")
            secret_key = os.environ.get("AWS_SECRET_ACCESS_KEY")
            region = os.environ.get("AWS_REGION") or os.environ.get("AWS_DEFAULT_REGION")

        store_kwargs: dict[str, str] = {}
        if region:
            store_kwargs["region"] = region
        if access_key and secret_key:
            store_kwargs["access_key_id"] = access_key
            store_kwargs["secret_access_key"] = secret_key

        store: ObjectStore = S3Store(bucket, **store_kwargs)  # type: ignore[arg-type]

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
) -> tuple[int, list[str], list[str]]:
    """Upload asset files to object storage.

    Args:
        store: Object store instance.
        catalog_root: Path to catalog root (for relative path calculation).
        prefix: Prefix in object storage.
        assets: List of asset file paths to upload.
        dry_run: If True, don't actually upload.

    Returns:
        Tuple of (files_uploaded, errors, uploaded_keys).
        uploaded_keys contains the object keys that were successfully uploaded,
        useful for rollback on subsequent failures.
    """
    files_uploaded = 0
    errors: list[str] = []
    uploaded_keys: list[str] = []
    total = len(assets)

    for i, asset_path in enumerate(assets, 1):
        try:
            # Calculate relative path from catalog root
            rel_path = asset_path.relative_to(catalog_root)
            target_key = f"{prefix}/{rel_path}".lstrip("/")

            if dry_run:
                info(f"[DRY RUN] Would upload ({i}/{total}): {rel_path} -> {target_key}")
            else:
                info(f"Uploading ({i}/{total}): {rel_path}")
                obs.put(store, target_key, asset_path)
                files_uploaded += 1
                uploaded_keys.append(target_key)
                success(f"Uploaded: {rel_path}")
        except Exception as e:
            error_msg = f"Failed to upload {asset_path}: {e}"
            errors.append(error_msg)
            error(error_msg)

    return files_uploaded, errors, uploaded_keys


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
    store, prefix = _setup_store(destination, profile=profile)

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

    # Check for conflicts
    if diff.has_conflict and not force:
        conflict_msg = (
            f"Remote has changes not present locally: {diff.remote_only}. "
            "Pull changes first or use --force to overwrite."
        )
        if not dry_run:
            raise PushConflictError(conflict_msg)
        else:
            warn(f"[DRY RUN] Would conflict: {conflict_msg}")
            raise PushConflictError(conflict_msg)

    # Nothing to push?
    # With --force, we still push if remote has versions we don't have (to overwrite remote state)
    if not diff.local_only and not (force and diff.remote_only):
        if dry_run:
            info("[DRY RUN] Nothing would be pushed - local and remote are in sync")
        else:
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

    if dry_run:
        info(f"[DRY RUN] Would push {len(diff.local_only)} version(s): {diff.local_only}")
        info(f"[DRY RUN] Would upload {len(assets)} asset file(s)")
        for asset in assets:
            rel_path = asset.relative_to(catalog_root)
            detail(f"  {rel_path}")
        return PushResult(
            success=True,
            files_uploaded=0,
            versions_pushed=0,
            conflicts=[],
            errors=[],
        )

    # Upload assets first (manifest-last pattern)
    info(f"Uploading {len(assets)} asset(s)...")
    files_uploaded, upload_errors, uploaded_keys = _upload_assets(
        store, catalog_root, prefix, assets, dry_run=dry_run
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
        )

    # Upload versions.json last (manifest-last pattern)
    info("Uploading versions.json...")
    try:
        _upload_versions_json(store, prefix, collection, local_data, etag, force=force)
        success(f"Pushed {len(diff.local_only)} version(s): {diff.local_only}")
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
        )

    return PushResult(
        success=True,
        files_uploaded=files_uploaded,
        versions_pushed=len(diff.local_only),
        conflicts=[],
        errors=[],
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
    """Discover all collections in a catalog by finding directories with versions.json.

    Collections are subdirectories of the catalog root that contain a versions.json file.
    Hidden directories (starting with '.') are excluded.
    Symlinks are followed but cycles are detected and skipped.

    Args:
        catalog_root: Path to the catalog root directory.

    Returns:
        Sorted list of collection names (directory names).

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

    for item in catalog_root.iterdir():
        # Skip non-directories
        if not item.is_dir():
            continue

        # Skip hidden directories (including .portolan)
        if item.name.startswith("."):
            continue

        # Resolve symlinks and detect cycles
        try:
            resolved = item.resolve()
        except OSError:
            # Cannot resolve (broken symlink or permission error)
            warn(f"Cannot resolve path {item}, skipping")
            continue

        # Skip if we've already seen this resolved path (symlink cycle)
        if resolved in visited_paths:
            warn(f"Symlink cycle detected at {item}, skipping")
            continue

        visited_paths.add(resolved)

        # Check for versions.json
        versions_path = item / "versions.json"
        if versions_path.exists():
            collections.append(item.name)

    return sorted(collections)


def push_all_collections(
    catalog_root: Path,
    destination: str,
    *,
    force: bool = False,
    dry_run: bool = False,
    profile: str | None = None,
    workers: int | None = None,
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
        workers: Number of parallel workers. None = auto-detect, 1 = sequential.

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
            success=False,  # Changed from True - empty catalog is not success
            total_collections=0,
            successful_collections=0,
            failed_collections=0,
            total_files_uploaded=0,
            total_versions_pushed=0,
        )

    # Determine number of workers
    if workers is None:
        workers = get_default_workers()
    # Cap workers at collection count (no wasted threads)
    workers = min(workers, total)

    info(f"Found {total} collection(s) to push")

    # Track aggregate stats
    successful = 0
    failed = 0
    total_files = 0
    total_versions = 0
    collection_errors: dict[str, list[str]] = {}

    def push_one_collection(collection: str) -> tuple[str, PushResult | None, str | None]:
        """Push a single collection and return (collection, result, error_msg)."""
        try:
            result = push(
                catalog_root=catalog_root,
                collection=collection,
                destination=destination,
                force=force,
                dry_run=dry_run,
                profile=profile,
            )
            return (collection, result, None)
        except (PushConflictError, FileNotFoundError, ValueError, OSError) as e:
            return (collection, None, str(e))

    # Process collections (sequential if workers=1, parallel otherwise)
    if workers == 1:
        # Sequential execution (existing behavior)
        for i, collection in enumerate(collections, 1):
            info(f"→ Pushing collection {i}/{total}: {collection}")
            coll, result, err_msg = push_one_collection(collection)

            if err_msg:
                failed += 1
                collection_errors[coll] = [err_msg]
                error(f"✗ Failed {coll}: {err_msg}")
            elif result and result.success:
                successful += 1
                total_files += result.files_uploaded
                total_versions += result.versions_pushed
                versions = result.versions_pushed
                files = result.files_uploaded
                success(f"✓ Pushed {coll}: {versions} version(s), {files} file(s)")
            elif result:
                failed += 1
                errors = result.errors + result.conflicts
                collection_errors[coll] = errors
                error(f"✗ Failed {coll}: {', '.join(errors)}")
    else:
        # Parallel execution with ThreadPoolExecutor
        info(f"Using {workers} parallel worker(s)")
        with ThreadPoolExecutor(max_workers=workers) as executor:
            # Submit all collections
            futures = {executor.submit(push_one_collection, coll): coll for coll in collections}

            # Process results as they complete
            for future in as_completed(futures):
                coll, result, err_msg = future.result()

                if err_msg:
                    failed += 1
                    collection_errors[coll] = [err_msg]
                    error(f"✗ Failed {coll}: {err_msg}")
                elif result and result.success:
                    successful += 1
                    total_files += result.files_uploaded
                    total_versions += result.versions_pushed
                    versions = result.versions_pushed
                    files = result.files_uploaded
                    success(f"✓ Pushed {coll}: {versions} version(s), {files} file(s)")
                elif result:
                    failed += 1
                    errors = result.errors + result.conflicts
                    collection_errors[coll] = errors
                    error(f"✗ Failed {coll}: {', '.join(errors)}")

    # Summary report
    info(f"\n{'=' * 60}")
    if failed == 0:
        msg = f"✓ Pushed {successful} collection(s), "
        msg += f"{total_versions} version(s), {total_files} file(s) total"
        success(msg)
    else:
        warn(f"Completed with errors: {successful} succeeded, {failed} failed")
        for collection, errors in collection_errors.items():
            warn(f"  {collection}: {', '.join(errors)}")

    return PushAllResult(
        success=(failed == 0),
        total_collections=total,
        successful_collections=successful,
        failed_collections=failed,
        total_files_uploaded=total_files,
        total_versions_pushed=total_versions,
        collection_errors=collection_errors,
    )
