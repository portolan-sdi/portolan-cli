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
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

import obstore as obs
from obstore.store import S3Store

from portolan_cli.output import detail, error, info, success, warn
from portolan_cli.upload import ObjectStore, parse_object_store_url

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
    """

    success: bool
    files_uploaded: int
    versions_pushed: int
    conflicts: list[str] = field(default_factory=list)
    errors: list[str] = field(default_factory=list)


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


def diff_versions(local_versions: list[str], remote_versions: list[str]) -> VersionDiff:
    """Compute diff between local and remote version lists.

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
    versions_path = catalog_root / ".portolan" / "collections" / collection / "versions.json"

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

    Args:
        destination: Object store URL (e.g., s3://bucket/prefix).
        profile: AWS profile name (for S3 only).

    Returns:
        Tuple of (store, prefix).
    """
    import os

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
    else:
        store = obs.store.from_url(bucket_url)

    return store, prefix


def _fetch_remote_versions(
    store: ObjectStore,
    prefix: str,
    collection: str,
) -> tuple[dict[str, Any] | None, str | None]:
    """Fetch remote versions.json and its etag.

    Args:
        store: Object store instance.
        prefix: Prefix within the bucket.
        collection: Collection identifier.

    Returns:
        Tuple of (versions_data, etag). Both are None if file doesn't exist.
    """
    key = f"{prefix}/.portolan/collections/{collection}/versions.json".lstrip("/")

    try:
        # Get metadata first to get etag
        meta = obs.head(store, key)
        etag = meta.get("e_tag")

        # Then get content
        result = obs.get(store, key)
        content_bytes: bytes = bytes(result.bytes())
        versions_data: dict[str, Any] = json.loads(content_bytes)

        return versions_data, etag
    except Exception as e:
        # Check if it's a "not found" error
        if "NotFound" in str(type(e).__name__) or "404" in str(e) or "NoSuchKey" in str(e):
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
    """
    assets_to_upload: list[Path] = []
    seen_hrefs: set[str] = set()

    for version_entry in versions_data.get("versions", []):
        if version_entry.get("version") not in versions_to_push:
            continue

        for asset_name, asset_data in version_entry.get("assets", {}).items():
            href = asset_data.get("href", asset_name)

            # Skip if we've already added this asset
            if href in seen_hrefs:
                continue
            seen_hrefs.add(href)

            # Resolve path relative to catalog root
            asset_path = catalog_root / href
            if asset_path.exists():
                assets_to_upload.append(asset_path.resolve())

    return assets_to_upload


def _upload_assets(
    store: ObjectStore,
    catalog_root: Path,
    prefix: str,
    assets: list[Path],
    *,
    dry_run: bool = False,
) -> tuple[int, list[str]]:
    """Upload asset files to object storage.

    Args:
        store: Object store instance.
        catalog_root: Path to catalog root (for relative path calculation).
        prefix: Prefix in object storage.
        assets: List of asset file paths to upload.
        dry_run: If True, don't actually upload.

    Returns:
        Tuple of (files_uploaded, errors).
    """
    files_uploaded = 0
    errors: list[str] = []

    for asset_path in assets:
        try:
            # Calculate relative path from catalog root
            rel_path = asset_path.relative_to(catalog_root)
            target_key = f"{prefix}/{rel_path}".lstrip("/")

            if dry_run:
                info(f"[DRY RUN] Would upload: {rel_path} -> {target_key}")
            else:
                info(f"Uploading: {rel_path}")
                obs.put(store, target_key, asset_path)
                files_uploaded += 1
                success(f"Uploaded: {rel_path}")
        except Exception as e:
            error_msg = f"Failed to upload {asset_path}: {e}"
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
    key = f"{prefix}/.portolan/collections/{collection}/versions.json".lstrip("/")
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

    # Read local versions
    local_data = _read_local_versions(catalog_root, collection)
    local_versions = [v.get("version") for v in local_data.get("versions", [])]

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
    diff = diff_versions(local_versions, remote_versions)

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
    if not diff.local_only:
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
    files_uploaded, upload_errors = _upload_assets(
        store, catalog_root, prefix, assets, dry_run=dry_run
    )

    if upload_errors:
        error("Asset upload failed, aborting push")
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
        raise PushConflictError("Remote changed during push, re-run push to try again") from e

    return PushResult(
        success=True,
        files_uploaded=files_uploaded,
        versions_pushed=len(diff.local_only),
        conflicts=[],
        errors=[],
    )
