"""Push module - sync local catalog changes to cloud object storage."""

from __future__ import annotations

import asyncio
import contextvars
import fnmatch  # Module-level import for performance (used in hot path)
import json
import posixpath
import threading
import time
import uuid
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass, field
from pathlib import Path
from typing import TYPE_CHECKING, Any

import obstore as obs
from obstore.exceptions import AlreadyExistsError, PreconditionError

from portolan_cli.async_utils import (
    AsyncIOExecutor,
    AsyncProgressReporter,
    CircuitBreakerError,
    get_default_concurrency,
)
from portolan_cli.catalog import intermediate_catalog_ids
from portolan_cli.derived_assets import is_optional_derivative
from portolan_cli.logo import LOGO_ASSETS_DIRNAME
from portolan_cli.sync.report import Report, tell
from portolan_cli.sync.upload import (
    ObjectStore,
    copy_object_async,
    put_object,
    put_object_async,
    setup_store,
)
from portolan_core.checksums import compute_checksum
from portolan_core.constants import LEGACY_GLOB_FIELD
from portolan_core.errors import PushVerifyError
from portolan_core.stac_links import published_tree
from portolan_core.trace import count, span, submit

if TYPE_CHECKING:
    from obstore import PutMode, PutResult

_KIB = 1024

__all__ = [
    "PushVersionDiff",
    "UploadMetrics",
    "VerifyReport",
    "VersionDiff",  # Deprecated alias
    "discover_collections",
    "format_file_size",
    "format_speed",
    "get_default_workers",
    "push",
    "push_all_collections",
    "push_async",
    "run_push",
]


def get_default_workers() -> int:
    """Get default number of workers for parallel operations."""
    return get_default_concurrency()


# Exceptions


class PushConflictError(Exception):
    """Raised when push detects conflict with remote state."""


# Data Classes


@dataclass
class VerifyReport:
    """Counts from ``push --verify``."""

    checked: int = 0
    mismatched: int = 0

    def merge(self, other: VerifyReport) -> None:
        """Add the counts of another report to this one."""
        self.checked += other.checked
        self.mismatched += other.mismatched


def verify_data(verify: VerifyReport | None) -> dict[str, Any]:
    """Return the ``verify`` field of the push JSON data, or no field without a verify."""
    if verify is None:
        return {}
    return {"verify": {"checked": verify.checked, "mismatched": verify.mismatched}}


@dataclass
class PushResult:
    """Result of a push operation."""

    success: bool
    files_uploaded: int
    versions_pushed: int
    conflicts: list[str] = field(default_factory=list)
    errors: list[str] = field(default_factory=list)
    metadata_errors: list[str] = field(default_factory=list)
    dry_run: bool = False
    would_push_versions: int = 0
    would_push_files: int = 0
    metrics: UploadMetrics | None = None
    verify: VerifyReport | None = None
    landed: bool = False

    @property
    def has_metadata_errors(self) -> bool:
        """True if any metadata files failed to upload."""
        return len(self.metadata_errors) > 0

    def to_dict(self) -> dict[str, Any]:
        """Return the JSON data of ``portolan push --collection``."""
        if not self.success:
            return verify_data(self.verify)
        return {
            "files_uploaded": self.files_uploaded,
            "versions_pushed": self.versions_pushed,
            "conflicts": self.conflicts,
            "errors": self.errors,
            **verify_data(self.verify),
        }


@dataclass
class PushVersionDiff:
    """Result of diffing local vs remote versions for push operations."""

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
    """Tracks upload performance metrics for summary display."""

    total_bytes: int = 0
    total_duration: float = 0.0
    file_count: int = 0
    elapsed_seconds: float = 0.0
    _lock: threading.Lock = field(default_factory=threading.Lock, repr=False)

    def record_elapsed(self, elapsed: float) -> None:
        """Record wall-clock elapsed time directly (thread-safe)."""
        with self._lock:
            self.elapsed_seconds += elapsed

    def record(self, size_bytes: int, duration_seconds: float) -> None:
        """Record metrics for a single upload (thread-safe)."""
        with self._lock:
            self.total_bytes += size_bytes
            self.total_duration += duration_seconds
            self.file_count += 1

    @property
    def average_speed(self) -> float:
        """Average upload speed in bytes per second."""
        # Prefer wall-clock time for accurate speed calculation
        if self.elapsed_seconds > 0:
            return self.total_bytes / self.elapsed_seconds
        # Fallback to total_duration (sum of individual task times)
        if self.total_duration == 0:
            return 0.0
        return self.total_bytes / self.total_duration

    def to_attrs(self) -> dict[str, float | int]:
        """Return the metrics as span attributes for ``push.assets``."""
        with self._lock:
            return {
                "files": self.file_count,
                "bytes": self.total_bytes,
                "elapsed_seconds": round(self.elapsed_seconds, 6),
                "upload_seconds_sum": round(self.total_duration, 6),
                "mb_per_s": round(self.average_speed / (1024 * 1024), 3),
            }

    def merge(self, other: UploadMetrics) -> None:
        """Merge metrics from another instance (thread-safe)."""
        with self._lock:
            self.elapsed_seconds += other.elapsed_seconds
            self.total_bytes += other.total_bytes
            self.total_duration += other.total_duration
            self.file_count += other.file_count


# Formatting Utilities


def format_file_size(size_bytes: int) -> str:
    """Format file size in human-readable form."""
    if size_bytes < _KIB:
        return f"{size_bytes} B"
    if size_bytes < 1024 * 1024:
        return f"{size_bytes / 1024:.1f} KB"
    if size_bytes < 1024 * 1024 * 1024:
        return f"{size_bytes / (1024 * 1024):.1f} MB"
    return f"{size_bytes / (1024 * 1024 * 1024):.1f} GB"


def format_speed(bytes_per_second: float) -> str:
    """Format upload speed in human-readable form."""
    if bytes_per_second < _KIB:
        return f"{int(bytes_per_second)} B/s"
    if bytes_per_second < 1024 * 1024:
        return f"{bytes_per_second / 1024:.1f} KiB/s"
    if bytes_per_second < 1024 * 1024 * 1024:
        return f"{bytes_per_second / (1024 * 1024):.1f} MiB/s"
    return f"{bytes_per_second / (1024 * 1024 * 1024):.1f} GiB/s"


# Glob Asset Transformation


def _transform_collection_glob_assets(
    content: bytes,
    destination: str,
    collection_path: str,
) -> bytes:
    """Transform collection.json to populate the glob field for partitioned assets."""
    try:
        data = json.loads(content)
    except json.JSONDecodeError:
        return content  # Return unchanged if not valid JSON

    assets = data.get("assets", {})
    modified = False

    for asset_data in assets.values():
        href = asset_data.get("href", "")
        # Check if this is a glob pattern (contains *)
        if "*" in href:
            # href is relative to collection.json, such as "./kdtree_cell=*/*.parquet".
            glob_pattern = href.lstrip("./")
            # Build URL preserving protocol separator
            base = destination.rstrip("/")
            remote_glob = f"{base}/{collection_path}/{glob_pattern}"

            # Always overwrite to keep the field in sync with the current remote
            if asset_data.get("partition:glob") != remote_glob:
                asset_data["partition:glob"] = remote_glob
                modified = True
            if LEGACY_GLOB_FIELD in asset_data:
                del asset_data[LEGACY_GLOB_FIELD]
                modified = True

    if modified:
        return json.dumps(data, indent=2).encode("utf-8")
    return content


# Version Diffing


def diff_version_lists(local_versions: list[str], remote_versions: list[str]) -> PushVersionDiff:
    """Compute diff between local and remote version string lists."""
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


# Local Versions Reading


def _read_local_versions(catalog_root: Path, collection: str) -> dict[str, Any]:
    """Read local versions.json for a collection."""
    # versions.json at collection root
    versions_path = catalog_root / collection / "versions.json"

    if not versions_path.exists():
        raise FileNotFoundError(f"versions.json not found: {versions_path}")

    try:
        count("versions_json.reads")
        data: dict[str, Any] = json.loads(versions_path.read_text(encoding="utf-8"))
        return data
    except json.JSONDecodeError as e:
        raise ValueError(f"Invalid JSON in versions.json: {e}") from e


# Remote Versions Fetching


def _parse_remote_versions(content_bytes: bytes, meta: Any) -> tuple[dict[str, Any], str | None]:
    """Parse a fetched remote versions.json and read its ETag.

    The ETag comes from the same get() result, which avoids a TOCTOU race.
    """
    etag = meta.get("e_tag") if meta else None
    count("versions_json.reads")
    versions_data: dict[str, Any] = json.loads(content_bytes)
    return versions_data, etag


def _is_not_found(e: Exception) -> bool:
    """Return True for a "not found" error. Cloud providers report it differently."""
    error_str = str(e).lower()
    error_type = type(e).__name__.lower()
    return any(
        x in error_str or x in error_type
        for x in ["notfound", "404", "nosuchkey", "does not exist"]
    )


# Asset Upload


def _current_snapshot(versions_data: dict[str, Any] | None) -> dict[str, Any] | None:
    """Return the current version entry of a versions.json document."""
    if versions_data is None:
        return None

    versions: list[dict[str, Any]] = versions_data.get("versions", [])
    if not versions:
        return None

    current_name = versions_data.get("current_version")
    return next((v for v in versions if v.get("version") == current_name), versions[-1])


def _snapshot_entries(snapshot: dict[str, Any] | None) -> dict[str, dict[str, Any]]:
    """Map each href in a version snapshot to its asset entry."""
    if snapshot is None:
        return {}

    entries: dict[str, dict[str, Any]] = {}
    for asset_name, asset_data in snapshot.get("assets", {}).items():
        entries[asset_data.get("href", asset_name)] = asset_data
    return entries


def _snapshot_hashes(snapshot: dict[str, Any] | None) -> dict[str, str | None]:
    """Map each href in a version snapshot to its sha256."""
    return {href: entry.get("sha256") for href, entry in _snapshot_entries(snapshot).items()}


def _matches_remote(local_sha256: str | None, remote_sha256: str | None) -> bool:
    """Return True when the remote current snapshot records the local bytes."""
    return bool(local_sha256) and remote_sha256 == local_sha256


def _skipped_entries(
    versions_data: dict[str, Any],
    remote_versions_data: dict[str, Any] | None,
) -> dict[str, dict[str, Any]]:
    """Return the local current entries that the snapshot diff skips."""
    remote_assets = _snapshot_hashes(_current_snapshot(remote_versions_data))
    return {
        href: entry
        for href, entry in _snapshot_entries(_current_snapshot(versions_data)).items()
        if _matches_remote(entry.get("sha256"), remote_assets.get(href))
    }


def _skipped_asset_sizes(
    versions_data: dict[str, Any],
    remote_versions_data: dict[str, Any] | None,
) -> dict[str, int | None]:
    """Map each href that the snapshot diff skips to its recorded size."""
    sizes: dict[str, int | None] = {}
    for href, entry in _skipped_entries(versions_data, remote_versions_data).items():
        size = entry.get("size_bytes")
        sizes[href] = size if isinstance(size, int) else None
    return sizes


async def _verify_skipped_assets_async(
    store: ObjectStore,
    prefix: str,
    versions_data: dict[str, Any],
    remote_versions_data: dict[str, Any] | None,
    *,
    concurrency: int,
    report: Report | None = None,
) -> tuple[dict[str, str | None], VerifyReport]:
    """Find the skipped assets whose remote object disagrees with the manifest."""
    sizes = _skipped_asset_sizes(versions_data, remote_versions_data)
    if not sizes:
        return {}, VerifyReport()

    async def disagrees(href: str) -> tuple[bool, str | None]:
        key = f"{prefix}/{href}".lstrip("/")
        try:
            meta = await obs.head_async(store, key)
        except FileNotFoundError:
            return True, None
        expected = sizes[href]
        mismatched = expected is not None and meta["size"] != expected
        etag = meta.get("e_tag")
        if mismatched and not etag:
            raise PushVerifyError([f"{href}: cannot safely repair an object without an ETag"])
        return mismatched, etag

    executor = AsyncIOExecutor[tuple[bool, str | None]](
        concurrency=concurrency, circuit_breaker_threshold=5
    )
    try:
        results = await executor.execute(items=list(sizes), operation=disagrees)
    except CircuitBreakerError as e:
        raise PushVerifyError([f"too many failed HEAD requests: {e}"]) from e

    failures = [f"{r.item}: {r.error}" for r in results if r.error is not None]
    if failures:
        raise PushVerifyError(failures)

    mismatched = {r.item: r.result[1] for r in results if r.result and r.result[0]}
    counts = VerifyReport(checked=len(sizes), mismatched=len(mismatched))
    tell(
        report,
        "info",
        f"Verified {counts.checked} skipped asset(s) against the store, "
        f"{counts.mismatched} missing or with another size",
    )
    return mismatched, counts


def _resolve_local_asset(
    catalog_root: Path, href: str, version_str: str | None, *, report: Report | None = None
) -> Path | None:
    """Return the local file of an asset to upload, or None to skip it."""
    asset_path = catalog_root / href
    if asset_path.exists():
        return asset_path.resolve()
    # The spec calls visual, thumbnail, style and collection-mirror optional derivatives.
    # An absent one must not fail a push. data and source stay a hard error.
    if is_optional_derivative(catalog_root, href):
        tell(report, "warn", f"Optional asset not found, skipped: {href} (version {version_str})")
        return None
    raise FileNotFoundError(f"Asset referenced in version {version_str} not found: {href}")


def _get_assets_to_upload(
    catalog_root: Path,
    versions_data: dict[str, Any],
    remote_versions_data: dict[str, Any] | None = None,
    *,
    reupload: frozenset[str] = frozenset(),
    report: Report | None = None,
) -> list[Path]:
    """Get list of asset files that need to be uploaded."""
    local_current = _current_snapshot(versions_data)
    version_str = local_current.get("version") if local_current else None
    has_remote = bool(_snapshot_entries(_current_snapshot(remote_versions_data)))
    # Skip only when the remote current snapshot has the same sha256 at the same href.
    # A renamed file still uploads. ``push --verify`` checks the same set.
    skipped = _skipped_entries(versions_data, remote_versions_data)

    assets_to_upload: list[Path] = []
    skipped_count = 0

    for href in _snapshot_entries(local_current):
        if href in skipped and href not in reupload:
            skipped_count += 1
            continue
        asset_path = _resolve_local_asset(catalog_root, href, version_str, report=report)
        if asset_path is not None:
            assets_to_upload.append(asset_path)

    # Log diffing results for user feedback
    new_count = len(assets_to_upload)
    if skipped_count > 0 or (has_remote and new_count > 0):
        tell(
            report,
            "info",
            f"Uploading {new_count} new/changed asset(s), skipping {skipped_count} unchanged",
        )

    return assets_to_upload


def _get_reupload_assets(
    catalog_root: Path,
    versions_data: dict[str, Any],
    reupload: frozenset[str],
    *,
    report: Report | None = None,
) -> list[Path]:
    """Return the local files of the hrefs that ``push --verify`` flagged."""
    local_current = _current_snapshot(versions_data)
    version_str = local_current.get("version") if local_current else None
    paths: list[Path] = []
    for href, entry in _snapshot_entries(local_current).items():
        if href in reupload:
            asset_path = _resolve_local_asset(catalog_root, href, version_str, report=report)
            if asset_path is not None:
                if compute_checksum(asset_path) != entry.get("sha256"):
                    raise PushVerifyError([f"{href}: local checksum differs from the manifest"])
                paths.append(asset_path)
    return paths


def _repaired_count(prefix: str, reupload: frozenset[str], uploaded_keys: list[str]) -> int:
    """Count the flagged hrefs that push uploaded."""
    written = set(uploaded_keys)
    return sum(1 for href in reupload if f"{prefix}/{href}".lstrip("/") in written)


def _with_metrics(msg: str, metrics: UploadMetrics) -> str:
    """Append the upload size and speed to a summary line when push sent bytes."""
    if metrics.total_bytes > 0:
        msg += f" ({format_file_size(metrics.total_bytes)}, {format_speed(metrics.average_speed)})"
    return msg


# One push or push --all sends each of these notices once. push --all sets a
# fresh set, and the collection tasks inherit it.
_run_notices: contextvars.ContextVar[set[str] | None] = contextvars.ContextVar(
    "push_run_notices", default=None
)

_KEYS_UNREADABLE_NOTICE = (
    "Cannot read which remote keys exist before the upload, so a failed push "
    "keeps every key it wrote. On S3, grant s3:ListBucket for a full rollback."
)


def _warn_once(message: str, *, report: Report | None = None) -> None:
    """Print ``message`` once for each push run."""
    seen = _run_notices.get()
    if seen is not None:
        if message in seen:
            return
        seen.add(message)
    tell(report, "warn", message)


def _target_key(catalog_root: Path, prefix: str, path: Path) -> str:
    """Return the object key of a local catalog file."""
    return f"{prefix}/{path.relative_to(catalog_root).as_posix()}".lstrip("/")


async def _existing_keys(
    store: ObjectStore,
    keys: list[str],
    list_prefix: str,
    concurrency: int,
    *,
    report: Report | None = None,
) -> dict[str, int | None]:
    """Return the size of each key that exists in the store before the upload."""
    below = f"{list_prefix}/"
    existing: dict[str, int | None] = {}
    try:
        listed = {
            str(m["path"]): int(m["size"])
            async for page in obs.list(store, prefix=list_prefix)
            for m in page
        }
        existing.update((k, listed[k]) for k in keys if k in listed)
        probe = [k for k in keys if not k.startswith(below)]
    except Exception:
        _warn_once(_KEYS_UNREADABLE_NOTICE, report=report)
        probe = keys
    semaphore = asyncio.Semaphore(concurrency)
    unreadable: list[str] = []

    async def size_of(key: str) -> None:
        async with semaphore:
            try:
                existing[key] = int((await obs.head_async(store, key))["size"])
            except Exception as e:
                if not (isinstance(e, FileNotFoundError) or _is_not_found(e)):
                    unreadable.append(key)
                    existing[key] = None

    await asyncio.gather(*(size_of(k) for k in probe))
    if unreadable:
        _warn_once(_KEYS_UNREADABLE_NOTICE, report=report)
    return existing


async def _back_up(
    store: ObjectStore,
    sizes: dict[str, int | None],
    prefix: str,
    concurrency: int,
    *,
    report: Report | None = None,
) -> dict[str, str]:
    """Copy each key that the push will overwrite to a backup key."""
    base = f"{prefix}/.portolan/push-backup/{uuid.uuid4().hex}".lstrip("/")
    semaphore = asyncio.Semaphore(concurrency)
    stopped = False

    async def copy(key: str) -> str | None:
        async with semaphore:
            if stopped:
                return None
            try:
                await copy_object_async(store, key, f"{base}/{key}", sizes[key])
                return f"{base}/{key}"
            except Exception as e:
                if not (isinstance(e, FileNotFoundError) or _is_not_found(e)):
                    tell(
                        report,
                        "warn",
                        f"Cannot back up {key}, a failed push leaves its new bytes: {e}",
                    )
                return None

    keys = list(sizes)
    tasks = [asyncio.ensure_future(copy(k)) for k in keys]
    try:
        copies = await asyncio.gather(*tasks)
    except BaseException:
        # A started copy lands even when the client stops waiting, and nothing
        # reclaims it. Let started copies finish, start no more, then delete every backup key.
        stopped = True
        await asyncio.gather(*tasks, return_exceptions=True)
        for key in keys:
            try:
                await obs.delete_async(store, f"{base}/{key}")
            except Exception as e:
                # A 404 for the bucket can leave the copy in place (Source
                # Cooperative answers a bulk delete on an account with 404).
                gone = isinstance(e, FileNotFoundError) or _is_not_found(e)
                if not (gone and not await _key_exists(store, f"{base}/{key}")):
                    tell(report, "warn", f"Failed to delete the backup of {key}: {e}")
        raise
    return {key: backup for key, backup in zip(keys, copies, strict=True) if backup}


async def _drop_backups(
    store: ObjectStore,
    backups: dict[str, str],
    sizes: dict[str, int | None],
    *,
    restore_over: dict[str, str | None] | None = None,
    report: Report | None = None,
) -> list[str]:
    """Delete each backup key, and return a message for each key that failed."""
    if restore_over is not None and backups:
        tell(report, "warn", f"Restoring {len(backups)} overwritten key(s) from the backups...")
    problems: list[str] = []
    for key, backup in backups.items():
        step = f"restore {key} from {backup}"
        try:
            if (
                restore_over is not None
                and key in restore_over
                and await _remote_etag(store, key) == restore_over[key]
            ):
                await copy_object_async(store, backup, key, sizes.get(key))
            step = f"delete the backup {backup} of {key}"
            await obs.delete_async(store, backup)
        except Exception as e:
            problems.append(f"Could not {step}: {e}")
            tell(report, "warn", problems[-1])
    return problems


async def _keys_to_restore(
    store: ObjectStore,
    prefix: str,
    collection: str,
    local_data: dict[str, Any],
    written: dict[str, str | None],
) -> dict[str, str | None]:
    """Return the keys that a push restores after it lost the versions.json race."""
    try:
        remote, _ = await _fetch_remote_versions_async(store, prefix, collection)
    except Exception:
        return written
    ours = _snapshot_hashes(_current_snapshot(local_data))
    theirs = _snapshot_hashes(_current_snapshot(remote))
    named = {f"{prefix}/{href}".lstrip("/"): ours.get(href) != sha for href, sha in theirs.items()}
    return {key: etag for key, etag in written.items() if named.get(key, ours != theirs)}


def _cleanup_uploaded_assets(
    store: ObjectStore, uploaded_keys: list[str], *, report: Report | None = None
) -> list[str]:
    """Delete the keys that a failed push created, and report each key that stays."""
    if uploaded_keys:
        tell(report, "warn", f"Rolling back {len(uploaded_keys)} uploaded asset(s)...")
    problems: list[str] = []
    for key in uploaded_keys:
        try:
            obs.delete(store, key)
            tell(report, "detail", f"Deleted: {key}")
        except Exception as e:
            problems.append(f"Could not delete {key}: {e}")
            tell(report, "warn", problems[-1])
    return problems


# STAC Metadata File Discovery and Upload


def _discover_stac_files(
    catalog_root: Path,
    collection: str,
    *,
    include_catalog: bool = False,
    report: Report | None = None,
) -> dict[str, list[Path]]:
    """Discover STAC metadata files that should be uploaded for a collection."""
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
        # Intermediate catalog files only for a standalone push (include_catalog=True).
        # A catalog-wide push uploads them once after all collections succeed.
        for inter_file in _discover_intermediate_catalog_files(
            catalog_root, [collection], report=report
        ):
            key = "readmes" if inter_file.name == "README.md" else "catalog"
            stac_files[key].append(inter_file)

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

    # 3. All item STAC files: {item_id}/{item_id}.json, not item.json.
    visited_paths: set[Path] = set()

    for item_dir in collection_dir.iterdir():
        # Skip non-directories and hidden directories
        if not item_dir.is_dir() or item_dir.name.startswith("."):
            continue

        # Symlink safety: resolve and detect cycles (matches discover_collections)
        try:
            resolved = item_dir.resolve()
        except OSError:
            tell(report, "warn", f"Cannot resolve path {item_dir}, skipping")
            continue

        if resolved in visited_paths:
            tell(report, "warn", f"Symlink cycle detected at {item_dir}, skipping")
            continue
        visited_paths.add(resolved)

        # Look for {item_id}.json where item_id = directory name
        item_id = item_dir.name
        item_json = item_dir / f"{item_id}.json"
        if item_json.exists():
            stac_files["items"].append(item_json)

    return stac_files


# Metadata File Discovery


def _matches_exclude_pattern(path: Path, pattern: str, base_dir: Path) -> bool:
    """Check if a path matches an exclusion pattern."""
    # Get relative path for pattern matching
    try:
        rel_path = path.relative_to(base_dir)
    except ValueError:
        return False

    rel_str = str(rel_path)
    name = path.name

    # ".portolan/" and ".portolan/*" both exclude anything under a directory named .portolan.
    if pattern.endswith(("/", "/*")):
        dir_name = pattern.rstrip("/").rstrip("*").rstrip("/")
        # Check if any path component matches the directory name
        return any(part == dir_name for part in rel_path.parts)

    # Glob pattern (contains *)
    if "*" in pattern:
        # Match against filename
        if fnmatch.fnmatch(name, pattern):
            return True
        # Also match against relative path for patterns like "**/*.py"
        return bool(fnmatch.fnmatch(rel_str, pattern))

    # Exact match against filename
    return name == pattern


def _get_exclude_patterns(catalog_root: Path | None = None) -> list[str]:
    """Get exclusion patterns from config or defaults."""
    from portolan_core.config import DEFAULT_SETTINGS, get_setting

    if catalog_root is None:
        return list(DEFAULT_SETTINGS.get("push.exclude", []))

    patterns = get_setting("push.exclude", catalog_path=catalog_root)
    if patterns is None:
        return list(DEFAULT_SETTINGS.get("push.exclude", []))
    return list(patterns)


# A file at or above this size uploads as a multipart transfer with chunk concurrency.
MULTIPART_THRESHOLD = 5 * 1024 * 1024

# Security: always excluded and merged with user patterns. No config can override them.
_SECURITY_EXCLUDE_PATTERNS: frozenset[str] = frozenset(
    [
        ".env",  # Environment files with secrets
        ".env.*",  # Environment file variants (.env.local, .env.production)
        ".git/",  # Git repository data
        ".portolan/",  # Internal Portolan state
    ]
)


def _get_effective_exclude_patterns(
    catalog_root: Path | None = None,
    additional_patterns: list[str] | None = None,
) -> list[str]:
    """Get effective exclusion patterns, always including security-critical ones."""
    # Start with security-critical patterns (always included)
    patterns: set[str] = set(_SECURITY_EXCLUDE_PATTERNS)

    # Add config/default patterns
    config_patterns = _get_exclude_patterns(catalog_root)
    patterns.update(config_patterns)

    # Add any additional patterns
    if additional_patterns:
        patterns.update(additional_patterns)

    return list(patterns)


def _load_versioned_asset_paths(catalog_root: Path, collection: str) -> set[str]:
    """Load versioned asset relative paths from versions.json."""
    versions_file = catalog_root / collection / "versions.json"
    if not versions_file.exists():
        return set()

    try:
        count("versions_json.reads")
        data = json.loads(versions_file.read_text(encoding="utf-8"))
        versioned_paths: set[str] = set()

        # Extract asset paths from all versions
        for version_entry in data.get("versions", []):
            assets = version_entry.get("assets", {})
            for asset_key, asset_info in assets.items():
                # asset_key is typically the relative path like "data.parquet"
                # or can be nested like "v1/data.parquet"
                if isinstance(asset_info, dict):
                    # Check for explicit href if available
                    href = asset_info.get("href", asset_key)
                    versioned_paths.add(href)
                versioned_paths.add(asset_key)

        return versioned_paths
    except (json.JSONDecodeError, KeyError, TypeError):
        # Invalid versions.json - return empty set, don't fail discovery
        return set()


# A file with one of these names is uploaded by its own phase, in a fixed order
# that keeps the push atomic.
_HANDLED_SEPARATELY = frozenset(
    {
        "versions.json",  # Uploaded last (manifest-last)
        "collection.json",  # Uploaded by _upload_stac_files
        "catalog.json",  # Uploaded by _push_all_upload_root_files
        "README.md",  # Uploaded after versions.json
    }
)


@dataclass(frozen=True)
class _ExcludeContext:
    """What _should_exclude_from_push needs to judge one file."""

    exclude_patterns: list[str]
    versioned_asset_paths: set[str]
    collection: str | None
    item_level: bool = True


def _is_versioned_asset(path: Path, base_dir: Path, ctx: _ExcludeContext) -> bool:
    """Report whether versions.json already names this file as an asset."""
    if not ctx.versioned_asset_paths:
        return False
    try:
        rel_posix = path.relative_to(base_dir).as_posix()
    except ValueError:
        return False
    if rel_posix in ctx.versioned_asset_paths or path.name in ctx.versioned_asset_paths:
        return True
    if ctx.collection is None:
        return False
    return rel_posix.removeprefix(f"{ctx.collection}/") in ctx.versioned_asset_paths


def _is_stac_item_file(path: Path, ctx: _ExcludeContext) -> bool:
    """Report whether _discover_stac_files already found this file."""
    # A STAC item lives at collection/item_dir/item_dir.json, so the JSON
    # filename matches its parent directory name.
    return ctx.item_level and path.suffix == ".json" and path.parent.name == path.stem


def _should_exclude_from_push(path: Path, base_dir: Path, ctx: _ExcludeContext) -> bool:
    """Report whether the catalog-file discovery must skip this file."""
    if path.name in _HANDLED_SEPARATELY:
        return True
    if _is_versioned_asset(path, base_dir, ctx):
        return True
    if _is_stac_item_file(path, ctx):
        return True
    return any(
        _matches_exclude_pattern(path, pattern, base_dir) for pattern in ctx.exclude_patterns
    )


def _discover_collection_dir_files(
    catalog_root: Path, collection: str, ctx: _ExcludeContext
) -> list[Path]:
    """Walk one collection directory for files to sync."""
    collection_dir = catalog_root / collection
    if not collection_dir.exists():
        return []
    return [
        item
        for item in collection_dir.rglob("*")
        if not item.is_symlink()
        and item.is_file()
        and not _should_exclude_from_push(item, catalog_root, ctx)
    ]


def _discover_root_dir_files(catalog_root: Path, ctx: _ExcludeContext) -> list[Path]:
    """Collect the root-level files to sync, and the logo in _assets/."""
    discovered = [
        item
        for item in catalog_root.iterdir()
        if not item.is_dir()
        and not item.is_symlink()
        and item.is_file()
        and not _should_exclude_from_push(item, catalog_root, ctx)
    ]

    assets_dir = catalog_root / LOGO_ASSETS_DIRNAME
    if assets_dir.is_dir() and not assets_dir.is_symlink():
        discovered.extend(
            item
            for item in sorted(assets_dir.rglob("*"))
            if not item.is_symlink()
            and item.is_file()
            and not _should_exclude_from_push(item, catalog_root, ctx)
        )
    return discovered


def _discover_catalog_files(
    catalog_root: Path,
    collection: str | None = None,
    *,
    include_catalog_root: bool = False,
    additional_exclude_patterns: list[str] | None = None,
    report: Report | None = None,
) -> list[Path]:
    """Discover the catalog files that should be synced to remote."""
    versioned_asset_paths: set[str] = set()
    if collection is not None:
        # _upload_assets_async already handles every asset versions.json names.
        versioned_asset_paths = _load_versioned_asset_paths(catalog_root, collection)

    ctx = _ExcludeContext(
        # Always use the effective patterns. They include the security ones.
        exclude_patterns=_get_effective_exclude_patterns(catalog_root, additional_exclude_patterns),
        versioned_asset_paths=versioned_asset_paths,
        collection=collection,
    )

    discovered: list[Path] = []
    documents: list[str] = []
    if collection is not None:
        discovered.extend(_discover_collection_dir_files(catalog_root, collection, ctx))
        documents.append(f"{collection}/collection.json")
    if include_catalog_root:
        discovered.extend(_discover_root_dir_files(catalog_root, ctx))
        documents.append("catalog.json")
    return _named_only(catalog_root, discovered, documents, report=report)


def _named_only(
    catalog_root: Path, files: list[Path], documents: list[str], *, report: Report | None = None
) -> list[Path]:
    """Keep the files that ``documents`` or their items name, and warn about the rest."""
    named = published_tree(catalog_root, documents)
    kept = [f for f in files if f.relative_to(catalog_root).as_posix() in named]
    strays = sorted(f.relative_to(catalog_root).as_posix() for f in files if f not in kept)
    if strays:
        _warn_once(
            f"Not pushed, because no STAC document names it: {', '.join(strays)}", report=report
        )
    return kept


def _stat_files_safely(files: list[Path], errors: list[str]) -> tuple[dict[Path, int], list[Path]]:
    """Stat files safely, recording errors for files that can't be stat'd."""
    file_sizes: dict[Path, int] = {}
    for f in files:
        try:
            file_sizes[f] = f.stat().st_size
        except OSError as e:
            errors.append(f"Failed to stat {f}: {e}")
    uploadable = [f for f in files if f in file_sizes]
    return file_sizes, uploadable


def _handle_push_dry_run(
    catalog_root: Path,
    collection: str,
    local_data: dict[str, Any],
    local_versions: list[str],
    *,
    verify: bool = False,
    report: Report | None = None,
) -> PushResult:
    """Handle push dry-run mode: show what would be pushed without network I/O."""
    # Try to get assets, but don't fail if some are missing (dry-run should be forgiving)
    try:
        assets = _get_assets_to_upload(catalog_root, local_data, report=report)
        asset_count = len(assets)
        asset_paths = [asset.relative_to(catalog_root) for asset in assets]
        missing_assets: list[str] = []
    except FileNotFoundError as e:
        # Asset file is missing - warn but continue with dry-run
        tell(report, "warn", f"[DRY RUN] Warning: {e}")
        asset_count = 0
        asset_paths = []
        missing_assets = [str(e)]

    tell(
        report,
        "info",
        f"[DRY RUN] Would push up to {len(local_versions)} version(s): {local_versions}",
    )
    tell(report, "info", f"[DRY RUN] Would upload up to {asset_count} asset file(s)")
    for rel_path in asset_paths:
        tell(report, "detail", f" {rel_path}")

    # Show metadata files that would be synced
    metadata_files = _discover_catalog_files(
        catalog_root,
        collection,
        include_catalog_root=True,
        report=report,  # Dry-run shows complete picture
    )
    if metadata_files:
        tell(report, "info", f"[DRY RUN] Would sync {len(metadata_files)} metadata file(s)")
        for f in metadata_files:
            rel_path = f.relative_to(catalog_root)
            tell(report, "detail", f" {rel_path}")

    # Show intermediate catalog files for nested collections
    intermediate_files = _discover_intermediate_catalog_files(
        catalog_root, [collection], report=report
    )
    if intermediate_files:
        tell(
            report,
            "info",
            f"[DRY RUN] Would upload {len(intermediate_files)} intermediate catalog file(s)",
        )
        for f in intermediate_files:
            tell(report, "detail", f" {f.relative_to(catalog_root).as_posix()}")

    tell(report, "warn", "[DRY RUN] Remote conflict detection skipped (requires network)")
    if verify:
        # A HEAD is a network request, so a dry run never sends one.
        tell(report, "warn", "[DRY RUN] --verify skipped (requires network)")
    tell(report, "warn", "[DRY RUN] Actual versions pushed may be fewer if remote already has some")

    return PushResult(
        success=True,
        files_uploaded=0,
        versions_pushed=0,
        conflicts=[],
        errors=missing_assets,
        dry_run=True,
        would_push_versions=len(local_versions),
        would_push_files=asset_count,
    )


# Main Push Function


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
    verify: bool = False,
    verbose: bool = False,
    report: Report | None = None,
    show_progress: bool = False,
) -> PushResult:
    """Push local catalog changes to cloud object storage (sync wrapper)."""
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
            verify=verify,
            verbose=verbose,
            report=report,
            show_progress=show_progress,
        )
    )


# Async Push Implementation


async def _fetch_remote_versions_async(
    store: ObjectStore,
    prefix: str,
    collection: str,
) -> tuple[dict[str, Any] | None, str | None]:
    """Async version of _fetch_remote_versions."""
    key = f"{prefix}/{collection}/versions.json".lstrip("/")

    try:
        # Use obstore's async get
        result = await obs.get_async(store, key)
        return _parse_remote_versions(bytes(await result.bytes_async()), result.meta)
    except FileNotFoundError:
        return None, None
    except Exception as e:
        if _is_not_found(e):
            return None, None
        raise


async def _upload_assets_async(  # noqa: PLR0915 - each transport option is threaded through each layer
    store: ObjectStore,
    catalog_root: Path,
    prefix: str,
    assets: list[Path],
    *,
    concurrency: int = 50,
    chunk_concurrency: int = 4,
    show_progress: bool = False,
    adaptive: bool = True,
    written: dict[str, str | None] | None = None,
    conditions: dict[str, str | None] | None = None,
    verbose: bool = False,
    report: Report | None = None,
) -> tuple[int, list[str], list[str], UploadMetrics]:
    """Upload asset files to object storage with async concurrent uploads."""
    metrics = UploadMetrics()
    written = {} if written is None else written

    if not assets:
        return 0, [], [], metrics

    total = len(assets)
    total_bytes = sum(p.stat().st_size for p in assets)

    # Pre-cache file sizes to avoid double stat() calls
    file_sizes: dict[Path, int] = {p: p.stat().st_size for p in assets}

    uploaded_keys: list[str] = []
    errors_list: list[str] = []

    def put_options(key: str) -> dict[str, Any]:
        return {} if conditions is None else {"mode": _manifest_mode(conditions[key], force=False)}

    def _upload_large_file_sync(
        file_path: Path, target_key: str, max_conc: int
    ) -> tuple[str, int, float]:
        """Upload a large file in the worker thread."""
        size_bytes = file_sizes[file_path]
        start = time.perf_counter()
        with span(
            "push.upload_file", key=target_key, bytes=size_bytes, multipart=conditions is None
        ):
            put = put_object(
                store, target_key, file_path, max_concurrency=max_conc, **put_options(target_key)
            )
        written[target_key] = put.get("e_tag")
        duration = time.perf_counter() - start
        return target_key, size_bytes, duration

    async def upload_one(asset_path_str: str) -> tuple[str, int, float]:
        """Upload a single asset, using appropriate method based on size."""
        asset_path = Path(asset_path_str)
        target_key = _target_key(catalog_root, prefix, asset_path)

        size_bytes = file_sizes[asset_path]

        if size_bytes >= MULTIPART_THRESHOLD:
            # Large file: sync multipart put() in a thread, to respect chunk_concurrency.
            # trace.submit hands the open span to the worker thread.
            return await asyncio.wrap_future(
                submit(
                    thread_pool,
                    _upload_large_file_sync,
                    asset_path,
                    target_key,
                    chunk_concurrency,
                )
            )
        # Small file: use put_async (more efficient, no multipart needed)
        start = time.perf_counter()
        with span("push.upload_file", key=target_key, bytes=size_bytes, multipart=False):
            put = await put_object_async(
                store, target_key, asset_path.read_bytes(), **put_options(target_key)
            )
        written[target_key] = put.get("e_tag")
        duration = time.perf_counter() - start
        return target_key, size_bytes, duration

    asset_strs = [str(p) for p in assets]

    # Suppress progress bar when verbose (verbose replaces progress bar)
    async with AsyncProgressReporter(
        total_files=total,
        total_bytes=total_bytes,
        json_mode=not show_progress or verbose,
    ) as reporter:

        def on_complete(
            item: str,
            result: tuple[str, int, float] | None,
            err: str | None,
            completed: int,
            total_count: int,
        ) -> None:
            """Track results (called from executor)."""
            rel_path = Path(item).relative_to(catalog_root).as_posix()
            if err:
                errors_list.append(f"Failed to upload {rel_path}: {err}")
                tell(report, "error", f"Failed: {rel_path} - {err}")
            elif result:
                target_key, size_bytes, duration = result
                uploaded_keys.append(target_key)
                metrics.record(size_bytes, duration)
                reporter.advance(bytes_uploaded=size_bytes)
                # Verbose mode: print per-file details
                if verbose:
                    speed = size_bytes / duration if duration > 0 else 0
                    tell(
                        report,
                        "detail",
                        f"Uploaded ({completed}/{total_count}): {rel_path} ({format_file_size(size_bytes)}, {format_speed(speed)})",
                    )

        # Create adaptive concurrency manager for slow-start
        adaptive_manager = None
        if adaptive:
            from portolan_cli.async_utils import AdaptiveConcurrencyManager

            adaptive_manager = AdaptiveConcurrencyManager(
                max_concurrency=concurrency,
                initial_concurrency=min(2, concurrency),
            )

        executor = AsyncIOExecutor[tuple[str, int, float]](
            concurrency=adaptive_manager.current_concurrency if adaptive_manager else concurrency,
            circuit_breaker_threshold=5,
            adaptive_manager=adaptive_manager,
        )

        # Worker threads for the large files. upload_one reads this name.
        thread_pool = ThreadPoolExecutor(max_workers=concurrency)
        try:
            await executor.execute(
                items=asset_strs,
                operation=upload_one,
                on_complete=on_complete,
            )
        except CircuitBreakerError as e:
            errors_list.append(f"Circuit breaker tripped: {e}")
            tell(report, "error", f"Too many consecutive failures, aborting: {e}")
        finally:
            thread_pool.shutdown(wait=True)

        metrics.record_elapsed(reporter.elapsed_seconds)

    return len(uploaded_keys), errors_list, uploaded_keys, metrics


async def _upload_stac_files_async(  # noqa: PLR0915 - each transport option is threaded through each layer
    store: ObjectStore,
    catalog_root: Path,
    prefix: str,
    stac_files: dict[str, list[Path]],
    *,
    destination: str,
    concurrency: int = 50,
    show_progress: bool = False,
    written: dict[str, str | None] | None = None,
    root_etags: dict[str, str | None] | None = None,
    verbose: bool = False,
    report: Report | None = None,
) -> tuple[int, list[str], list[str]]:
    """Upload STAC metadata files asynchronously."""
    written = {} if written is None else written
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
                target_key = _target_key(catalog_root, prefix, file_path)
                file_size = file_sizes[file_path]

                content = file_path.read_bytes()

                # Transform collection.json to populate partition:glob
                if file_path.name == "collection.json":
                    # Extract collection path (e.g., "buildings" from "buildings/collection.json")
                    collection_path = rel_path.parent.as_posix()
                    content = _transform_collection_glob_assets(
                        content, destination, collection_path
                    )

                put = await _put_file(store, target_key, content, root_etags, report=report)
                if put is not None:
                    written[target_key] = put.get("e_tag")
                return target_key, file_size
            except Exception as e:
                error_msg = f"Failed to upload {file_path}: {e}"
                errors.append(error_msg)
                tell(report, "error", error_msg)
                return None

    # Suppress progress bar when verbose (verbose replaces progress bar)
    async with AsyncProgressReporter(
        total_files=len(uploadable_files),
        total_bytes=total_bytes,
        json_mode=not show_progress or verbose,
    ) as reporter:
        completed = 0
        total_count = len(uploadable_files)

        def log_verbose(file_path: Path, size: int) -> None:
            """Report one uploaded STAC file in verbose mode."""
            nonlocal completed
            completed += 1
            if verbose:
                rel_path = file_path.relative_to(catalog_root)
                tell(
                    report,
                    "detail",
                    f"Uploaded STAC ({completed}/{total_count}): {rel_path} ({format_file_size(size)})",
                )

        async def upload_wave(files: list[Path]) -> None:
            """Upload one wave of files in parallel and record each result."""
            if not files:
                return
            results = await asyncio.gather(*[upload_one(f) for f in files])
            for i, result in enumerate(results):
                if result is None:
                    continue
                key, size = result
                uploaded_keys.append(key)
                reporter.advance(bytes_uploaded=size)
                log_verbose(files[i], size)

        # Wave 1: every item STAC file, in parallel.
        await upload_wave(item_files)
        # Wave 2: collection.json, after the items complete.
        await upload_wave(collection_files)
        # Wave 3: catalog.json last, one file at a time (manifest-last pattern).
        for file_path in catalog_files:
            await upload_wave([file_path])

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
    """Upload versions.json with optimistic locking (async version)."""
    key = f"{prefix}/{collection}/versions.json".lstrip("/")
    # Use compact JSON for remote STAC files (saves bandwidth)
    content = json.dumps(versions_data, separators=(",", ":")).encode("utf-8")
    await _put_manifest(store, key, content, etag, force=force)


def _manifest_mode(etag: str | None, *, force: bool) -> PutMode | None:
    """Return the put mode for a versions.json write."""
    if force:
        return None
    return {"e_tag": etag} if etag else "create"


async def _put_manifest(
    store: ObjectStore, key: str, content: bytes, etag: str | None, *, force: bool
) -> None:
    """Write a versions.json under its ETag condition."""
    try:
        await put_object_async(store, key, content, mode=_manifest_mode(etag, force=force))
    except (PreconditionError, AlreadyExistsError) as e:
        raise PushConflictError("Remote changed during push, re-run push to try again") from e


# The documents at the catalog root that a push of any collection rewrites.
# Two writers of different collections race on them.
_ROOT_DOCUMENTS = ("catalog.json", "README.md", "AGENTS.md", "versions.json")
# The number of root catalog.json writes before the push gives up.
_ROOT_CATALOG_ATTEMPTS = 3


async def _root_etags(
    store: ObjectStore, prefix: str, keys: list[str], *, report: Report | None = None
) -> dict[str, str | None]:
    """Read the ETag of each root document in ``keys``, before the first upload."""
    root = {f"{prefix}/{name}".lstrip("/") for name in _ROOT_DOCUMENTS}
    wanted = sorted(root.intersection(keys))
    read = await asyncio.gather(
        *(_remote_etag(store, key) for key in wanted), return_exceptions=True
    )
    etags: dict[str, str | None] = {}
    for key, etag in zip(wanted, read, strict=True):
        if not isinstance(etag, BaseException):
            etags[key] = etag
        elif posixpath.basename(key) == "versions.json":
            raise etag
        else:
            _warn_once(
                f"Cannot read the ETag of {key}. The push writes it without a condition.",
                report=report,
            )
    return etags


def _merge_child_links(local: bytes, remote: bytes) -> bytes:
    """Add each child link of the remote catalog.json that the local one lacks."""
    document = json.loads(local)
    links: list[dict[str, Any]] = document.setdefault("links", [])
    hrefs = {
        posixpath.normpath(str(link.get("href"))) for link in links if link.get("rel") == "child"
    }
    added = 0
    for link in json.loads(remote).get("links", []):
        href = posixpath.normpath(str(link.get("href")))
        if link.get("rel") == "child" and href not in hrefs:
            hrefs.add(href)
            links.append(link)
            added += 1
    return json.dumps(document, indent=2).encode("utf-8") if added else local


async def _put_file(
    store: ObjectStore,
    key: str,
    content: bytes,
    root_etags: dict[str, str | None] | None = None,
    *,
    report: Report | None = None,
) -> PutResult | None:
    """Write one catalog file. A root document takes an ETag condition."""
    if root_etags is None or key not in root_etags:
        return await put_object_async(store, key, content)
    if posixpath.basename(key) != "catalog.json":
        try:
            mode = _manifest_mode(root_etags[key], force=False)
            return await put_object_async(store, key, content, mode=mode)
        except (PreconditionError, AlreadyExistsError):
            tell(
                report,
                "warn",
                f"Another writer changed {key} during the push. The remote copy stays.",
            )
            return None
    for _ in range(_ROOT_CATALOG_ATTEMPTS):
        body, etag = content, None
        try:
            remote = await obs.get_async(store, key)
            etag = remote.meta.get("e_tag")
            body = _merge_child_links(content, bytes(await remote.bytes_async()))
        except Exception as e:
            if not (isinstance(e, FileNotFoundError) or _is_not_found(e)):
                raise
        try:
            put = await put_object_async(store, key, body, mode=_manifest_mode(etag, force=False))
            return put if body is content else None
        except (PreconditionError, AlreadyExistsError):
            continue
    raise PushConflictError("Remote changed during push, re-run push to try again")


async def _upload_metadata_files_async(
    store: ObjectStore,
    catalog_root: Path,
    prefix: str,
    files: list[Path],
    *,
    written: dict[str, str | None] | None = None,
    root_etags: dict[str, str | None] | None = None,
    concurrency: int = 50,
    verbose: bool = False,
    report: Report | None = None,
) -> tuple[int, list[str]]:
    """Upload metadata files (style.json, thumbnails, etc.) to remote."""
    if not files:
        return 0, []

    written = {} if written is None else written
    errors: list[str] = []
    semaphore = asyncio.Semaphore(concurrency)

    async def upload_one(file_path: Path) -> bool:
        """Upload a single metadata file with semaphore control."""
        async with semaphore:
            try:
                rel_path = file_path.relative_to(catalog_root)
                content = file_path.read_bytes()
                key = _target_key(catalog_root, prefix, file_path)
                put = await _put_file(store, key, content, root_etags, report=report)
                if put is not None:
                    written[key] = put.get("e_tag")
                if verbose:
                    tell(report, "detail", f" Uploaded {rel_path} ({len(content)} bytes)")
                return True
            except Exception as e:
                error_msg = f"Failed to upload metadata file {file_path.name}: {e}"
                errors.append(error_msg)
                tell(report, "error", error_msg)
                return False

    # Upload all metadata files in parallel
    tasks = [upload_one(f) for f in files]
    results = await asyncio.gather(*tasks)
    uploaded_count = sum(1 for r in results if r)

    if uploaded_count > 0 and not verbose:
        tell(report, "info", f"Synced {uploaded_count} metadata file(s)")

    return uploaded_count, errors


async def _execute_push_uploads_async(  # noqa: PLR0913, PLR0914, PLR0915 - each transport option is threaded through each layer
    store: ObjectStore,
    catalog_root: Path,
    prefix: str,
    collection: str,
    local_data: dict[str, Any],
    diff: PushVersionDiff,
    etag: str | None,
    *,
    destination: str,
    concurrency: int,
    chunk_concurrency: int,
    show_progress: bool,
    force: bool,
    include_catalog: bool = True,
    remote_data: dict[str, Any] | None = None,
    adaptive: bool = True,
    reupload: frozenset[str] = frozenset(),
    verbose: bool = False,
    report: Report | None = None,
) -> PushResult:
    """Execute the upload phase of push_async."""
    # Get assets to upload (only new/changed)
    assets = _get_assets_to_upload(
        catalog_root, local_data, remote_versions_data=remote_data, reupload=reupload, report=report
    )
    # include_catalog is True for a standalone push, so the remote is a complete
    # clonable catalog. push_all_collections uploads the root files once at the end.
    try:
        stac_files = _discover_stac_files(
            catalog_root, collection, include_catalog=include_catalog, report=report
        )
    except FileNotFoundError as e:
        tell(report, "error", str(e))
        return PushResult(success=False, files_uploaded=0, versions_pushed=0, errors=[str(e)])

    # The plan names every key before the first write. Rollback deletes the keys
    # that were absent and restores the ones that existed.
    stac_paths = [*stac_files.get("items", []), *stac_files.get("collection", [])]
    stac_paths += stac_files.get("catalog", [])
    metadata_files = _discover_catalog_files(
        catalog_root, collection, include_catalog_root=include_catalog, report=report
    )
    plan_paths = [*assets, *stac_paths, *metadata_files]
    planned = [_target_key(catalog_root, prefix, p) for p in plan_paths]
    list_prefix = f"{prefix}/{collection}".lstrip("/")
    readmes = [_target_key(catalog_root, prefix, p) for p in stac_files["readmes"]]
    root_etags = await _root_etags(store, prefix, [*planned, *readmes], report=report)
    sizes = await _existing_keys(store, planned, list_prefix, concurrency, report=report)
    backups = await _back_up(store, sizes, prefix, concurrency, report=report)
    written: dict[str, str | None] = {}
    files_uploaded, metrics = 0, UploadMetrics()

    async def failed(errors: list[str], metadata_errors: list[str] | None = None) -> PushResult:
        # The rollback reports each key that it could not put back.
        # A new list keeps the caller's list unchanged.
        errors = [
            *errors,
            *await _drop_backups(store, backups, sizes, restore_over=written, report=report),
        ]
        errors += _cleanup_uploaded_assets(
            store, [k for k in written if k not in sizes], report=report
        )
        return PushResult(
            success=False,
            files_uploaded=files_uploaded,
            versions_pushed=0,
            errors=errors,
            metadata_errors=metadata_errors or [],
            metrics=metrics,
        )

    try:
        with span("push.assets", collection=collection) as stage:
            # Upload assets first (manifest-last: versions.json goes last)
            files_uploaded, upload_errors, _, metrics = await _upload_assets_async(
                store,
                catalog_root,
                prefix,
                assets,
                concurrency=concurrency,
                chunk_concurrency=chunk_concurrency,
                show_progress=show_progress,
                adaptive=adaptive,
                written=written,
                verbose=verbose,
                report=report,
            )
            stage.set(**metrics.to_attrs())

        if upload_errors:
            tell(report, "error", "Asset upload failed, aborting push")
            return await failed(upload_errors)

        # Upload STAC metadata files (async)
        with span("push.stac_files", collection=collection):
            stac_uploaded, stac_errors, _ = await _upload_stac_files_async(
                store,
                catalog_root,
                prefix,
                stac_files,
                destination=destination,
                concurrency=concurrency,
                show_progress=show_progress,
                written=written,
                root_etags=root_etags,
                verbose=verbose,
                report=report,
            )
            files_uploaded += stac_uploaded

            if stac_errors:
                tell(report, "error", "STAC metadata upload failed, aborting push")
                return await failed(stac_errors)

        # Upload metadata files (style.json, thumbnails) after STAC files and
        # before versions.json (manifest-last).
        with span("push.metadata", collection=collection):
            metadata_uploaded, metadata_errors = await _upload_metadata_files_async(
                store,
                catalog_root,
                prefix,
                metadata_files,
                written=written,
                root_etags=root_etags,
                concurrency=concurrency,
                verbose=verbose,
                report=report,
            )
            files_uploaded += metadata_uploaded

        # Metadata errors are warnings, not fatal (push continues)
        # But we surface them in PushResult so callers can act on partial sync
        all_metadata_errors: list[str] = list(metadata_errors)

        # Upload versions.json (async, manifest-last)
        with span("push.versions_json", collection=collection):
            tell(report, "info", "Uploading versions.json...")
            try:
                await _upload_versions_json_async(
                    store, prefix, collection, local_data, etag, force=force
                )
            except PushConflictError:
                # Another writer won the race. Delete no key, because that writer can own one.
                # Restore a backup only where the winning manifest does not name our bytes.
                restore = await _keys_to_restore(store, prefix, collection, local_data, written)
                await _drop_backups(store, backups, sizes, restore_over=restore, report=report)
                raise
            except Exception as e:
                tell(report, "error", f"Failed to upload versions.json: {e}")
                return await failed([f"Failed to upload versions.json: {e}"], all_metadata_errors)
    except PushConflictError:
        raise
    except BaseException:
        # An interrupt or an unexpected error stops the push before its
        # versions.json names the new bytes, so the push rolls back.
        await failed([])
        raise
    # The new versions.json is live, so a backup that stays fails the push but
    # does not roll it back. Each push uses a new backup directory.
    backup_errors = await _drop_backups(store, backups, sizes, report=report)
    repaired = _repaired_count(prefix, reupload, list(written))
    if diff.local_only:
        msg = f"Pushed {len(diff.local_only)} version(s): {diff.local_only}"
    elif repaired:
        msg = f"Repaired {repaired} asset(s) (no new versions)"
    else:
        # Force re-push with no version delta refreshes metadata only
        msg = "Refreshed metadata (no new versions)"
    tell(report, "success", _with_metrics(msg, metrics))

    # Upload READMEs last (async, parallel)
    with span("push.readmes", collection=collection):
        readme_uploaded, readme_errors = await _upload_metadata_files_async(
            store,
            catalog_root,
            prefix,
            stac_files["readmes"],
            root_etags=root_etags,
            concurrency=concurrency,
            verbose=verbose,
            report=report,
        )
        files_uploaded += readme_uploaded
    # README errors are also non-fatal metadata issues
    all_metadata_errors.extend(readme_errors)

    # Log metadata warnings (after push succeeds, so user sees them)
    if all_metadata_errors:
        tell(report, "warn", f"{len(all_metadata_errors)} metadata file(s) failed to upload:")
        for err in all_metadata_errors:
            tell(report, "warn", f" {err}")

    return PushResult(
        success=not backup_errors,
        files_uploaded=files_uploaded,
        versions_pushed=len(diff.local_only),
        conflicts=[],
        errors=backup_errors,
        metadata_errors=all_metadata_errors,
        metrics=metrics,
        landed=True,
    )


async def _sync_collection_metadata_only(
    store: ObjectStore,
    catalog_root: Path,
    prefix: str,
    collection: str,
    *,
    include_catalog_root: bool,
    concurrency: int,
    verbose: bool = False,
    report: Report | None = None,
) -> PushResult:
    """Sync a collection's non-versioned metadata when no version changed."""
    files = _discover_catalog_files(
        catalog_root, collection, include_catalog_root=include_catalog_root, report=report
    )
    if include_catalog_root:
        files += _discover_intermediate_catalog_files(catalog_root, [collection], report=report)
    keys = [_target_key(catalog_root, prefix, path) for path in files]
    metadata_uploaded, metadata_errors = await _upload_metadata_files_async(
        store,
        catalog_root,
        prefix,
        files,
        root_etags=await _root_etags(store, prefix, keys, report=report),
        concurrency=concurrency,
        verbose=verbose,
        report=report,
    )
    # Warn with a summary count, the same as the normal upload path.
    if metadata_errors:
        tell(report, "warn", f"{len(metadata_errors)} metadata file(s) failed to upload:")
        for err in metadata_errors:
            tell(report, "warn", f" {err}")
    return PushResult(
        success=True,
        files_uploaded=metadata_uploaded,
        versions_pushed=0,
        conflicts=[],
        errors=[],
        metadata_errors=list(metadata_errors),
    )


async def _repair_flagged_assets_async(  # noqa: PLR0913 - each transport option is threaded through each layer
    store: ObjectStore,
    catalog_root: Path,
    prefix: str,
    collection: str,
    local_data: dict[str, Any],
    reupload: frozenset[str],
    *,
    include_catalog_root: bool,
    concurrency: int,
    chunk_concurrency: int,
    show_progress: bool,
    adaptive: bool,
    verified_etags: dict[str, str | None],
    verbose: bool = False,
    report: Report | None = None,
) -> PushResult:
    """Repair the flagged objects when no version is new and there is no force."""
    assets = _get_reupload_assets(catalog_root, local_data, reupload, report=report)
    files_uploaded, upload_errors, uploaded_keys, metrics = await _upload_assets_async(
        store,
        catalog_root,
        prefix,
        assets,
        concurrency=concurrency,
        chunk_concurrency=chunk_concurrency,
        show_progress=show_progress,
        adaptive=adaptive,
        conditions={f"{prefix}/{href}".lstrip("/"): etag for href, etag in verified_etags.items()},
        verbose=verbose,
        report=report,
    )
    if upload_errors:
        tell(report, "error", "Asset repair failed. The remote versions.json stays as is")
        return PushResult(
            success=False,
            files_uploaded=files_uploaded,
            versions_pushed=0,
            conflicts=[],
            errors=upload_errors,
            metrics=metrics,
        )

    result = await _sync_collection_metadata_only(
        store,
        catalog_root,
        prefix,
        collection,
        include_catalog_root=include_catalog_root,
        concurrency=concurrency,
        verbose=verbose,
        report=report,
    )
    repaired = _repaired_count(prefix, reupload, uploaded_keys)
    if repaired:
        tell(
            report,
            "success",
            _with_metrics(f"Repaired {repaired} asset(s) (no new versions)", metrics),
        )
    result.files_uploaded += files_uploaded
    result.metrics = metrics
    return result


async def push_async(  # noqa: PLR0913 - each transport option is threaded through each layer
    catalog_root: Path,
    collection: str,
    destination: str,
    *,
    force: bool = False,
    dry_run: bool = False,
    profile: str | None = None,
    region: str | None = None,
    concurrency: int | None = None,
    chunk_concurrency: int | None = None,
    adaptive: bool = True,
    show_progress: bool = False,
    include_catalog: bool = True,
    verify: bool = False,
    verbose: bool = False,
    report: Report | None = None,
) -> PushResult:
    """Push local catalog changes to cloud object storage (async version)."""
    from portolan_cli.async_utils import get_default_chunk_concurrency

    concurrency = concurrency or get_default_concurrency()
    chunk_concurrency = chunk_concurrency or get_default_chunk_concurrency()

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
        return _handle_push_dry_run(
            catalog_root, collection, local_data, local_versions, verify=verify, report=report
        )

    if _run_notices.get() is None:
        _run_notices.set(set())

    # Setup store and fetch remote versions
    with span("push.fetch_remote", collection=collection):
        store, prefix = setup_store(destination, profile=profile, region=region, report=report)
        tell(report, "info", f"Checking remote state: {destination}")
        remote_data, etag = await _fetch_remote_versions_async(store, prefix, collection)

    # Extract remote versions
    if remote_data is None:
        tell(report, "info", "No remote versions.json found (first push)")
        remote_versions: list[str] = []
    else:
        remote_versions = [v.get("version") for v in remote_data.get("versions", [])]
        tell(report, "detail", f"Remote version: {remote_data.get('current_version')}")

    # Diff versions and check for conflicts
    diff = diff_version_lists(local_versions, remote_versions)
    if diff.has_conflict and not force:
        raise PushConflictError(
            f"Remote has changes not present locally: {diff.remote_only}. "
            "Pull changes first or use --force to overwrite."
        )

    # With --verify, check each skipped asset against the store.
    # Without the flag, push sends no request for a skipped asset.
    verified_etags: dict[str, str | None] = {}
    verify_report: VerifyReport | None = None
    if verify:
        verified_etags, verify_report = await _verify_skipped_assets_async(
            store, prefix, local_data, remote_data, concurrency=concurrency, report=report
        )

    reupload = frozenset(verified_etags)

    # With --force and no new version, re-upload regenerated metadata. The sha256 diff
    # still skips each asset whose bytes match the remote current snapshot.
    if not force and not diff.local_only:
        if reupload:
            # --verify found a mismatch, with no new version and no force. Upload
            # the flagged assets only, and leave the remote versions.json as is.
            result = await _repair_flagged_assets_async(
                store,
                catalog_root,
                prefix,
                collection,
                local_data,
                reupload,
                include_catalog_root=include_catalog,
                concurrency=concurrency,
                chunk_concurrency=chunk_concurrency,
                show_progress=show_progress,
                adaptive=adaptive,
                verified_etags=verified_etags,
                verbose=verbose,
                report=report,
            )
        else:
            result = await _sync_collection_metadata_only(
                store,
                catalog_root,
                prefix,
                collection,
                include_catalog_root=include_catalog,
                concurrency=concurrency,
                verbose=verbose,
                report=report,
            )
        result.verify = verify_report
        return result

    # With a new version or --force, flagged hrefs join the normal upload set.

    # Execute uploads (with remote data for sha256 diffing)
    result = await _execute_push_uploads_async(
        store,
        catalog_root,
        prefix,
        collection,
        local_data,
        diff,
        etag,
        destination=destination,
        concurrency=concurrency,
        chunk_concurrency=chunk_concurrency,
        show_progress=show_progress,
        force=force,
        include_catalog=include_catalog,
        remote_data=remote_data,
        adaptive=adaptive,
        reupload=reupload,
        verbose=verbose,
        report=report,
    )
    result.verify = verify_report
    return result


# Catalog-Wide Push


@dataclass
class PushAllResult:
    """Result of pushing all collections in a catalog."""

    success: bool
    total_collections: int
    successful_collections: int
    failed_collections: int
    total_files_uploaded: int
    total_versions_pushed: int
    dry_run: bool = False
    total_would_push_files: int = 0
    total_would_push_versions: int = 0
    collection_errors: dict[str, list[str]] = field(default_factory=dict)
    verify: VerifyReport | None = None
    collection_exceptions: dict[str, BaseException] = field(default_factory=dict)

    def to_dict(self) -> dict[str, Any]:
        """Return the JSON data of ``portolan push`` for every collection."""
        return {
            "total_collections": self.total_collections,
            "successful_collections": self.successful_collections,
            "failed_collections": self.failed_collections,
            "total_files_uploaded": self.total_files_uploaded,
            "total_versions_pushed": self.total_versions_pushed,
            "dry_run": self.dry_run,
            "total_would_push_files": self.total_would_push_files,
            "total_would_push_versions": self.total_would_push_versions,
            "collection_errors": self.collection_errors,
            **verify_data(self.verify),
        }


def discover_collections(catalog_root: Path, *, report: Report | None = None) -> list[str]:
    """Recursively discover all collections by finding directories with versions.json."""
    if not catalog_root.exists():
        raise ValueError(f"Catalog root does not exist: {catalog_root}")

    # Validate this is actually a catalog (has sentinel file)
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
        tell(report, "warn", f"Permission denied during catalog scan: {e}")
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
            tell(report, "warn", f"Cannot resolve path {collection_dir}, skipping")
            continue

        # Skip if we've already seen this resolved path (symlink cycle)
        if resolved in visited_paths:
            tell(report, "warn", f"Symlink cycle detected at {collection_dir}, skipping")
            continue

        visited_paths.add(resolved)
        collections.append(rel_path.as_posix())

    return sorted(collections)


def _push_all_process_result(
    coll: str,
    result: PushResult | None,
    err_msg: str | BaseException | None,
    current_completed: int,
    total: int,
    stats: dict[str, Any],
    *,
    report: Report | None = None,
) -> None:
    """Process result of a single collection push (helper for push_all_collections)."""
    if isinstance(err_msg, BaseException):
        stats.setdefault("exceptions", {})[coll] = err_msg
        err_msg = f"{type(err_msg).__name__}: {err_msg}"
    if err_msg:
        tell(report, "error", f"[{current_completed}/{total}] Failed {coll}: {err_msg}")
        stats["failed"] += 1
        stats["errors"][coll] = [err_msg]
    elif result and result.success:
        # A dry run reports the local upper-bound estimate as would-push stats.
        # The uploaded stats stay at 0, as in the single-collection path.
        if result.dry_run:
            v = result.would_push_versions
            f = result.would_push_files
            tell(
                report,
                "success",
                f"[{current_completed}/{total}] {coll}: "
                f"would push up to {v} version(s), {f} file(s)",
            )
            stats["total_would_push_files"] += f
            stats["total_would_push_versions"] += v
        else:
            v = result.versions_pushed
            f = result.files_uploaded
            tell(
                report,
                "success",
                f"[{current_completed}/{total}] {coll}: {v} version(s), {f} file(s)",
            )
            stats["total_files"] += f
            stats["total_versions"] += v
        stats["successful"] += 1
        if result.metrics:
            stats["metrics"].merge(result.metrics)
        if result.verify is not None and stats["verify"] is not None:
            stats["verify"].merge(result.verify)
    elif result:
        errors_list = result.errors + result.conflicts
        tell(
            report,
            "error",
            f"[{current_completed}/{total}] Failed {coll}: {', '.join(errors_list)}",
        )
        stats["failed"] += 1
        stats["landed"] += result.landed
        stats["errors"][coll] = errors_list
        # A push can fail after verify. Its counts still describe the store.
        if result.verify is not None and stats["verify"] is not None:
            stats["verify"].merge(result.verify)
    else:
        tell(report, "error", f"[{current_completed}/{total}] Failed {coll}: Unknown error")
        stats["failed"] += 1
        stats["errors"][coll] = ["Unknown error"]


def _discover_intermediate_catalog_files(
    catalog_root: Path, collections: list[str], *, report: Report | None = None
) -> list[Path]:
    """Discover the files of each intermediate catalog for nested collections."""
    seen: set[str] = set()
    for collection in collections:
        seen.update(intermediate_catalog_ids(collection))

    # A catalog level holds no items, so a file such as a/a.json is a sidecar.
    ctx = _ExcludeContext(
        exclude_patterns=_get_effective_exclude_patterns(catalog_root, None),
        versioned_asset_paths=set(),
        collection=None,
        item_level=False,
    )

    files: list[Path] = []
    for sub in sorted(seen):
        sub_dir = catalog_root / sub
        # _should_exclude_from_push skips these two names, because other phases
        # upload them at the root and collection levels. Here this phase does.
        for name in ("catalog.json", "README.md"):
            path = sub_dir / name
            if path.exists():
                files.append(path)
        if not sub_dir.is_dir() or sub_dir.is_symlink():
            continue
        listed = [
            item
            for item in sorted(sub_dir.iterdir(), key=lambda p: p.name)
            if not item.is_symlink()
            and item.is_file()
            and not _should_exclude_from_push(item, catalog_root, ctx)
        ]
        files.extend(_named_only(catalog_root, listed, [f"{sub}/catalog.json"], report=report))
    return files


def _root_upload_skip_reason(
    catalog_root: Path, catalog_json: Path, stats: dict[str, Any]
) -> str | None:
    """Return why the root-file upload must not run, or None to run it."""
    # A landed collection is live remotely, so the root must link it.
    if stats["failed"] > stats["landed"]:
        return "Skipping root file upload because some collections failed"
    if stats["successful"] + stats["landed"] == 0:
        return "Skipping root file upload because no collections were pushed"
    if not catalog_json.exists():
        # Without the root catalog.json, no child link reaches the intermediate
        # catalogs, so they are skipped too.
        return f"catalog.json not found at {catalog_root} - remote catalog may be incomplete"
    return None


def _report_root_upload_dry_run(
    catalog_root: Path,
    root_metadata: list[Path],
    intermediate_files: list[Path],
    *,
    report: Report | None = None,
) -> None:
    """Print the root files a real run would upload."""
    if (catalog_root / "README.md").exists():
        tell(report, "info", "[DRY RUN] Would upload README.md")
    if root_metadata:
        tell(report, "info", f"[DRY RUN] Would sync {len(root_metadata)} root metadata file(s)")
        for f in root_metadata:
            tell(report, "detail", f" {f.relative_to(catalog_root).as_posix()}")
    tell(report, "info", "[DRY RUN] Would upload catalog.json")
    if intermediate_files:
        tell(
            report,
            "info",
            f"[DRY RUN] Would upload {len(intermediate_files)} intermediate catalog file(s)",
        )
        for f in intermediate_files:
            tell(report, "detail", f" {f.relative_to(catalog_root).as_posix()}")
    if (catalog_root / "versions.json").exists():
        tell(report, "info", "[DRY RUN] Would upload versions.json")


async def _upload_root_files(
    store: ObjectStore,
    prefix: str,
    catalog_root: Path,
    root_metadata: list[Path],
    intermediate_files: list[Path],
    stats: dict[str, Any],
    root_etags: dict[str, str | None] | None = None,
    *,
    force: bool = False,
    report: Report | None = None,
) -> None:
    """Upload the root files in manifest-last order."""
    root_etags = root_etags or {}

    async def put(path: Path) -> None:
        # The key is the catalog-relative path, not the basename. `_assets/brand.png`
        # must land under `_assets/` for the catalog's rel="icon" href to resolve.
        key = _target_key(catalog_root, prefix, path)
        await _put_file(store, key, path.read_bytes(), root_etags, report=report)
        stats["total_files"] += 1

    root_readme = catalog_root / "README.md"
    if root_readme.exists():
        await put(root_readme)
        tell(report, "success", "Uploaded README.md")

    for meta_file in root_metadata:
        await put(meta_file)
        tell(report, "detail", f" Synced {meta_file.relative_to(catalog_root).as_posix()}")
    if root_metadata:
        tell(report, "info", f"Synced {len(root_metadata)} root metadata file(s)")

    await put(catalog_root / "catalog.json")
    tell(report, "success", "Uploaded catalog.json")

    for inter_file in intermediate_files:
        await put(inter_file)
        tell(report, "detail", f" Uploaded {inter_file.relative_to(catalog_root).as_posix()}")
    if intermediate_files:
        tell(report, "info", f"Uploaded {len(intermediate_files)} intermediate catalog file(s)")

    root_versions = catalog_root / "versions.json"
    if root_versions.exists():
        key = _target_key(catalog_root, prefix, root_versions)
        await _put_manifest(
            store, key, root_versions.read_bytes(), root_etags.get(key), force=force
        )
        tell(report, "success", "Uploaded versions.json")
        stats["total_files"] += 1


async def _key_exists(store: ObjectStore, key: str) -> bool:
    """Return True if the remote key exists."""
    try:
        await obs.head_async(store, key)
    except Exception as e:
        if isinstance(e, FileNotFoundError) or _is_not_found(e):
            return False
        raise
    return True


async def _remote_etag(store: ObjectStore, key: str) -> str | None:
    """Return the ETag of a remote key, or None if the key does not exist."""
    try:
        meta = await obs.head_async(store, key)
    except Exception as e:
        if isinstance(e, FileNotFoundError) or _is_not_found(e):
            return None
        raise
    return meta.get("e_tag")


async def _push_all_upload_root_files(
    catalog_root: Path,
    destination: str,
    profile: str | None,
    region: str | None,
    dry_run: bool,
    stats: dict[str, Any],
    collections: list[str] | None = None,
    root_etags: dict[str, str | None] | None = None,
    *,
    force: bool = False,
    report: Report | None = None,
) -> bool:
    """Upload root-level files after all collections."""
    catalog_json = catalog_root / "catalog.json"

    skip_reason = _root_upload_skip_reason(catalog_root, catalog_json, stats)
    if skip_reason is not None:
        tell(report, "warn", skip_reason)
        return True

    # Discover root metadata files
    root_metadata = _discover_catalog_files(catalog_root, include_catalog_root=True, report=report)

    # Discover intermediate catalog files for nested collections. They upload
    # after the root catalog.json, before versions.json.
    intermediate_files = _discover_intermediate_catalog_files(
        catalog_root, collections or [], report=report
    )

    if dry_run:
        _report_root_upload_dry_run(catalog_root, root_metadata, intermediate_files, report=report)
        return True

    try:
        store, prefix = setup_store(destination, profile=profile, region=region, report=report)
        await _upload_root_files(
            store,
            prefix,
            catalog_root,
            root_metadata,
            intermediate_files,
            stats,
            root_etags,
            force=force,
            report=report,
        )
        return True
    except Exception as e:
        tell(report, "error", f"Failed to upload root files: {e}")
        stats["errors"]["root_files"] = [str(e)]
        stats.setdefault("exceptions", {})["root_files"] = e
        return False


async def push_all_collections_async(  # noqa: PLR0913, PLR0914, PLR0915 - each transport option is threaded through each layer
    catalog_root: Path,
    destination: str,
    *,
    force: bool = False,
    dry_run: bool = False,
    profile: str | None = None,
    region: str | None = None,
    concurrency: int | None = None,
    file_concurrency: int | None = None,
    chunk_concurrency: int | None = None,
    adaptive: bool = True,
    verify: bool = False,
    verbose: bool = False,
    report: Report | None = None,
) -> PushAllResult:
    """Push all collections in a catalog to cloud storage asynchronously."""
    from portolan_cli.version_ops import check_backend_supports_push

    blocked = check_backend_supports_push(catalog_root)
    if blocked is not None:
        raise NotImplementedError(blocked.message)
    collections = discover_collections(catalog_root, report=report)
    total = len(collections)

    if total == 0:
        tell(report, "warn", "No initialized collections found in catalog")
        tell(report, "warn", "Collections need a versions.json file to be pushable")
        return PushAllResult(
            success=True,
            total_collections=0,
            successful_collections=0,
            failed_collections=0,
            total_files_uploaded=0,
            total_versions_pushed=0,
            dry_run=dry_run,
        )

    tell(report, "info", f"Found {total} collection(s) to push")
    _run_notices.set(set())

    # The root document writes are conditional on the ETags read here, before
    # any collection writes.
    root_etags = None
    if not dry_run and (catalog_root / "catalog.json").exists():
        try:
            store, prefix = setup_store(destination, profile=profile, region=region, report=report)
        except Exception:
            # The root upload sets up the store again and reports the error.
            root_etags = None
        else:
            local = [catalog_root / name for name in _ROOT_DOCUMENTS]
            keys = [_target_key(catalog_root, prefix, p) for p in local if p.exists()]
            root_etags = await _root_etags(store, prefix, keys, report=report)

    # Track aggregate stats in a dict for helper function access
    stats: dict[str, Any] = {
        "successful": 0,
        "failed": 0,
        "landed": 0,
        "total_files": 0,
        "total_versions": 0,
        "total_would_push_files": 0,
        "total_would_push_versions": 0,
        "metrics": UploadMetrics(),
        "errors": {},
        "exceptions": {},
        # A dry run sends no HEAD, so it has no verify counts.
        "verify": VerifyReport() if verify and not dry_run else None,
    }

    max_concurrent = (
        concurrency if concurrency is not None else min(total, get_default_concurrency())
    )
    max_concurrent = min(max_concurrent, total)

    tell(report, "info", f"Using concurrency: {max_concurrent}")

    # Semaphore for collection-level concurrency control
    semaphore = asyncio.Semaphore(max_concurrent)

    async def push_one(collection: str) -> tuple[str, PushResult | None, Exception | None]:
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
                    chunk_concurrency=chunk_concurrency,  # Pass chunk-level concurrency
                    adaptive=adaptive,  # Pass adaptive slow-start flag
                    include_catalog=False,  # Uploaded once at end by _push_all_upload_root_files
                    verify=verify,
                    verbose=verbose,
                    report=report,
                )
                return (collection, result, None)
            except Exception as e:
                return (collection, None, e)

    # Run all collection pushes concurrently
    tasks = [push_one(coll) for coll in collections]
    results = await asyncio.gather(*tasks, return_exceptions=True)

    # Process results
    for i, result in enumerate(results):
        if isinstance(result, BaseException):
            coll = collections[i]
            tell(report, "error", f"[{i + 1}/{total}] Failed {coll}: {result}")
            stats["failed"] += 1
            stats["errors"][coll] = [str(result)]
            stats["exceptions"][coll] = result
        elif isinstance(result, tuple):
            coll, push_result, err_msg = result
            _push_all_process_result(coll, push_result, err_msg, i + 1, total, stats, report=report)

    catalog_ok = await _push_all_upload_root_files(
        catalog_root,
        destination,
        profile,
        region,
        dry_run,
        stats,
        collections,
        root_etags,
        force=force,
        report=report,
    )

    overall_success = stats["failed"] == 0 and catalog_ok
    tell(report, "info", f"\n{'=' * 60}")
    if overall_success:
        if dry_run:
            wv = stats["total_would_push_versions"]
            wf = stats["total_would_push_files"]
            msg = f"[DRY RUN] Would push {stats['successful']} collection(s), "
            msg += f"{wv} version(s), {wf} file(s)"
        else:
            msg = f"Pushed {stats['successful']} collection(s), "
            msg += f"{stats['total_versions']} version(s), {stats['total_files']} file(s)"
            if stats["metrics"].total_bytes > 0:
                size = format_file_size(stats["metrics"].total_bytes)
                speed = format_speed(stats["metrics"].average_speed)
                msg += f" ({size}, avg {speed})"
        tell(report, "success", msg)
    else:
        tell(
            report,
            "warn",
            f"Completed with errors: {stats['successful']} succeeded, {stats['failed']} failed",
        )
        for coll_name, errs in stats["errors"].items():
            tell(report, "warn", f" {coll_name}: {', '.join(errs)}")

    return PushAllResult(
        success=overall_success,
        total_collections=total,
        successful_collections=stats["successful"],
        failed_collections=stats["failed"],
        total_files_uploaded=stats["total_files"],
        total_versions_pushed=stats["total_versions"],
        dry_run=dry_run,
        total_would_push_files=stats["total_would_push_files"],
        total_would_push_versions=stats["total_would_push_versions"],
        collection_errors=stats["errors"],
        verify=stats["verify"],
        collection_exceptions=stats["exceptions"],
    )


def push_all_collections(  # noqa: PLR0913 - each transport option is threaded through each layer
    catalog_root: Path,
    destination: str,
    *,
    force: bool = False,
    dry_run: bool = False,
    profile: str | None = None,
    region: str | None = None,
    workers: int | None = None,
    file_concurrency: int | None = None,
    chunk_concurrency: int | None = None,
    adaptive: bool = True,
    max_connections: int | None = None,
    verify: bool = False,
    verbose: bool = False,
    report: Report | None = None,
) -> PushAllResult:
    """Push all collections in a catalog to cloud storage (sync wrapper)."""
    from portolan_cli.async_utils import (
        adjust_concurrency_for_max_connections,
        get_default_chunk_concurrency,
        get_default_concurrency,
    )

    # Divide max_connections by the worker count, or 4 workers with 32 each open 128.
    effective_file_conc = file_concurrency or get_default_concurrency()
    effective_chunk_conc = chunk_concurrency or get_default_chunk_concurrency()

    if max_connections is not None:
        # workers=None auto-detects, so estimate with the default concurrency.
        effective_workers = workers if workers is not None else get_default_concurrency()
        effective_workers = max(1, effective_workers)  # Ensure at least 1

        # Divide max_connections by workers to get per-collection budget
        per_collection_budget = max(1, max_connections // effective_workers)

        effective_file_conc, effective_chunk_conc = adjust_concurrency_for_max_connections(
            effective_file_conc, effective_chunk_conc, per_collection_budget
        )

    return asyncio.run(
        push_all_collections_async(
            catalog_root=catalog_root,
            destination=destination,
            force=force,
            dry_run=dry_run,
            profile=profile,
            region=region,
            concurrency=workers,
            file_concurrency=effective_file_conc,
            chunk_concurrency=effective_chunk_conc,
            adaptive=adaptive,
            verbose=verbose,
            report=report,
            verify=verify,
        )
    )


def effective_push_concurrency(
    concurrency: int, chunk_concurrency: int, max_connections: int | None
) -> tuple[int, int]:
    """Apply the ``max_connections`` cap to the push concurrency."""
    from portolan_cli.async_utils import adjust_concurrency_for_max_connections

    if max_connections is None:
        return concurrency, chunk_concurrency
    return adjust_concurrency_for_max_connections(concurrency, chunk_concurrency, max_connections)


def run_push(  # noqa: PLR0913 - each transport option is threaded through each layer
    catalog_root: Path,
    destination: str,
    *,
    collection: str | None = None,
    force: bool = False,
    dry_run: bool = False,
    profile: str | None = None,
    region: str | None = None,
    workers: int | None = None,
    concurrency: int = 8,
    chunk_concurrency: int = 4,
    max_connections: int | None = None,
    adaptive: bool = True,
    verify: bool = False,
    verbose: bool = False,
    report: Report | None = None,
    # The library draws the bar only when the caller asks. Moving the
    # renderer to the CLI is a follow-up.
    show_progress: bool = False,
) -> PushResult | PushAllResult:
    """Run ``portolan push``: push one collection, or every collection."""
    from portolan_cli.version_ops import check_backend_supports_push

    # Another backend has no file manifest to publish, so refuse before any write.
    # A dry run writes nothing, so it skips the check.
    if not dry_run:
        blocked = check_backend_supports_push(catalog_root, collection)
        if blocked is not None:
            raise NotImplementedError(blocked.message)
    file_conc, chunk_conc = effective_push_concurrency(
        concurrency, chunk_concurrency, max_connections
    )
    if collection is None:
        return push_all_collections(
            catalog_root=catalog_root,
            destination=destination,
            force=force,
            dry_run=dry_run,
            profile=profile,
            region=region,
            workers=workers,
            file_concurrency=file_conc,
            chunk_concurrency=chunk_conc,
            max_connections=max_connections,
            adaptive=adaptive,
            verify=verify,
            verbose=verbose,
            report=report,
        )
    # max_connections already applies through file_conc and chunk_conc.
    return asyncio.run(
        push_async(
            catalog_root=catalog_root,
            collection=collection,
            destination=destination,
            force=force,
            dry_run=dry_run,
            profile=profile,
            region=region,
            concurrency=file_conc,
            chunk_concurrency=chunk_conc,
            adaptive=adaptive,
            verify=verify,
            show_progress=show_progress,
            verbose=verbose,
            report=report,
        )
    )
