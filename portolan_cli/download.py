"""Download files from cloud object storage (S3, GCS, Azure).

This module provides functionality to download files and directories from cloud
object storage using the obstore library. It supports:

- S3 (including S3-compatible endpoints like MinIO)
- Google Cloud Storage (GCS)
- Azure Blob Storage

Credential discovery follows the obstore/cloud provider conventions:
- S3: ~/.aws/credentials, environment variables, or explicit profile
- GCS: GOOGLE_APPLICATION_CREDENTIALS or gcloud auth
- Azure: AZURE_STORAGE_ACCOUNT_KEY, SAS token, or Azure CLI

Basic Usage:
    from portolan_cli.download import download_file, download_directory
    from portolan_cli.upload import check_credentials

    # Check credentials before download
    valid, hint = check_credentials("s3://mybucket/path")
    if not valid:
        print(hint)
        return

    # Download a single file
    result = download_file(
        source="s3://mybucket/data.parquet",
        destination=Path("data.parquet"),
    )

    # Download a directory
    result = download_directory(
        source="s3://mybucket/dataset/",
        destination=Path("output/"),
        pattern="*.parquet",  # Optional filter
    )

Custom S3 Endpoints (MinIO, source.coop):
    result = download_file(
        source="s3://mybucket/data.parquet",
        destination=Path("data.parquet"),
        s3_endpoint="minio.example.com:9000",
        s3_region="us-east-1",
    )

Note:
    This module provides internal library functionality only. It does not
    include a CLI command - that will be added as part of the sync command
    (issue #16).

    Credential checking and store setup are reused from upload.py to maintain
    consistency and avoid duplication.
"""

from __future__ import annotations

import fnmatch
import time
from concurrent.futures import Future, ThreadPoolExecutor, as_completed
from dataclasses import dataclass, field
from pathlib import Path

import obstore as obs
from obstore.store import (
    AzureStore,
    GCSStore,
    HTTPStore,
    LocalStore,
    MemoryStore,
    S3Store,
)

from portolan_cli.output import detail, error, info, success, warn
from portolan_cli.upload import (
    _setup_store_and_kwargs,
    parse_object_store_url,
)

# Type alias for all supported object stores (same as upload.py)
ObjectStore = S3Store | GCSStore | AzureStore | HTTPStore | LocalStore | MemoryStore


# =============================================================================
# Exceptions
# =============================================================================


class DownloadIntegrityError(Exception):
    """Raised when downloaded file fails integrity verification."""

    pass


class PathTraversalError(ValueError):
    """Raised when a remote key contains path traversal attempt."""

    pass


# =============================================================================
# Data Classes
# =============================================================================


@dataclass
class DownloadResult:
    """Result of a download operation.

    Attributes:
        success: True if all files were downloaded successfully.
        files_downloaded: Number of files successfully downloaded.
        files_failed: Number of files that failed to download.
        total_bytes: Total bytes downloaded (only successful files).
        errors: List of (local_path, exception) tuples for failed downloads.
                Uses Path type for consistency with UploadResult.
    """

    success: bool
    files_downloaded: int
    files_failed: int
    total_bytes: int
    errors: list[tuple[Path, Exception]] = field(default_factory=list)


# =============================================================================
# Security Validation
# =============================================================================


def _validate_local_path(local_path: Path, destination: Path) -> None:
    """Validate that local path is within destination directory.

    Protects against path traversal attacks where remote keys contain '..'
    sequences that could escape the destination directory.

    Args:
        local_path: The computed local file path
        destination: The intended destination directory

    Raises:
        PathTraversalError: If local_path escapes destination directory
    """
    # Resolve both paths to absolute paths (resolves symlinks and ..)
    resolved_local = local_path.resolve()
    resolved_dest = destination.resolve()

    # Check if local path is within destination
    try:
        resolved_local.relative_to(resolved_dest)
    except ValueError as err:
        raise PathTraversalError(
            f"Path traversal detected: {local_path} escapes destination {destination}"
        ) from err


# =============================================================================
# Target Path Building
# =============================================================================


def _build_local_path(remote_key: str, prefix: str, destination: Path) -> Path:
    """Build local file path preserving directory structure.

    Args:
        remote_key: Full remote object key (e.g., "data/subdir/file.parquet")
        prefix: Prefix from source URL (e.g., "data/")
        destination: Local destination directory

    Returns:
        Local file path preserving relative structure
    """
    # Remove prefix from key to get relative path
    if prefix and remote_key.startswith(prefix):
        relative_path = remote_key[len(prefix) :].lstrip("/")
    else:
        relative_path = remote_key.rsplit("/", 1)[-1]  # Just filename

    return destination / relative_path


def _get_local_path_for_file(
    source_prefix: str, destination: Path, is_dir_destination: bool
) -> Path:
    """Determine the local path for a single file download.

    Args:
        source_prefix: Prefix extracted from source URL
        destination: Local destination path
        is_dir_destination: True if destination is/should be a directory

    Returns:
        Local file path for the download
    """
    if is_dir_destination or destination.is_dir():
        # Destination is a directory, use filename from source
        filename = source_prefix.rsplit("/", 1)[-1] if "/" in source_prefix else source_prefix
        return destination / filename
    else:
        # Destination is the exact file path
        return destination


# =============================================================================
# Download Functions
# =============================================================================


def _download_one_file(
    store: ObjectStore,
    remote_key: str,
    local_path: Path,
    file_size: int,
) -> tuple[Path, Exception | None, int]:
    """Download a single file and return result tuple for parallel processing.

    Args:
        store: Object store instance
        remote_key: Remote object key to download
        local_path: Local path to save the file
        file_size: Expected file size in bytes

    Returns:
        Tuple of (local_path, error_or_none, bytes_downloaded)
        Uses Path for consistency with UploadResult.
    """
    try:
        size_mb = file_size / (1024 * 1024)
        start_time = time.time()

        filename = local_path.name
        info(f"Downloading {filename} ({size_mb:.2f} MB) <- {remote_key}")

        # Ensure parent directory exists
        local_path.parent.mkdir(parents=True, exist_ok=True)

        # Stream download to file with cleanup on failure
        try:
            response = obs.get(store, remote_key)
            with open(local_path, "wb") as f:
                for chunk in response:
                    f.write(chunk)
        except Exception:
            # Clean up partial file on any download failure
            if local_path.exists():
                local_path.unlink()
            raise

        # Verify file integrity - actual size must match expected
        actual_size = local_path.stat().st_size
        if file_size > 0 and actual_size != file_size:
            # Clean up corrupted file
            local_path.unlink()
            raise DownloadIntegrityError(
                f"Size mismatch: expected {file_size} bytes, got {actual_size} bytes"
            )

        elapsed = time.time() - start_time
        speed_mbps = size_mb / elapsed if elapsed > 0 else 0

        success(f"{filename} ({speed_mbps:.2f} MB/s)")
        return local_path, None, actual_size
    except Exception as e:
        # Clean up partial file if it exists (for any uncaught exceptions)
        if local_path.exists():
            local_path.unlink()
        error(f"{local_path.name}: {e}")
        return local_path, e, 0


def download_file(
    source: str,
    destination: Path,
    *,
    profile: str | None = None,
    dry_run: bool = False,
    overwrite: bool = True,
    s3_endpoint: str | None = None,
    s3_region: str | None = None,
    s3_use_ssl: bool = True,
    chunk_concurrency: int = 12,
) -> DownloadResult:
    """Download a single file from S3/GCS/Azure.

    Args:
        source: Object store URL (e.g., s3://bucket/key)
        destination: Local file path or directory
        profile: AWS profile name (for S3 only)
        dry_run: If True, show what would be downloaded without actually downloading
        overwrite: If False, skip existing files (default: True)
        s3_endpoint: Custom S3-compatible endpoint (e.g., "minio.example.com:9000")
        s3_region: S3 region (default: auto-detected)
        s3_use_ssl: Whether to use HTTPS for S3 endpoint (default: True)
        chunk_concurrency: Max concurrent chunks per file (default: 12)

    Returns:
        DownloadResult with download statistics

    Raises:
        ValueError: If source or destination is empty/invalid

    Example:
        >>> result = download_file("s3://bucket/data.parquet", Path("data.parquet"))
        >>> if result.success:
        ...     print(f"Downloaded {result.files_downloaded} file(s)")
    """
    # Input validation
    if not source or not source.strip():
        raise ValueError("source URL cannot be empty")
    if not str(destination):
        raise ValueError("destination path cannot be empty")

    bucket_url, prefix = parse_object_store_url(source)
    local_path = _get_local_path_for_file(
        prefix, destination, destination.is_dir() if destination.exists() else False
    )

    # Overwrite protection
    if not overwrite and local_path.exists():
        detail(f"Skipping existing file: {local_path}")
        return DownloadResult(
            success=True,
            files_downloaded=0,
            files_failed=0,
            total_bytes=0,
            errors=[],
        )

    if dry_run:
        info(f"[DRY RUN] Would download: {prefix} -> {local_path}")
        return DownloadResult(
            success=True,
            files_downloaded=0,
            files_failed=0,
            total_bytes=0,
            errors=[],
        )

    store, kwargs = _setup_store_and_kwargs(
        bucket_url, profile, chunk_concurrency, s3_endpoint, s3_region, s3_use_ssl
    )

    try:
        start_time = time.time()

        # Ensure parent directory exists
        local_path.parent.mkdir(parents=True, exist_ok=True)

        # Get the file with streaming
        response = obs.get(store, prefix)
        file_size = response.meta.get("size", 0)
        size_mb = file_size / (1024 * 1024) if file_size else 0

        info(f"Downloading {local_path.name} ({size_mb:.2f} MB) <- {prefix}")

        # Stream download to file with cleanup on failure
        try:
            with open(local_path, "wb") as f:
                for chunk in response:
                    f.write(chunk)
        except Exception:
            # Clean up partial file on any download failure
            if local_path.exists():
                local_path.unlink()
            raise

        # Verify file integrity - actual size must match expected
        actual_size = local_path.stat().st_size
        if file_size > 0 and actual_size != file_size:
            # Clean up corrupted file
            local_path.unlink()
            raise DownloadIntegrityError(
                f"Size mismatch: expected {file_size} bytes, got {actual_size} bytes"
            )

        elapsed = time.time() - start_time
        speed_mbps = size_mb / elapsed if elapsed > 0 else 0

        success(f"Download complete ({speed_mbps:.2f} MB/s)")

        return DownloadResult(
            success=True,
            files_downloaded=1,
            files_failed=0,
            total_bytes=actual_size,
            errors=[],
        )
    except Exception as e:
        # Clean up partial file if it exists (may have already been deleted)
        try:
            if local_path.exists():
                local_path.unlink()
        except OSError:
            pass  # Already deleted or permission issue
        error(f"Download failed: {e}")
        return DownloadResult(
            success=False,
            files_downloaded=0,
            files_failed=1,
            total_bytes=0,
            errors=[(local_path, e)],
        )


def _list_remote_files(
    store: ObjectStore, prefix: str, pattern: str | None = None
) -> list[dict[str, int | str]]:
    """List files from remote prefix, optionally filtering by pattern.

    Args:
        store: Object store instance
        prefix: Remote prefix to list
        pattern: Optional glob pattern for filtering files (e.g., "*.parquet")

    Returns:
        List of file metadata dicts with 'path' and 'size' keys
    """
    files: list[dict[str, int | str]] = []

    for batch in obs.list(store, prefix=prefix):
        for meta in batch:
            remote_path = str(meta["path"])
            size = int(meta["size"])

            # Apply pattern filter if specified
            if pattern:
                filename = remote_path.rsplit("/", 1)[-1]
                if not fnmatch.fnmatch(filename, pattern):
                    continue

            files.append({"path": remote_path, "size": size})

    return files


def _print_dry_run_directory(
    files: list[dict[str, int | str]], prefix: str, destination: Path, total_size_mb: float
) -> None:
    """Print dry-run information for directory download."""
    info(f"[DRY RUN] Would download {len(files)} file(s) ({total_size_mb:.2f} MB total)")
    for f in files[:10]:
        remote_path = str(f["path"])
        local_path = _build_local_path(remote_path, prefix, destination)
        detail(f"  {remote_path} -> {local_path}")
    if len(files) > 10:
        detail(f"  ... and {len(files) - 10} more file(s)")


def _execute_parallel_downloads(
    store: ObjectStore,
    files: list[dict[str, int | str]],
    prefix: str,
    destination: Path,
    max_files: int,
    fail_fast: bool,
    overwrite: bool = True,
) -> list[tuple[Path, Exception | None, int]]:
    """Execute parallel downloads using ThreadPoolExecutor.

    Args:
        store: Object store instance
        files: List of file metadata to download
        prefix: Source prefix for relative path calculation
        destination: Local destination directory
        max_files: Max concurrent file downloads
        fail_fast: Stop on first error if True
        overwrite: If False, skip existing files

    Returns:
        List of (local_path, error_or_none, bytes_downloaded) tuples
    """
    results: list[tuple[Path, Exception | None, int]] = []

    # Pre-filter files: validate paths and check for existing files
    files_to_download: list[tuple[str, int, Path]] = []
    for file_meta in files:
        remote_key = str(file_meta["path"])
        file_size = int(file_meta["size"])
        local_path = _build_local_path(remote_key, prefix, destination)

        # Path traversal protection
        try:
            _validate_local_path(local_path, destination)
        except PathTraversalError as e:
            warn(f"Skipping {remote_key}: {e}")
            results.append((local_path, e, 0))
            continue

        # Overwrite protection
        if not overwrite and local_path.exists():
            detail(f"Skipping existing: {local_path.name}")
            continue

        files_to_download.append((remote_key, file_size, local_path))

    if not files_to_download:
        return results

    # Type alias for download result future
    DownloadResultFuture = Future[tuple[Path, Exception | None, int]]

    if fail_fast:
        # For fail_fast mode, submit futures incrementally to ensure we can stop
        # after the first error without starting additional tasks.
        with ThreadPoolExecutor(max_workers=max_files) as executor:
            pending: dict[DownloadResultFuture, str] = {}
            files_iter = iter(files_to_download)

            # Submit initial batch up to max_files
            for remote_key, file_size, local_path in files_iter:
                future: DownloadResultFuture = executor.submit(
                    _download_one_file, store, remote_key, local_path, file_size
                )
                pending[future] = remote_key
                if len(pending) >= max_files:
                    break

            while pending:
                # Wait for next completed future
                done: DownloadResultFuture = next(iter(as_completed(pending)))
                result = done.result()
                results.append(result)
                del pending[done]

                # Stop on first error
                if result[1] is not None:
                    for pending_future in pending:
                        pending_future.cancel()
                    break

                # Submit next file if available
                try:
                    remote_key, file_size, local_path = next(files_iter)
                    new_future: DownloadResultFuture = executor.submit(
                        _download_one_file, store, remote_key, local_path, file_size
                    )
                    pending[new_future] = remote_key
                except StopIteration:
                    pass
    else:
        # For non-fail_fast mode, submit all futures upfront for maximum parallelism
        with ThreadPoolExecutor(max_workers=max_files) as executor:
            future_to_file = {
                executor.submit(
                    _download_one_file, store, remote_key, local_path, file_size
                ): remote_key
                for remote_key, file_size, local_path in files_to_download
            }

            for future in as_completed(future_to_file):
                result = future.result()
                results.append(result)

    return results


def download_directory(
    source: str,
    destination: Path,
    *,
    pattern: str | None = None,
    profile: str | None = None,
    max_files: int = 4,
    chunk_concurrency: int = 12,
    fail_fast: bool = False,
    dry_run: bool = False,
    overwrite: bool = True,
    s3_endpoint: str | None = None,
    s3_region: str | None = None,
    s3_use_ssl: bool = True,
) -> DownloadResult:
    """Download a directory from S3/GCS/Azure with parallel downloads.

    Args:
        source: Object store URL (e.g., s3://bucket/prefix/)
        destination: Local directory path
        pattern: Optional glob pattern for filtering files (e.g., "*.parquet")
        profile: AWS profile name (for S3 only)
        max_files: Max number of files to download in parallel (default: 4)
        chunk_concurrency: Max concurrent chunks per file (default: 12)
        fail_fast: If True, stop on first error; otherwise continue and report at end
        dry_run: If True, show what would be downloaded without actually downloading
        overwrite: If False, skip existing files (default: True)
        s3_endpoint: Custom S3-compatible endpoint (e.g., "minio.example.com:9000")
        s3_region: S3 region (default: auto-detected)
        s3_use_ssl: Whether to use HTTPS for S3 endpoint (default: True)

    Returns:
        DownloadResult with download statistics

    Raises:
        ValueError: If destination is an existing file (not a directory)

    Example:
        >>> result = download_directory(
        ...     "s3://bucket/dataset/",
        ...     Path("output/"),
        ...     pattern="*.parquet",
        ... )
        >>> print(f"Downloaded {result.files_downloaded} files")
    """
    # Input validation - destination must be a directory
    if destination.exists() and destination.is_file():
        raise ValueError(f"Destination must be a directory, not a file: {destination}")

    bucket_url, prefix = parse_object_store_url(source)

    store, kwargs = _setup_store_and_kwargs(
        bucket_url, profile, chunk_concurrency, s3_endpoint, s3_region, s3_use_ssl
    )

    # Note: chunk_concurrency is stored in kwargs['max_concurrency'] but obstore.get
    # doesn't support max_concurrency parameter. The kwargs are kept for potential
    # future use with obstore enhancements.
    _ = kwargs  # Mark as intentionally unused for now

    # List remote files
    files = _list_remote_files(store, prefix, pattern)

    # Early return for empty remote directory
    if not files:
        detail(f"No files found at {source}")
        return DownloadResult(
            success=True, files_downloaded=0, files_failed=0, total_bytes=0, errors=[]
        )

    total_size = sum(int(f["size"]) for f in files)
    total_size_mb = total_size / (1024 * 1024)

    if dry_run:
        _print_dry_run_directory(files, prefix, destination, total_size_mb)
        return DownloadResult(
            success=True, files_downloaded=0, files_failed=0, total_bytes=0, errors=[]
        )

    # Ensure destination directory exists
    destination.mkdir(parents=True, exist_ok=True)

    info(f"Found {len(files)} file(s) to download ({total_size_mb:.2f} MB total)")

    # Ensure max_files is at least 1 to avoid ThreadPoolExecutor ValueError
    max_files = max(1, max_files)

    results = _execute_parallel_downloads(
        store, files, prefix, destination, max_files, fail_fast, overwrite
    )

    # Calculate results
    errors = [(path, err) for path, err, _ in results if err is not None]
    successful_bytes = sum(bytes_downloaded for _, err, bytes_downloaded in results if err is None)
    files_downloaded = len(results) - len(errors)

    return DownloadResult(
        success=len(errors) == 0,
        files_downloaded=files_downloaded,
        files_failed=len(errors),
        total_bytes=successful_bytes,
        errors=errors,
    )
