"""Upload files to cloud object storage (S3, GCS, Azure).

This module provides functionality to upload files and directories to cloud
object storage using the obstore library. It supports:

- S3 (including S3-compatible endpoints like MinIO)
- Google Cloud Storage (GCS)
- Azure Blob Storage

Credential discovery follows the obstore/cloud provider conventions:
- S3: ~/.aws/credentials, environment variables, or explicit profile
- GCS: GOOGLE_APPLICATION_CREDENTIALS or gcloud auth
- Azure: AZURE_STORAGE_ACCOUNT_KEY, SAS token, or Azure CLI

Basic Usage:
    from portolan_cli.upload import upload_file, upload_directory, check_credentials

    # Check credentials before upload
    valid, hint = check_credentials("s3://mybucket/path")
    if not valid:
        print(hint)
        return

    # Upload a single file
    result = upload_file(
        source=Path("data.parquet"),
        destination="s3://mybucket/data.parquet",
    )

    # Upload a directory
    result = upload_directory(
        source=Path("output/"),
        destination="s3://mybucket/dataset/",
        pattern="*.parquet",  # Optional filter
    )

Custom S3 Endpoints (MinIO, source.coop):
    result = upload_file(
        source=Path("data.parquet"),
        destination="s3://mybucket/data.parquet",
        s3_endpoint="minio.example.com:9000",
        s3_region="us-east-1",
    )

Note:
    This module provides internal library functionality only. It does not
    include a CLI command - that will be added as part of the sync command
    (issue #16).
"""

from __future__ import annotations

import configparser
import os
import re
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
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

from portolan_cli.output import detail, error, info, success

# Type alias for all supported object stores
ObjectStore = S3Store | GCSStore | AzureStore | HTTPStore | LocalStore | MemoryStore

# =============================================================================
# Data Classes
# =============================================================================


@dataclass
class UploadResult:
    """Result of an upload operation.

    Attributes:
        success: True if all files were uploaded successfully.
        files_uploaded: Number of files successfully uploaded.
        files_failed: Number of files that failed to upload.
        total_bytes: Total bytes uploaded (only successful files).
        errors: List of (file_path, exception) tuples for failed uploads.
    """

    success: bool
    files_uploaded: int
    files_failed: int
    total_bytes: int
    errors: list[tuple[Path, Exception]] = field(default_factory=list)


# =============================================================================
# URL Parsing
# =============================================================================


def parse_object_store_url(url: str) -> tuple[str, str]:
    """Parse object store URL into (bucket_url, prefix).

    The bucket_url is what obstore needs to create a store.
    The prefix is the path within that bucket.

    Examples:
        s3://bucket/prefix/path -> (s3://bucket, prefix/path)
        gs://bucket/path -> (gs://bucket, path)
        az://account/container/path -> (az://account/container, path)

    Args:
        url: Full object store URL

    Returns:
        Tuple of (bucket_url, prefix)

    Raises:
        ValueError: If URL scheme is not supported
    """
    if url.startswith("s3://"):
        parts = url[5:].split("/", 1)
        bucket = parts[0]
        prefix = parts[1] if len(parts) > 1 else ""
        return f"s3://{bucket}", prefix

    elif url.startswith("gs://"):
        parts = url[5:].split("/", 1)
        bucket = parts[0]
        prefix = parts[1] if len(parts) > 1 else ""
        return f"gs://{bucket}", prefix

    elif url.startswith("az://"):
        # Azure: az://account/container/path
        parts = url[5:].split("/", 2)
        if len(parts) < 2:
            raise ValueError(f"Invalid Azure URL: {url}. Expected az://account/container/path")
        account, container = parts[0], parts[1]
        prefix = parts[2] if len(parts) > 2 else ""
        return f"az://{account}/{container}", prefix

    elif url.startswith(("https://", "http://")):
        # HTTP stores - return as-is
        return url, ""

    else:
        raise ValueError(f"Unsupported URL scheme: {url}")


# =============================================================================
# Credential Loading
# =============================================================================


def _load_aws_credentials_from_profile(
    profile: str = "default",
) -> tuple[str | None, str | None, str | None]:
    """Load AWS credentials from ~/.aws/credentials file.

    Uses Python's built-in configparser to read credentials without requiring boto3.

    Args:
        profile: AWS profile name (default: "default")

    Returns:
        Tuple of (access_key_id, secret_access_key, region)
        Any value may be None if not found.
    """
    creds_file = Path.home() / ".aws" / "credentials"
    config_file = Path.home() / ".aws" / "config"

    access_key: str | None = None
    secret_key: str | None = None
    region: str | None = None

    # Read credentials
    if creds_file.exists():
        parser = configparser.ConfigParser()
        parser.read(creds_file)

        if profile in parser.sections():
            section = parser[profile]
            access_key = section.get("aws_access_key_id")
            secret_key = section.get("aws_secret_access_key")
        elif profile == "default" and "DEFAULT" in parser:
            access_key = parser["DEFAULT"].get("aws_access_key_id")
            secret_key = parser["DEFAULT"].get("aws_secret_access_key")

    # Read region from config
    if config_file.exists():
        config = configparser.ConfigParser()
        config.read(config_file)

        # Profile sections in config are named "profile <name>" except for default
        profile_section = profile if profile == "default" else f"profile {profile}"
        if profile_section in config.sections():
            region = config[profile_section].get("region")
        elif profile == "default" and "DEFAULT" in config:
            region = config["DEFAULT"].get("region")

    return access_key, secret_key, region


def _try_infer_region_from_bucket(bucket: str) -> str | None:
    """Try to infer AWS region from bucket name.

    Some S3-compatible services include region in bucket name, e.g.:
    - us-west-2.opendata.source.coop -> us-west-2
    - eu-central-1.example.com -> eu-central-1

    This is a best-effort heuristic and should not be relied upon.

    Args:
        bucket: S3 bucket name

    Returns:
        Region string if detected, None otherwise
    """
    # Pattern matches AWS region format at start of bucket name
    region_pattern = (
        r"^(us|eu|ap|sa|ca|me|af)-"
        r"(north|south|east|west|central|northeast|southeast|northwest|southwest)-\d"
    )
    match = re.match(region_pattern, bucket)
    if match:
        # Extract full region (e.g., "us-west-2" from "us-west-2.opendata.source.coop")
        region_end = bucket.find(".")
        if region_end > 0:
            return bucket[:region_end]
    return None


# =============================================================================
# Credential Checking
# =============================================================================


def _check_s3_credentials(profile: str | None = None) -> tuple[bool, str]:
    """Check if S3 credentials are available.

    Args:
        profile: AWS profile name to check (optional)

    Returns:
        Tuple of (credentials_found, hint_message)
    """
    # If profile specified, check credentials file
    if profile:
        access_key, secret_key, _ = _load_aws_credentials_from_profile(profile)
        if access_key and secret_key:
            return True, ""
        else:
            hints = []
            hints.append(f"AWS profile '{profile}' not found or incomplete.")
            hints.append("")
            hints.append("Ensure your ~/.aws/credentials file has this profile:")
            hints.append(f"  [{profile}]")
            hints.append("  aws_access_key_id = YOUR_ACCESS_KEY")
            hints.append("  aws_secret_access_key = YOUR_SECRET_KEY")
            hints.append("")
            hints.append("Or use environment variables instead:")
            hints.append("  export AWS_ACCESS_KEY_ID=your_access_key")
            hints.append("  export AWS_SECRET_ACCESS_KEY=your_secret_key")
            return False, "\n".join(hints)

    # Check environment variables first
    access_key = os.environ.get("AWS_ACCESS_KEY_ID")
    secret_key = os.environ.get("AWS_SECRET_ACCESS_KEY")

    if access_key and secret_key:
        return True, ""

    # Fall back to default profile in ~/.aws/credentials
    access_key, secret_key, _ = _load_aws_credentials_from_profile("default")
    if access_key and secret_key:
        return True, ""

    hints = []
    hints.append("S3 credentials not found. To configure credentials:")
    hints.append("")
    hints.append("Option 1: Set environment variables")
    hints.append("  export AWS_ACCESS_KEY_ID=your_access_key")
    hints.append("  export AWS_SECRET_ACCESS_KEY=your_secret_key")
    hints.append("  export AWS_REGION=us-west-2  # required for most buckets")
    hints.append("")
    hints.append("Option 2: Use --profile flag with AWS credentials file")
    hints.append("  portolan sync --profile myprofile")
    hints.append("")
    hints.append("Option 3: Configure AWS CLI")
    hints.append("  aws configure")

    return False, "\n".join(hints)


def _check_gcs_credentials() -> tuple[bool, str]:
    """Check if GCS credentials are available.

    Checks all credential sources that obstore's GCSStore.from_url() supports.

    Returns:
        Tuple of (credentials_found, hint_message)
    """
    # Check for application default credentials file
    gcloud_creds = os.environ.get("GOOGLE_APPLICATION_CREDENTIALS")
    if gcloud_creds and os.path.exists(gcloud_creds):
        return True, ""

    # Check for service account path (alias)
    sa_path = os.environ.get("GOOGLE_SERVICE_ACCOUNT") or os.environ.get(
        "GOOGLE_SERVICE_ACCOUNT_PATH"
    )
    if sa_path and os.path.exists(sa_path):
        return True, ""

    # Check for inline service account key JSON
    if os.environ.get("GOOGLE_SERVICE_ACCOUNT_KEY"):
        return True, ""

    # Check for ADC in default location (~/.config/gcloud/application_default_credentials.json)
    adc_path = Path.home() / ".config" / "gcloud" / "application_default_credentials.json"
    if adc_path.exists():
        return True, ""

    hints = []
    hints.append("GCS credentials not found. To configure credentials:")
    hints.append("")
    hints.append("Option 1: Set service account key file")
    hints.append("  export GOOGLE_APPLICATION_CREDENTIALS=/path/to/key.json")
    hints.append("")
    hints.append("Option 2: Set inline service account key")
    hints.append("  export GOOGLE_SERVICE_ACCOUNT_KEY='{...json...}'")
    hints.append("")
    hints.append("Option 3: Use application default credentials")
    hints.append("  gcloud auth application-default login")

    return False, "\n".join(hints)


def _check_azure_credentials() -> tuple[bool, str]:
    """Check if Azure credentials are available.

    Checks all credential sources that obstore's AzureStore.from_url() supports.

    Returns:
        Tuple of (credentials_found, hint_message)
    """
    # Check for storage account key (multiple aliases)
    if any(
        os.environ.get(key)
        for key in [
            "AZURE_STORAGE_ACCOUNT_KEY",
            "AZURE_STORAGE_ACCESS_KEY",
            "AZURE_STORAGE_MASTER_KEY",
        ]
    ):
        return True, ""

    # Check for SAS token
    if os.environ.get("AZURE_STORAGE_SAS_TOKEN") or os.environ.get("AZURE_STORAGE_SAS_KEY"):
        return True, ""

    # Check for service principal / managed identity credentials
    if os.environ.get("AZURE_CLIENT_ID"):
        return True, ""

    # Check for federated token (workload identity)
    federated_token = os.environ.get("AZURE_FEDERATED_TOKEN_FILE")
    if federated_token and os.path.exists(federated_token):
        return True, ""

    hints = []
    hints.append("Azure credentials not found. To configure credentials:")
    hints.append("")
    hints.append("Option 1: Set storage account key")
    hints.append("  export AZURE_STORAGE_ACCOUNT_KEY=your_key")
    hints.append("")
    hints.append("Option 2: Set SAS token")
    hints.append("  export AZURE_STORAGE_SAS_TOKEN=your_token")
    hints.append("")
    hints.append("Option 3: Use Azure CLI")
    hints.append("  az login")

    return False, "\n".join(hints)


def check_credentials(destination: str, profile: str | None = None) -> tuple[bool, str]:
    """Check if credentials are available for the destination.

    Args:
        destination: Object store URL (s3://, gs://, az://)
        profile: AWS profile name (for S3 only)

    Returns:
        Tuple of (credentials_ok, hint_message)
    """
    if destination.startswith("s3://"):
        return _check_s3_credentials(profile)
    elif destination.startswith("gs://"):
        return _check_gcs_credentials()
    elif destination.startswith("az://"):
        return _check_azure_credentials()
    else:
        # HTTP or other - assume ok
        return True, ""


# =============================================================================
# Store Setup
# =============================================================================


def _setup_store_and_kwargs(
    bucket_url: str,
    profile: str | None,
    chunk_concurrency: int,
    s3_endpoint: str | None = None,
    s3_region: str | None = None,
    s3_use_ssl: bool = True,
) -> tuple[ObjectStore, dict[str, int]]:
    """Setup object store and upload kwargs.

    Args:
        bucket_url: The object store bucket URL (e.g., s3://bucket)
        profile: AWS profile name (loads credentials from ~/.aws/credentials)
        chunk_concurrency: Max concurrent chunks per file
        s3_endpoint: Custom S3-compatible endpoint (e.g., "minio.example.com:9000")
        s3_region: S3 region (auto-detected from env var or profile config)
        s3_use_ssl: Whether to use HTTPS for S3 endpoint (default: True)

    Returns:
        Tuple of (store, kwargs) where kwargs are passed to obs.put

    Note: For S3, credentials are loaded from (in order):
    1. --profile flag (reads ~/.aws/credentials)
    2. Environment variables (AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY)
    3. Default profile in ~/.aws/credentials (automatic fallback)
    """
    if bucket_url.startswith("s3://"):
        bucket = bucket_url.replace("s3://", "").split("/")[0]

        # Load credentials from profile, environment, or default profile
        access_key: str | None = None
        secret_key: str | None = None
        profile_region: str | None = None

        if profile:
            access_key, secret_key, profile_region = _load_aws_credentials_from_profile(profile)
        else:
            # Try environment variables first
            access_key = os.environ.get("AWS_ACCESS_KEY_ID")
            secret_key = os.environ.get("AWS_SECRET_ACCESS_KEY")

            # Fall back to default profile if no env vars
            if not (access_key and secret_key):
                access_key, secret_key, profile_region = _load_aws_credentials_from_profile(
                    "default"
                )

        # Determine region: explicit flag > env var > profile config > bucket heuristic
        region = s3_region
        if not region:
            region = os.environ.get("AWS_REGION") or os.environ.get("AWS_DEFAULT_REGION")
        if not region and profile_region:
            region = profile_region
        if not region:
            region = _try_infer_region_from_bucket(bucket)

        # Build S3Store with appropriate configuration
        store_kwargs: dict[str, str] = {"region": region} if region else {}

        if access_key and secret_key:
            store_kwargs["access_key_id"] = access_key
            store_kwargs["secret_access_key"] = secret_key

        if s3_endpoint:
            protocol = "https" if s3_use_ssl else "http"
            store_kwargs["endpoint"] = f"{protocol}://{s3_endpoint}"
            if not region:
                store_kwargs["region"] = "us-east-1"  # Default for custom endpoints

        store: ObjectStore = S3Store(bucket, **store_kwargs)  # type: ignore[arg-type]
    else:
        # Non-S3 stores (GCS, Azure, HTTP)
        store = obs.store.from_url(bucket_url)

    kwargs = {"max_concurrency": chunk_concurrency}
    return store, kwargs


# =============================================================================
# Target Key Building
# =============================================================================


def _build_target_key(file_path: Path, source: Path, prefix: str) -> str:
    """Build target key preserving directory structure.

    Args:
        file_path: Path to the file being uploaded
        source: Source directory (base for relative path calculation)
        prefix: Prefix to prepend to the key

    Returns:
        Target key for the object store (always uses forward slashes)
    """
    rel_path = file_path.relative_to(source)
    # Always use POSIX separators for object store keys (forward slashes)
    rel_posix = rel_path.as_posix()
    if prefix:
        return f"{prefix.rstrip('/')}/{rel_posix}"
    return rel_posix


def _get_target_key(source: Path, prefix: str, is_dir_destination: bool) -> str:
    """Determine the target key for a single file upload.

    Args:
        source: Source file path
        prefix: Prefix extracted from destination URL
        is_dir_destination: True if destination ends with '/'

    Returns:
        Target key for the object store
    """
    if is_dir_destination:
        # Destination is a directory, append filename
        return f"{prefix}/{source.name}".strip("/")
    else:
        # Destination is the exact key
        return prefix.strip("/")


# =============================================================================
# Upload Functions
# =============================================================================


def _upload_one_file(
    store: ObjectStore,
    file_path: Path,
    source: Path,
    prefix: str,
    **kwargs: int,
) -> tuple[Path, Exception | None, int]:
    """Upload a single file and return result tuple for parallel processing.

    Args:
        store: Object store instance
        file_path: Path to file to upload
        source: Source directory for relative path calculation
        prefix: Prefix for target key
        **kwargs: Additional args passed to obs.put

    Returns:
        Tuple of (file_path, error_or_none, bytes_uploaded)
    """
    try:
        target_key = _build_target_key(file_path, source, prefix)
        file_size = file_path.stat().st_size
        size_mb = file_size / (1024 * 1024)
        start_time = time.time()

        info(f"Uploading {file_path.name} ({size_mb:.2f} MB) -> {target_key}")

        obs.put(store, target_key, file_path, max_concurrency=kwargs.get("max_concurrency", 12))

        elapsed = time.time() - start_time
        speed_mbps = size_mb / elapsed if elapsed > 0 else 0

        success(f"{file_path.name} ({speed_mbps:.2f} MB/s)")
        return file_path, None, file_size
    except Exception as e:
        error(f"{file_path.name}: {e}")
        return file_path, e, 0


def upload_file(
    source: Path,
    destination: str,
    *,
    profile: str | None = None,
    dry_run: bool = False,
    s3_endpoint: str | None = None,
    s3_region: str | None = None,
    s3_use_ssl: bool = True,
    chunk_concurrency: int = 12,
) -> UploadResult:
    """Upload a single file to S3/GCS/Azure.

    Args:
        source: Local file path
        destination: Object store URL (e.g., s3://bucket/key)
        profile: AWS profile name (for S3 only)
        dry_run: If True, show what would be uploaded without actually uploading
        s3_endpoint: Custom S3-compatible endpoint (e.g., "minio.example.com:9000")
        s3_region: S3 region (default: auto-detected)
        s3_use_ssl: Whether to use HTTPS for S3 endpoint (default: True)
        chunk_concurrency: Max concurrent chunks per file (default: 12)

    Returns:
        UploadResult with upload statistics

    Raises:
        FileNotFoundError: If source file does not exist

    Example:
        >>> result = upload_file(Path("data.parquet"), "s3://bucket/data.parquet")
        >>> if result.success:
        ...     print(f"Uploaded {result.files_uploaded} file(s)")
    """
    if not source.exists():
        raise FileNotFoundError(f"Source file not found: {source}")

    if not source.is_file():
        raise ValueError(f"Source is not a file: {source}")

    bucket_url, prefix = parse_object_store_url(destination)
    target_key = _get_target_key(source, prefix, destination.endswith("/"))
    file_size = source.stat().st_size
    size_mb = file_size / (1024 * 1024)

    if dry_run:
        info(f"[DRY RUN] Would upload: {source.name} ({size_mb:.2f} MB) -> {target_key}")
        return UploadResult(
            success=True,
            files_uploaded=0,
            files_failed=0,
            total_bytes=0,
            errors=[],
        )

    store, kwargs = _setup_store_and_kwargs(
        bucket_url, profile, chunk_concurrency, s3_endpoint, s3_region, s3_use_ssl
    )

    try:
        start_time = time.time()
        info(f"Uploading {source.name} ({size_mb:.2f} MB) -> {target_key}")

        obs.put(store, target_key, source, max_concurrency=kwargs.get("max_concurrency", 12))

        elapsed = time.time() - start_time
        speed_mbps = size_mb / elapsed if elapsed > 0 else 0

        success(f"Upload complete ({speed_mbps:.2f} MB/s)")

        return UploadResult(
            success=True,
            files_uploaded=1,
            files_failed=0,
            total_bytes=file_size,
            errors=[],
        )
    except Exception as e:
        error(f"Upload failed: {e}")
        return UploadResult(
            success=False,
            files_uploaded=0,
            files_failed=1,
            total_bytes=0,
            errors=[(source, e)],
        )


def _find_files_to_upload(source: Path, pattern: str | None) -> list[Path]:
    """Find files to upload from a source directory.

    Args:
        source: Source directory path
        pattern: Optional glob pattern for filtering files

    Returns:
        List of file paths to upload
    """
    if pattern:
        files = list(source.rglob(pattern))
    else:
        files = list(source.rglob("*"))
    return [f for f in files if f.is_file()]


def _print_dry_run_directory(
    files: list[Path], source: Path, prefix: str, total_size_mb: float
) -> None:
    """Print dry-run information for directory upload."""
    info(f"[DRY RUN] Would upload {len(files)} file(s) ({total_size_mb:.2f} MB total)")
    for f in files[:10]:
        target_key = _build_target_key(f, source, prefix)
        detail(f"  {f.name} -> {target_key}")
    if len(files) > 10:
        detail(f"  ... and {len(files) - 10} more file(s)")


def _execute_parallel_uploads(
    store: ObjectStore,
    files: list[Path],
    source: Path,
    prefix: str,
    max_files: int,
    fail_fast: bool,
    kwargs: dict[str, int],
) -> list[tuple[Path, Exception | None, int]]:
    """Execute parallel uploads using ThreadPoolExecutor.

    Args:
        store: Object store instance
        files: List of files to upload
        source: Source directory for relative path calculation
        prefix: Prefix for target keys
        max_files: Max concurrent file uploads
        fail_fast: Stop on first error if True
        kwargs: Additional args for obs.put

    Returns:
        List of (file_path, error_or_none, bytes_uploaded) tuples
    """
    results: list[tuple[Path, Exception | None, int]] = []
    with ThreadPoolExecutor(max_workers=max_files) as executor:
        future_to_file = {
            executor.submit(_upload_one_file, store, f, source, prefix, **kwargs): f for f in files
        }

        for future in as_completed(future_to_file):
            result = future.result()
            results.append(result)
            if fail_fast and result[1] is not None:
                # Cancel remaining futures on first error.
                # Note: future.cancel() only prevents futures that haven't started yet;
                # already-running tasks will complete. This is a Python ThreadPoolExecutor
                # limitation and is acceptable behavior for our use case.
                for pending_future in future_to_file:
                    pending_future.cancel()
                break

    return results


def upload_directory(
    source: Path,
    destination: str,
    *,
    pattern: str | None = None,
    profile: str | None = None,
    max_files: int = 4,
    chunk_concurrency: int = 12,
    fail_fast: bool = False,
    dry_run: bool = False,
    s3_endpoint: str | None = None,
    s3_region: str | None = None,
    s3_use_ssl: bool = True,
) -> UploadResult:
    """Upload a directory to S3/GCS/Azure with parallel uploads.

    Args:
        source: Local directory path
        destination: Object store URL (e.g., s3://bucket/prefix/)
        pattern: Optional glob pattern for filtering files (e.g., "*.parquet")
        profile: AWS profile name (for S3 only)
        max_files: Max number of files to upload in parallel (default: 4)
        chunk_concurrency: Max concurrent chunks per file (default: 12)
        fail_fast: If True, stop on first error; otherwise continue and report at end
        dry_run: If True, show what would be uploaded without actually uploading
        s3_endpoint: Custom S3-compatible endpoint (e.g., "minio.example.com:9000")
        s3_region: S3 region (default: auto-detected)
        s3_use_ssl: Whether to use HTTPS for S3 endpoint (default: True)

    Returns:
        UploadResult with upload statistics

    Example:
        >>> result = upload_directory(
        ...     Path("output/"),
        ...     "s3://bucket/dataset/",
        ...     pattern="*.parquet",
        ... )
        >>> print(f"Uploaded {result.files_uploaded} files")
    """
    if not source.exists():
        raise FileNotFoundError(f"Source directory not found: {source}")

    if not source.is_dir():
        raise ValueError(f"Source is not a directory: {source}")

    files = _find_files_to_upload(source, pattern)

    # Early return for empty directory
    if not files:
        detail(f"No files found in {source}")
        return UploadResult(
            success=True, files_uploaded=0, files_failed=0, total_bytes=0, errors=[]
        )

    total_size = sum(f.stat().st_size for f in files)
    total_size_mb = total_size / (1024 * 1024)
    bucket_url, prefix = parse_object_store_url(destination)

    if dry_run:
        _print_dry_run_directory(files, source, prefix, total_size_mb)
        return UploadResult(
            success=True, files_uploaded=0, files_failed=0, total_bytes=0, errors=[]
        )

    store, kwargs = _setup_store_and_kwargs(
        bucket_url, profile, chunk_concurrency, s3_endpoint, s3_region, s3_use_ssl
    )

    info(f"Found {len(files)} file(s) to upload ({total_size_mb:.2f} MB total)")

    # Ensure max_files is at least 1 to avoid ThreadPoolExecutor ValueError
    max_files = max(1, max_files)

    results = _execute_parallel_uploads(store, files, source, prefix, max_files, fail_fast, kwargs)

    # Calculate results
    errors = [(path, err) for path, err, _ in results if err is not None]
    successful_bytes = sum(bytes_uploaded for _, err, bytes_uploaded in results if err is None)
    files_uploaded = len(results) - len(errors)

    return UploadResult(
        success=len(errors) == 0,
        files_uploaded=files_uploaded,
        files_failed=len(errors),
        total_bytes=successful_bytes,
        errors=errors,
    )
