"""Upload files to cloud object storage (S3, GCS, Azure)."""

from __future__ import annotations

import configparser
import mimetypes
import os
import re
import time
from concurrent.futures import Future, ThreadPoolExecutor, as_completed
from dataclasses import dataclass, field
from functools import lru_cache
from pathlib import Path, PurePosixPath
from typing import TYPE_CHECKING, Any

import obstore as obs
from obstore.store import (
    AzureStore,
    GCSStore,
    HTTPStore,
    LocalStore,
    MemoryStore,
    S3Store,
)

from portolan_cli.sync.report import Report, tell
from portolan_core.errors import InsecureS3EndpointError, ProfileCredentialsError
from portolan_core.trace import submit

if TYPE_CHECKING:
    from obstore import PutMode, PutResult
    from obstore.store import S3Credential, S3CredentialProvider

_AZURE_MIN_PARTS = 2
_AZURE_PREFIX_INDEX = 2
_MAX_FILES_LISTED = 10

# Type alias for all supported object stores
ObjectStore = S3Store | GCSStore | AzureStore | HTTPStore | LocalStore | MemoryStore

# One upload result: the path, the error if the upload failed, and the byte
# count.
UploadResultFuture = Future[tuple[Path, Exception | None, int]]

# Data Classes


@dataclass
class UploadResult:
    """Result of an upload operation."""

    success: bool
    files_uploaded: int
    files_failed: int
    total_bytes: int
    errors: list[tuple[Path, Exception]] = field(default_factory=list)


# URL Parsing


def parse_object_store_url(url: str) -> tuple[str, str]:
    """Parse object store URL into (bucket_url, prefix)."""
    if url.startswith("s3://"):
        parts = url[5:].split("/", 1)
        bucket = parts[0]
        # Strip trailing slashes to prevent double-slash in path construction
        prefix = parts[1].rstrip("/") if len(parts) > 1 else ""
        return f"s3://{bucket}", prefix

    if url.startswith("gs://"):
        parts = url[5:].split("/", 1)
        bucket = parts[0]
        # Strip trailing slashes to prevent double-slash in path construction
        prefix = parts[1].rstrip("/") if len(parts) > 1 else ""
        return f"gs://{bucket}", prefix

    if url.startswith("az://"):
        # Azure: az://account/container/path
        parts = url[5:].split("/", 2)
        if len(parts) < _AZURE_MIN_PARTS:
            raise ValueError(f"Invalid Azure URL: {url}. Expected az://account/container/path")
        account, container = parts[0], parts[1]
        # Strip trailing slashes to prevent double-slash in path construction
        prefix = parts[_AZURE_PREFIX_INDEX].rstrip("/") if len(parts) > _AZURE_PREFIX_INDEX else ""
        return f"az://{account}/{container}", prefix

    if url.startswith(("https://", "http://")):
        # HTTP stores - return as-is
        return url, ""

    raise ValueError(f"Unsupported URL scheme: {url}")


# Credential Loading


def _load_aws_credentials_from_profile(
    profile: str = "default",
) -> tuple[str | None, str | None, str | None, str | None]:
    """Load AWS credentials from the shared AWS credentials file."""
    creds_file = _aws_shared_file("AWS_SHARED_CREDENTIALS_FILE", "credentials")
    config_file = _aws_shared_file("AWS_CONFIG_FILE", "config")

    access_key: str | None = None
    secret_key: str | None = None
    session_token: str | None = None
    region: str | None = None

    # Read credentials
    if creds_file is not None and creds_file.exists():
        parser = configparser.ConfigParser()
        parser.read(creds_file)

        if profile in parser.sections():
            section = parser[profile]
            access_key = section.get("aws_access_key_id")
            secret_key = section.get("aws_secret_access_key")
            session_token = section.get("aws_session_token")
        elif profile == "default" and "DEFAULT" in parser:
            access_key = parser["DEFAULT"].get("aws_access_key_id")
            secret_key = parser["DEFAULT"].get("aws_secret_access_key")
            session_token = parser["DEFAULT"].get("aws_session_token")

    # Read region from config
    if config_file is not None and config_file.exists():
        config = configparser.ConfigParser()
        config.read(config_file)

        # Profile sections in config are named "profile <name>" except for default
        profile_section = profile if profile == "default" else f"profile {profile}"
        if profile_section in config.sections():
            region = config[profile_section].get("region")
        elif profile == "default" and "DEFAULT" in config:
            region = config["DEFAULT"].get("region")

    return access_key, secret_key, session_token, region


def _aws_shared_file(env_var: str, name: str) -> Path | None:
    """Return the path of a shared AWS file."""
    configured = os.environ.get(env_var)
    if configured:
        return Path(configured).expanduser()
    try:
        return Path.home() / ".aws" / name
    except RuntimeError:
        # Path.home() raises when the environment names no home directory.
        return None


def _read_profile_setting(profile: str, key: str) -> str | None:
    """Read one key of a profile from the AWS config file."""
    config_file = _aws_shared_file("AWS_CONFIG_FILE", "config")
    if config_file is None or not config_file.exists():
        return None

    config = configparser.ConfigParser()
    config.read(config_file)

    # Profile sections in config are named "profile <name>" except for default
    profile_section = profile if profile == "default" else f"profile {profile}"
    if profile_section in config.sections():
        return config[profile_section].get(key)
    if profile == "default" and "DEFAULT" in config:
        return config["DEFAULT"].get(key)
    return None


def _effective_profile(profile: str | None) -> str:
    """Return the profile the AWS tools read when the caller names none."""
    return profile or os.environ.get("AWS_PROFILE") or "default"


def _read_profile_endpoint_url(profile: str) -> str | None:
    """Read the ``endpoint_url`` a profile sets in the AWS config file."""
    return _read_profile_setting(profile, "endpoint_url")


def _try_infer_region_from_bucket(bucket: str) -> str | None:
    """Try to infer AWS region from bucket name."""
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


# Credential Checking


def _check_s3_credentials(profile: str | None = None) -> tuple[bool, str]:
    """Check if S3 credentials are available."""
    # With profile credentials, check the file, then credential_process.
    # `_should_load_profile` decides, as the upload does: env keys beat the default profile.
    if profile and _should_load_profile(profile):
        access_key, secret_key, _, _ = _load_aws_credentials_from_profile(profile)
        if access_key and secret_key:
            return True, ""
        if _resolve_credential_provider(profile) is not None:
            return True, ""
        hints = []
        hints.append(f"AWS profile '{profile}' not found or incomplete.")
        hints.append("")
        hints.append("Ensure your ~/.aws/credentials file has this profile:")
        hints.append(f"  [{profile}]")
        hints.append("  aws_access_key_id = YOUR_ACCESS_KEY")
        hints.append("  aws_secret_access_key = YOUR_SECRET_KEY")
        hints.append("")
        hints.append("Or let botocore resolve the profile:")
        hints.append("  pip install 'portolan-cli[aws]'")
        hints.append("  Then credential_process, an assumed role, a web")
        hints.append("  identity token, and IAM Identity Center all work.")
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

    # Fall back to the profile the AWS tools read
    access_key, secret_key, _, _ = _load_aws_credentials_from_profile(_effective_profile(None))
    if access_key and secret_key:
        return True, ""

    # Then to anything botocore resolves from the default profile
    if _resolve_credential_provider(None) is not None:
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
    hints.append("Option 2b: Install the aws extra to use any AWS profile type")
    hints.append("  pip install 'portolan-cli[aws]'")
    hints.append("")
    hints.append("Option 3: Configure AWS CLI")
    hints.append("  aws configure")

    return False, "\n".join(hints)


def _check_gcs_credentials() -> tuple[bool, str]:
    """Check if GCS credentials are available."""
    # Check for application default credentials file
    gcloud_creds = os.environ.get("GOOGLE_APPLICATION_CREDENTIALS")
    if gcloud_creds and Path(gcloud_creds).exists():
        return True, ""

    # Check for service account path (alias)
    sa_path = os.environ.get("GOOGLE_SERVICE_ACCOUNT") or os.environ.get(
        "GOOGLE_SERVICE_ACCOUNT_PATH"
    )
    if sa_path and Path(sa_path).exists():
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
    """Check if Azure credentials are available."""
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
    if federated_token and Path(federated_token).exists():
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
    """Check if credentials are available for the destination."""
    if destination.startswith("s3://"):
        return _check_s3_credentials(profile)
    if destination.startswith("gs://"):
        return _check_gcs_credentials()
    if destination.startswith("az://"):
        return _check_azure_credentials()
    # HTTP or other - assume ok
    return True, ""


# Store Setup


def _resolve_s3_endpoint_settings(
    s3_endpoint: str | None,
    s3_use_ssl: bool | None,
    profile: str | None = None,
    *,
    report: Report | None = None,
) -> tuple[str | None, bool]:
    """Resolve S3 endpoint settings from the argument, the environment, or the profile."""
    from portolan_core.config import resolve_s3_endpoint_settings

    environment_settings = resolve_s3_endpoint_settings()
    endpoint = s3_endpoint if s3_endpoint is not None else environment_settings.endpoint
    use_ssl = s3_use_ssl if s3_use_ssl is not None else environment_settings.use_ssl
    if endpoint is not None:
        return endpoint, use_ssl

    # obstore reads AWS_ENDPOINT_URL itself, with its AWS_ALLOW_HTTP opt-in.
    # Leave it to obstore, so a plaintext local endpoint keeps working.
    if os.environ.get("AWS_ENDPOINT_URL_S3"):
        aws_endpoint: str | None = os.environ["AWS_ENDPOINT_URL_S3"]
        source = "AWS_ENDPOINT_URL_S3"
    elif os.environ.get("AWS_ENDPOINT_URL"):
        return None, use_ssl
    else:
        # The profile applies even when the caller names none, as it does for
        # the AWS CLI.
        effective_profile = _effective_profile(profile)
        aws_endpoint = _read_profile_endpoint_url(effective_profile)
        source = f"AWS profile '{effective_profile}'"
    if aws_endpoint is None:
        return None, use_ssl

    # Name the source, so a redirected upload is never silent.
    # The caller may print this line, so it holds no credential of the URL.
    from portolan_cli.utils import redact_credentials

    tell(report, "detail", f"Endpoint {redact_credentials(aws_endpoint)} from {source}")

    # Both sources write a full URL, so its scheme carries the TLS setting.
    # An explicit argument or PORTOLAN_S3_USE_SSL still wins.
    if s3_use_ssl is None and os.environ.get("PORTOLAN_S3_USE_SSL") is None:
        use_ssl = not aws_endpoint.startswith("http://")
    return aws_endpoint, use_ssl


def _should_load_profile(profile: str) -> bool:
    """Return whether a named profile should override environment credentials."""
    has_environment_credentials = os.environ.get("AWS_ACCESS_KEY_ID") and os.environ.get(
        "AWS_SECRET_ACCESS_KEY"
    )
    return profile != "default" or not has_environment_credentials


S3Credentials = tuple[str | None, str | None, str | None, str | None]


def _resolve_s3_credentials(profile: str | None) -> S3Credentials:
    """Load credentials from an explicit profile or the process environment."""
    if profile is not None and _should_load_profile(profile):
        return _load_aws_credentials_from_profile(profile)

    access_key = os.environ.get("AWS_ACCESS_KEY_ID")
    secret_key = os.environ.get("AWS_SECRET_ACCESS_KEY")
    session_token = os.environ.get("AWS_SESSION_TOKEN")
    if access_key and secret_key:
        return access_key, secret_key, session_token, None
    return _load_aws_credentials_from_profile(_effective_profile(profile))


class _GuardedCredentialProvider:
    """Report a credential failure of a later call with its profile."""

    def __init__(self, provider: S3CredentialProvider, profile: str) -> None:
        self._provider = provider
        self._profile = profile
        config = getattr(provider, "config", None)
        if config is not None:
            # obstore reads the region of the session from this attribute.
            self.config = config

    def __call__(self) -> S3Credential:
        """Fetch the credentials, or report the profile that fails."""
        try:
            return self._provider()  # type: ignore[return-value]
        except ProfileCredentialsError:
            raise
        except Exception as error:
            raise ProfileCredentialsError(self._profile, str(error)) from error


# shortcut: keyed on the profile name only, so a later AWS env var change keeps the first
# provider. Key on the config file paths too if a process changes them.
@lru_cache(maxsize=8)
def _boto3_credential_provider(profile: str | None) -> S3CredentialProvider | None:
    """Build the provider that botocore backs, once for each profile."""
    try:
        import boto3
        from botocore.exceptions import ProfileNotFound
        from obstore.auth.boto3 import Boto3CredentialProvider
    except ImportError:
        return None

    try:
        built = Boto3CredentialProvider(boto3.Session(profile_name=profile))
    except (ProfileNotFound, ValueError):
        # A missing profile, or a profile that resolves to no credentials.
        # obstore raises ValueError for the second case.
        return None
    except Exception as error:
        # A credential_process that fails, an assume-role that is refused, or
        # an expired SSO token. The store must not go out unauthenticated.
        raise ProfileCredentialsError(_effective_profile(profile), str(error)) from error

    return _GuardedCredentialProvider(built, _effective_profile(profile))


def _resolve_credential_provider(profile: str | None) -> S3CredentialProvider | None:
    """Build a credential provider that botocore backs, through obstore."""
    if profile is not None and not _should_load_profile(profile):
        return None

    access_key, secret_key, _, _ = _load_aws_credentials_from_profile(_effective_profile(profile))
    if access_key and secret_key:
        return None

    return _boto3_credential_provider(profile)


def _resolve_s3_region(
    s3_region: str | None, profile_region: str | None, bucket: str
) -> str | None:
    """Resolve a region from explicit, environment, profile, or bucket settings."""
    if s3_region:
        return s3_region
    environment_region = os.environ.get("AWS_REGION") or os.environ.get("AWS_DEFAULT_REGION")
    if environment_region:
        return environment_region
    if profile_region:
        return profile_region
    return _try_infer_region_from_bucket(bucket)


def _normalize_s3_endpoint(endpoint: str) -> str:
    """Remove an optional scheme from an S3-compatible endpoint."""
    if endpoint.startswith("https://"):
        return endpoint[8:]
    if endpoint.startswith("http://"):
        return endpoint[7:]
    return endpoint


def _add_s3_endpoint_settings(
    store_kwargs: dict[str, str | bool], endpoint: str | None, use_ssl: bool
) -> None:
    """Add path-style custom endpoint settings to S3 store options."""
    if endpoint is None:
        return
    store_kwargs["virtual_hosted_style_request"] = False
    protocol = "https" if use_ssl else "http"
    store_kwargs["endpoint"] = f"{protocol}://{_normalize_s3_endpoint(endpoint)}"
    if "region" not in store_kwargs:
        store_kwargs["region"] = "us-east-1"


def _create_s3_store(
    bucket_url: str,
    profile: str | None,
    s3_endpoint: str | None,
    s3_region: str | None,
    s3_use_ssl: bool | None,
    *,
    report: Report | None = None,
) -> ObjectStore:
    """Create an S3 store with credentials and endpoint settings."""
    endpoint, use_ssl = _resolve_s3_endpoint_settings(
        s3_endpoint, s3_use_ssl, profile, report=report
    )
    bucket = bucket_url.replace("s3://", "").split("/")[0]
    access_key, secret_key, session_token, profile_region = _resolve_s3_credentials(profile)
    credential_provider = None
    if not (access_key and secret_key):
        credential_provider = _resolve_credential_provider(profile)
    if profile_region is None:
        # Environment keys skip the profile credentials, but the endpoint still
        # comes from the profile. Read the region the profile signs with.
        profile_region = _read_profile_setting(_effective_profile(profile), "region")
    region = _resolve_s3_region(s3_region, profile_region, bucket)

    has_credentials = any((access_key, secret_key, session_token, credential_provider))
    if endpoint and not use_ssl and has_credentials:
        raise InsecureS3EndpointError(_normalize_s3_endpoint(endpoint))

    store_kwargs: dict[str, Any] = {}
    if region:
        store_kwargs["region"] = region
    if credential_provider is not None:
        store_kwargs["credential_provider"] = credential_provider
    if access_key and secret_key:
        store_kwargs["access_key_id"] = access_key
        store_kwargs["secret_access_key"] = secret_key
        if session_token:
            store_kwargs["aws_session_token"] = session_token
    if "." in bucket:
        store_kwargs["virtual_hosted_style_request"] = False
    _add_s3_endpoint_settings(store_kwargs, endpoint, use_ssl)

    if endpoint and not use_ssl:
        store_kwargs["skip_signature"] = True
        return S3Store(bucket, client_options={"allow_http": True}, **store_kwargs)
    return S3Store(bucket, **store_kwargs)


def _setup_store_and_kwargs(
    bucket_url: str,
    profile: str | None,
    chunk_concurrency: int,
    s3_endpoint: str | None = None,
    s3_region: str | None = None,
    s3_use_ssl: bool | None = None,
    *,
    report: Report | None = None,
) -> tuple[ObjectStore, dict[str, int]]:
    """Setup object store and upload kwargs."""
    if bucket_url.startswith("s3://"):
        store = _create_s3_store(
            bucket_url, profile, s3_endpoint, s3_region, s3_use_ssl, report=report
        )
    else:
        # Non-S3 stores (GCS, Azure, HTTP)
        store = obs.store.from_url(bucket_url)

    return store, {"max_concurrency": chunk_concurrency}


def setup_store(
    destination: str,
    *,
    profile: str | None = None,
    region: str | None = None,
    report: Report | None = None,
) -> tuple[ObjectStore, str]:
    """Setup object store from a destination URL."""
    bucket_url, prefix = parse_object_store_url(destination)
    store, _ = _setup_store_and_kwargs(
        bucket_url,
        profile=profile,
        chunk_concurrency=4,  # Default, not used for non-upload operations
        s3_region=region,
        report=report,
    )
    return store, prefix


# Target Key Building


def _build_target_key(file_path: Path, source: Path, prefix: str) -> str:
    """Build target key preserving directory structure."""
    rel_path = file_path.relative_to(source)
    # Always use POSIX separators for object store keys (forward slashes)
    rel_posix = rel_path.as_posix()
    if prefix:
        return f"{prefix.rstrip('/')}/{rel_posix}"
    return rel_posix


def _get_target_key(source: Path, prefix: str, is_dir_destination: bool) -> str:
    """Determine the target key for a single file upload."""
    if is_dir_destination:
        # Destination is a directory, append filename
        return f"{prefix}/{source.name}".strip("/")
    # Destination is the exact key
    return prefix.strip("/")


# The one put helper

# Azure serves an object sent without a type as application/octet-stream.
# The stdlib table lacks or disagrees on these types.
_CONTENT_TYPES: dict[str, str] = {
    ".json": "application/json",
    ".geojson": "application/geo+json",
    ".parquet": "application/vnd.apache.parquet",
    ".tif": "image/tiff; application=geotiff; profile=cloud-optimized",
    ".tiff": "image/tiff; application=geotiff; profile=cloud-optimized",
    ".pmtiles": "application/vnd.pmtiles",
    ".png": "image/png",
    ".md": "text/markdown",
    # The stdlib reads the Windows registry, which maps .xml to text/xml.
    ".xml": "application/xml",
}

# S3 and GCS accept at most 10000 parts, and obstore sends 5 MiB parts by
# default. The part size grows so that no file needs more than 9000 parts.
MIN_PART_SIZE = 5 * 1024 * 1024
_MAX_PARTS = 9000


def content_type_for(key: str) -> str:
    """Return the Content-Type for an object key, from its extension."""
    suffix = PurePosixPath(key).suffix.lower()
    guessed = _CONTENT_TYPES.get(suffix) or mimetypes.guess_type(key)[0]
    return guessed or "application/octet-stream"


def multipart_chunk_size(size: int) -> int:
    """Return the multipart part size for an object of ``size`` bytes."""
    return max(MIN_PART_SIZE, -(-size // _MAX_PARTS))


def _put_kwargs(
    key: str, data: bytes | Path, mode: PutMode | None, max_concurrency: int | None
) -> dict[str, Any]:
    """Build the obstore put arguments. The parameters are those of ``put_object``."""
    size = data.stat().st_size if isinstance(data, Path) else len(data)
    kwargs: dict[str, Any] = {
        "attributes": {"Content-Type": content_type_for(key)},
        "chunk_size": multipart_chunk_size(size),
    }
    if mode is not None:
        kwargs["mode"] = mode
    if max_concurrency is not None:
        kwargs["max_concurrency"] = max_concurrency
    return kwargs


def put_object(
    store: ObjectStore,
    key: str,
    data: bytes | Path,
    *,
    mode: PutMode | None = None,
    max_concurrency: int | None = None,
) -> PutResult:
    """Write one object. Every write in portolan goes through this function."""
    return obs.put(store, key, data, **_put_kwargs(key, data, mode, max_concurrency))


async def put_object_async(
    store: ObjectStore,
    key: str,
    data: bytes | Path,
    *,
    mode: PutMode | None = None,
    max_concurrency: int | None = None,
) -> PutResult:
    """Write one object without blocking the event loop.

    The arguments are those of ``put_object``.
    """
    return await obs.put_async(store, key, data, **_put_kwargs(key, data, mode, max_concurrency))


# S3 CopyObject refuses a source above 5 GiB. A larger object streams through
# the client, in the multipart part size of an upload of the same size.
COPY_SOURCE_LIMIT = 5 * 2**30


async def copy_object_async(store: ObjectStore, src: str, dst: str, size: int | None) -> None:
    """Copy ``src`` to ``dst``. A source of unknown size tries the server copy."""
    if size is None or size <= COPY_SOURCE_LIMIT:
        await obs.copy_async(store, src, dst)
        return
    source = await obs.get_async(store, src)
    chunk = multipart_chunk_size(size)
    await obs.put_async(
        store,
        dst,
        source.stream(min_chunk_size=chunk),
        attributes=source.attributes,
        chunk_size=chunk,
    )


# Upload Functions


def _upload_one_file(
    store: ObjectStore,
    file_path: Path,
    source: Path,
    prefix: str,
    verbose: bool = False,
    *,
    report: Report | None = None,
    **kwargs: int,
) -> tuple[Path, Exception | None, int]:
    """Upload a single file and return result tuple for parallel processing."""
    try:
        target_key = _build_target_key(file_path, source, prefix)
        file_size = file_path.stat().st_size
        size_mb = file_size / (1024 * 1024)
        start_time = time.time()

        # Per-file output only in verbose mode
        if verbose:
            tell(report, "info", f"Uploading {file_path.name} ({size_mb:.2f} MB) -> {target_key}")

        put_object(store, target_key, file_path, max_concurrency=kwargs.get("max_concurrency", 4))

        elapsed = time.time() - start_time
        speed_mbps = size_mb / elapsed if elapsed > 0 else 0

        # Per-file output only in verbose mode
        if verbose:
            tell(report, "success", f"{file_path.name} ({speed_mbps:.2f} MB/s)")
        return file_path, None, file_size
    except Exception as e:
        # Errors are always shown
        tell(report, "error", f"{file_path.name}: {e}")
        return file_path, e, 0


def upload_file(
    source: Path,
    destination: str,
    *,
    profile: str | None = None,
    dry_run: bool = False,
    s3_endpoint: str | None = None,
    s3_region: str | None = None,
    s3_use_ssl: bool | None = None,
    chunk_concurrency: int = 4,
    report: Report | None = None,
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
        chunk_concurrency: Max concurrent chunks per file (default: 4)
        report: Receives each line of the upload. Without it, the lines go to the log.

    Returns:
        UploadResult with upload statistics

    Raises:
        FileNotFoundError: If source file does not exist
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
        tell(
            report,
            "info",
            f"[DRY RUN] Would upload: {source.name} ({size_mb:.2f} MB) -> {target_key}",
        )
        return UploadResult(
            success=True,
            files_uploaded=0,
            files_failed=0,
            total_bytes=0,
            errors=[],
        )

    store, kwargs = _setup_store_and_kwargs(
        bucket_url, profile, chunk_concurrency, s3_endpoint, s3_region, s3_use_ssl, report=report
    )

    try:
        start_time = time.time()
        tell(report, "info", f"Uploading {source.name} ({size_mb:.2f} MB) -> {target_key}")

        put_object(store, target_key, source, max_concurrency=kwargs.get("max_concurrency", 4))

        elapsed = time.time() - start_time
        speed_mbps = size_mb / elapsed if elapsed > 0 else 0

        tell(report, "success", f"Upload complete ({speed_mbps:.2f} MB/s)")

        return UploadResult(
            success=True,
            files_uploaded=1,
            files_failed=0,
            total_bytes=file_size,
            errors=[],
        )
    except Exception as e:
        tell(report, "error", f"Upload failed: {e}")
        return UploadResult(
            success=False,
            files_uploaded=0,
            files_failed=1,
            total_bytes=0,
            errors=[(source, e)],
        )


def _find_files_to_upload(source: Path, pattern: str | None) -> list[Path]:
    """Find files to upload from a source directory."""
    files = list(source.rglob(pattern)) if pattern else list(source.rglob("*"))
    return [f for f in files if f.is_file()]


def _print_dry_run_directory(
    files: list[Path],
    source: Path,
    prefix: str,
    total_size_mb: float,
    *,
    report: Report | None = None,
) -> None:
    """Print dry-run information for directory upload."""
    tell(
        report,
        "info",
        f"[DRY RUN] Would upload {len(files)} file(s) ({total_size_mb:.2f} MB total)",
    )
    for f in files[:10]:
        target_key = _build_target_key(f, source, prefix)
        tell(report, "detail", f"  {f.name} -> {target_key}")
    if len(files) > _MAX_FILES_LISTED:
        tell(report, "detail", f"  ... and {len(files) - _MAX_FILES_LISTED} more file(s)")


def _execute_parallel_uploads(
    store: ObjectStore,
    files: list[Path],
    source: Path,
    prefix: str,
    max_files: int,
    fail_fast: bool,
    kwargs: dict[str, int],
    *,
    verbose: bool = False,
    report: Report | None = None,
) -> list[tuple[Path, Exception | None, int]]:
    """Execute parallel uploads using ThreadPoolExecutor."""
    results: list[tuple[Path, Exception | None, int]] = []

    if fail_fast:
        # For fail_fast mode, submit futures incrementally to ensure we can stop
        # after the first error without starting additional tasks.
        with ThreadPoolExecutor(max_workers=max_files) as executor:
            pending: dict[UploadResultFuture, Path] = {}
            files_iter = iter(files)

            # Submit initial batch up to max_files
            for f in files_iter:
                future: UploadResultFuture = submit(
                    executor,
                    _upload_one_file,
                    store,
                    f,
                    source,
                    prefix,
                    verbose,
                    report=report,
                    **kwargs,
                )
                pending[future] = f
                if len(pending) >= max_files:
                    break

            while pending:
                # Wait for next completed future
                done: UploadResultFuture = next(iter(as_completed(pending)))
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
                    next_file = next(files_iter)
                    new_future: UploadResultFuture = submit(
                        executor,
                        _upload_one_file,
                        store,
                        next_file,
                        source,
                        prefix,
                        verbose,
                        report=report,
                        **kwargs,
                    )
                    pending[new_future] = next_file
                except StopIteration:
                    pass
    else:
        # For non-fail_fast mode, submit all futures upfront for maximum parallelism
        with ThreadPoolExecutor(max_workers=max_files) as executor:
            future_to_file = {
                submit(
                    executor,
                    _upload_one_file,
                    store,
                    f,
                    source,
                    prefix,
                    verbose,
                    report=report,
                    **kwargs,
                ): f
                for f in files
            }

            for future in as_completed(future_to_file):
                result = future.result()
                results.append(result)

    return results


def upload_directory(  # noqa: PLR0913 - each upload option is an argument
    source: Path,
    destination: str,
    *,
    pattern: str | None = None,
    profile: str | None = None,
    max_files: int = 4,
    chunk_concurrency: int = 4,
    fail_fast: bool = False,
    dry_run: bool = False,
    s3_endpoint: str | None = None,
    s3_region: str | None = None,
    s3_use_ssl: bool | None = None,
    verbose: bool = False,
    report: Report | None = None,
) -> UploadResult:
    """Upload a directory to S3/GCS/Azure with parallel uploads."""
    if not source.exists():
        raise FileNotFoundError(f"Source directory not found: {source}")

    if not source.is_dir():
        raise ValueError(f"Source is not a directory: {source}")

    files = _find_files_to_upload(source, pattern)

    # Early return for empty directory
    if not files:
        tell(report, "detail", f"No files found in {source}")
        return UploadResult(
            success=True, files_uploaded=0, files_failed=0, total_bytes=0, errors=[]
        )

    total_size = sum(f.stat().st_size for f in files)
    total_size_mb = total_size / (1024 * 1024)
    bucket_url, prefix = parse_object_store_url(destination)

    if dry_run:
        _print_dry_run_directory(files, source, prefix, total_size_mb, report=report)
        return UploadResult(
            success=True, files_uploaded=0, files_failed=0, total_bytes=0, errors=[]
        )

    store, kwargs = _setup_store_and_kwargs(
        bucket_url, profile, chunk_concurrency, s3_endpoint, s3_region, s3_use_ssl, report=report
    )

    tell(report, "info", f"Found {len(files)} file(s) to upload ({total_size_mb:.2f} MB total)")

    # Ensure max_files is at least 1 to avoid ThreadPoolExecutor ValueError
    max_files = max(1, max_files)

    results = _execute_parallel_uploads(
        store, files, source, prefix, max_files, fail_fast, kwargs, verbose=verbose, report=report
    )

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
