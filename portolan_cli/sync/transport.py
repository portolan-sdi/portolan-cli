"""Read a remote catalog over the object store and clone it."""

from __future__ import annotations

import json
import tempfile
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

from portolan_cli.catalog import init_catalog, intermediate_catalog_ids
from portolan_cli.sync.download import download_file
from portolan_cli.sync.pull import PullError, PullResult, pull
from portolan_cli.sync.report import Report, silent
from portolan_core.json_io import write_json_atomic

_MIN_URL_PARTS = 3

# URL Parsing Utilities


def infer_local_path_from_url(remote_url: str) -> Path:
    """Infer local directory name from a remote catalog URL."""
    # Strip trailing slashes
    url = remote_url.rstrip("/")

    # Split by '/' and get last non-empty component
    parts = url.split("/")

    # Filter out empty parts (from multiple slashes)
    parts = [p for p in parts if p]

    # Need at least scheme://bucket/catalog, such as ['s3:', '', 'bucket', 'catalog'].
    if len(parts) < _MIN_URL_PARTS:
        raise ValueError(
            f"Cannot infer local path from URL: {remote_url}. "
            "URL must include a catalog name (e.g., s3://bucket/catalog-name)."
        )

    catalog_name = parts[-1]

    # The last part should not be empty or just the bucket
    if not catalog_name or catalog_name.endswith(":"):
        raise ValueError(
            f"Cannot infer local path from URL: {remote_url}. "
            "URL must include a catalog name (e.g., s3://bucket/catalog-name)."
        )

    return Path(catalog_name)


# Remote Catalog Fetching


def _fetch_remote_catalog_json(
    remote_url: str,
    *,
    profile: str | None = None,
    report: Report = silent,
) -> dict[str, Any]:
    """Fetch and parse catalog.json from a remote catalog."""
    # Build remote catalog.json path
    catalog_url = f"{remote_url.rstrip('/')}/catalog.json"

    # Download to temp file
    with tempfile.NamedTemporaryFile(suffix=".json", delete=False) as tmp:
        tmp_path = Path(tmp.name)

    try:
        result = download_file(
            source=catalog_url,
            destination=tmp_path,
            profile=profile,
            report=report,
        )

        if not result.success:
            error_msgs = (
                ", ".join(f"{path}: {exc}" for path, exc in result.errors)
                if result.errors
                else "Unknown error"
            )
            raise CloneError(f"Failed to fetch remote catalog.json: {error_msgs}")

        # Parse the downloaded file
        with Path(tmp_path).open(encoding="utf-8") as f:
            data: dict[str, Any] = json.load(f)
            return data

    except json.JSONDecodeError as e:
        raise CloneError(f"Failed to parse remote catalog.json: {e}") from e

    finally:
        # Clean up temp file
        if tmp_path.exists():
            tmp_path.unlink()


def list_remote_collections(
    remote_url: str,
    *,
    profile: str | None = None,
    max_depth: int = 10,
    report: Report = silent,
) -> list[str]:
    """List all collections available in a remote catalog."""
    # Track visited URLs to detect circular references
    visited_urls: set[str] = set()

    return _list_remote_collections_recursive(
        remote_url=remote_url,
        profile=profile,
        max_depth=max_depth,
        current_depth=0,
        visited_urls=visited_urls,
        path_prefix="",  # Start with empty prefix at root
        report=report,
    )


def _resolve_href(base_url: str, href: str) -> str:
    """Resolve a potentially relative href against a base URL."""
    # If href is already absolute (has scheme), return as-is
    if "://" in href:
        return href

    # Handle relative paths
    # Strip trailing slash from base
    base = base_url.rstrip("/")

    # Handle "./" prefix
    href = href.removeprefix("./")

    # Handle "../" prefix
    while href.startswith("../"):
        href = href[3:]
        # Go up one directory in base
        if "/" in base:
            base = base.rsplit("/", 1)[0]

    return f"{base}/{href}"


def _extract_catalog_url_from_href(base_url: str, href: str) -> str:
    """Extract the catalog directory URL from a catalog.json href."""
    full_url = _resolve_href(base_url, href)

    # Remove the catalog.json suffix to get the catalog directory
    if full_url.endswith("/catalog.json"):
        return full_url[: -len("/catalog.json")]
    if full_url.endswith("catalog.json"):
        return full_url[: -len("catalog.json")].rstrip("/")

    return full_url


def _list_remote_collections_recursive(
    remote_url: str,
    *,
    profile: str | None,
    max_depth: int,
    current_depth: int,
    visited_urls: set[str],
    path_prefix: str = "",
    report: Report = silent,
) -> list[str]:
    """Recursively list collections from a catalog and its subcatalogs."""
    # Normalize URL for visited check
    normalized_url = remote_url.rstrip("/")

    # Check for circular reference
    if normalized_url in visited_urls:
        return []

    # Check depth limit
    if current_depth >= max_depth:
        report("warn", f"Maximum catalog depth ({max_depth}) reached at {remote_url}")
        return []

    # Mark as visited
    visited_urls.add(normalized_url)

    # Fetch this catalog
    catalog_data = _fetch_remote_catalog_json(remote_url, profile=profile, report=report)

    collections: list[str] = []

    # Parse STAC links to find child collections and subcatalogs
    links = catalog_data.get("links", [])
    for link in links:
        if link.get("rel") != "child":
            continue

        href = link.get("href", "")
        if not href:
            continue

        # Determine if this is a subcatalog or a collection
        if href.endswith("catalog.json"):
            # This is a subcatalog - recurse into it
            subcatalog_url = _extract_catalog_url_from_href(remote_url, href)

            # Extract the subcatalog directory name to build the path prefix
            # e.g., "./climate/catalog.json" -> "climate"
            subcatalog_dir = _extract_subcatalog_dir_from_href(href)
            new_prefix = f"{path_prefix}{subcatalog_dir}/" if subcatalog_dir else path_prefix

            nested_collections = _list_remote_collections_recursive(
                remote_url=subcatalog_url,
                profile=profile,
                max_depth=max_depth,
                current_depth=current_depth + 1,
                visited_urls=visited_urls,
                path_prefix=new_prefix,
                report=report,
            )
            collections.extend(nested_collections)

        elif href.endswith("collection.json"):
            # This is an actual collection - extract the name with full path
            collection_name = _extract_collection_name_from_href(href)
            if collection_name:
                # Prepend path prefix for nested collections
                full_path = f"{path_prefix}{collection_name}" if path_prefix else collection_name
                collections.append(full_path)

        else:
            # Unknown link type - could be a collection without .json suffix
            # Try to extract a name anyway (backwards compatibility)
            collection_name = _extract_collection_name_from_href(href)
            if collection_name:
                full_path = f"{path_prefix}{collection_name}" if path_prefix else collection_name
                collections.append(full_path)

    return collections


def _extract_subcatalog_dir_from_href(href: str) -> str:
    """Extract the subcatalog directory name from an href."""
    # Remove catalog.json suffix
    if href.endswith("/catalog.json"):
        href = href[: -len("/catalog.json")]
    elif href.endswith("catalog.json"):
        href = href[: -len("catalog.json")]

    # Remove any trailing slashes
    href = href.rstrip("/")

    # Handle relative paths
    href = href.removeprefix("./")

    # Get the last path component (subcatalog directory name)
    if "/" in href:
        return href.split("/")[-1]

    return href


def _extract_collection_name_from_href(href: str) -> str:
    """Extract collection path from an href."""
    # Remove collection.json suffix if present
    if href.endswith("/collection.json"):
        href = href[: -len("/collection.json")]
    elif href.endswith("collection.json"):
        href = href[: -len("collection.json")]

    # Remove any trailing slashes
    href = href.rstrip("/")

    # Handle relative paths - preserve full structure
    if href.startswith("./"):
        return href[2:]  # Remove "./" prefix, keep rest (e.g., "environment/bodemkwaliteit")

    # Handle absolute URLs - extract only leaf directory
    # (absolute URLs are already resolved to a specific location)
    if "://" in href:
        # Get the last path component for absolute URLs
        if "/" in href:
            return href.split("/")[-1]
        return href

    # For other cases (bare relative paths), return as-is
    return href


# Clone Function


@dataclass
class CloneResult:
    """Result of a clone operation."""

    success: bool
    pull_result: PullResult | None
    local_path: Path
    errors: list[str] = field(default_factory=list)
    collections_cloned: list[str] = field(default_factory=list)
    total_files_downloaded: int = 0

    def to_dict(self) -> dict[str, Any]:
        """Return the envelope ``data`` of a successful ``portolan clone``."""
        data: dict[str, Any] = {
            "local_path": str(self.local_path),
            "collections_cloned": self.collections_cloned,
            "total_files_downloaded": self.total_files_downloaded,
        }
        if self.pull_result is not None:
            data["pull"] = {
                "files_downloaded": self.pull_result.files_downloaded,
                "remote_version": self.pull_result.remote_version,
            }
        return data


class CloneError(Exception):
    """Exception raised when clone fails."""


def _drop_missing_child_links(local_path: Path, collections: list[str]) -> None:
    """Drop each child link of a cloned catalog.json that the clone cannot resolve."""
    ids = {"", *(i for coll in collections for i in intermediate_catalog_ids(coll))}
    for path in (local_path / catalog_id / "catalog.json" for catalog_id in sorted(ids)):
        if not path.is_file():
            continue
        document = json.loads(path.read_text(encoding="utf-8"))
        links = document.get("links", [])
        kept = [
            link
            for link in links
            if link.get("rel") != "child"
            or "://" in str(link.get("href", ""))
            or (path.parent / str(link.get("href", ""))).is_file()
        ]
        if len(kept) != len(links):
            document["links"] = kept
            write_json_atomic(path, document)


def clone(  # noqa: PLR0915 - one ordered clone sequence
    remote_url: str,
    local_path: Path,
    collection: str | None = None,
    *,
    profile: str | None = None,
    report: Report = silent,
    show_progress: bool = False,
) -> CloneResult:
    """Clone a remote catalog to a local directory."""
    errors: list[str] = []
    collections_cloned: list[str] = []
    total_files: int = 0

    # Check if target already exists
    if local_path.exists():
        contents = list(local_path.iterdir())
        if contents:
            error_msg = f"Target directory is not empty: {local_path}"
            report("error", error_msg)
            return CloneResult(
                success=False,
                pull_result=None,
                local_path=local_path,
                errors=[error_msg],
            )

    # Determine which collections to clone
    if collection is None:
        # Clone all collections - fetch remote catalog to discover them
        report("info", "Discovering remote collections...")
        try:
            collections_to_clone = list_remote_collections(
                remote_url, profile=profile, report=report
            )
        except CloneError as e:
            error_msg = str(e)
            report("error", error_msg)
            return CloneResult(
                success=False,
                pull_result=None,
                local_path=local_path,
                errors=[error_msg],
            )

        if not collections_to_clone:
            error_msg = f"Remote catalog has no collections to clone: {remote_url}"
            report("error", error_msg)
            return CloneResult(
                success=False,
                pull_result=None,
                local_path=local_path,
                errors=[error_msg],
            )

        report(
            "info",
            f"Found {len(collections_to_clone)} collection(s): {', '.join(collections_to_clone)}",
        )
    else:
        collections_to_clone = [collection]

    # Create target directory
    report("info", f"Cloning to {local_path}...")
    local_path.mkdir(parents=True, exist_ok=True)

    # Initialize catalog
    report("info", "Initializing catalog...")
    try:
        init_catalog(
            local_path,
            title=f"Clone of {remote_url}",
            description=f"Cloned from {remote_url}",
            # No license here: each cloned collection.json arrives carrying the
            # license the publisher set, and a local default would misstate it.
            license_id=None,
        )
    except Exception as e:
        error_msg = f"Failed to initialize catalog: {e}"
        errors.append(error_msg)
        report("error", error_msg)
        return CloneResult(
            success=False,
            pull_result=None,
            local_path=local_path,
            errors=errors,
        )

    # Pull each collection
    last_pull_result: PullResult | None = None

    for coll in collections_to_clone:
        report("info", f"Pulling collection '{coll}' from {remote_url}...")
        try:
            pull_result = pull(
                remote_url=remote_url,
                local_root=local_path,
                collection=coll,
                force=False,  # No local changes to force-overwrite
                dry_run=False,
                profile=profile,
                stac_tree=True,
                report=report,
                show_progress=show_progress,
            )

            if not pull_result.success:
                # Check if remote doesn't exist
                if pull_result.remote_version is None:
                    error_msg = f"Remote collection '{coll}' not found at {remote_url}"
                else:
                    error_msg = f"Pull failed for collection '{coll}'"
                errors.append(error_msg)
                report("error", error_msg)
                # Continue with other collections instead of failing immediately
                continue

            collections_cloned.append(coll)
            total_files += pull_result.files_downloaded
            last_pull_result = pull_result
            report("detail", f"Cloned {pull_result.files_downloaded} file(s) from '{coll}'")

        except PullError as e:
            error_msg = f"Clone failed for collection '{coll}': {e}"
            errors.append(error_msg)
            report("error", error_msg)
            # Continue with other collections
            continue

    _drop_missing_child_links(local_path, collections_to_clone)

    # Determine overall success
    if not collections_cloned:
        # No collections were successfully cloned
        return CloneResult(
            success=False,
            pull_result=last_pull_result,
            local_path=local_path,
            errors=errors,
            collections_cloned=[],
            total_files_downloaded=0,
        )

    # At least some collections succeeded
    if errors:
        # Partial success
        report(
            "warn",
            f"Cloned {len(collections_cloned)}/{len(collections_to_clone)} collections with errors",
        )
        return CloneResult(
            success=False,  # Partial failure is still failure
            pull_result=last_pull_result,
            local_path=local_path,
            errors=errors,
            collections_cloned=collections_cloned,
            total_files_downloaded=total_files,
        )

    # Full success
    report(
        "success",
        f"Cloned {total_files} file(s) from {len(collections_cloned)} collection(s) to {local_path}",
    )

    return CloneResult(
        success=True,
        pull_result=last_pull_result,
        local_path=local_path,
        errors=[],
        collections_cloned=collections_cloned,
        total_files_downloaded=total_files,
    )
