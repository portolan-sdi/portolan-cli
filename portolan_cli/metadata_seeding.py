"""Metadata YAML seeding from extracted sources.

This module provides functionality to seed .portolan/metadata.yaml files
from extracted metadata sources (ArcGIS, GDAL, etc.).

The seeding process:
1. Takes extracted metadata from a source
2. Maps it to the metadata.yaml structure
3. Adds TODO markers for required fields that aren't available
4. Writes to disk (respecting overwrite=False by default)

Usage:
    from portolan_cli.metadata_extraction import ExtractedMetadata
    from portolan_cli.metadata_seeding import seed_metadata_yaml

    extracted = ExtractedMetadata(
        source_type="arcgis_featureserver",
        source_url="https://...",
        attribution="City of Philadelphia",
    )

    if seed_metadata_yaml(extracted, Path(".portolan/metadata.yaml")):
        print("Seeded metadata.yaml")
"""

from __future__ import annotations

import logging
import os
import tempfile
from pathlib import Path
from typing import TYPE_CHECKING, Any

import yaml

from portolan_cli.constants import TODO_MARKER
from portolan_cli.extract.common.urls import sanitize_published_string
from portolan_cli.licensing import OTHER_LICENSE, ResolvedLicense
from portolan_cli.providers import PRODUCER_ROLE

if TYPE_CHECKING:
    from portolan_cli.metadata_extraction import ExtractedMetadata

logger = logging.getLogger(__name__)


def seed_metadata_yaml(
    extracted: ExtractedMetadata,
    metadata_path: Path,
    *,
    overwrite: bool = False,
    placeholders: bool = True,
    license_override: ResolvedLicense | None = None,
) -> bool:
    """Seed a metadata.yaml file from extracted metadata.

    Creates a metadata.yaml file with pre-filled values from extraction and
    TODO markers for required fields that need human input.

    Uses atomic write (write to temp file + rename) to prevent race conditions
    when multiple processes attempt to seed the same file.

    Args:
        extracted: Extracted metadata from a data source.
        metadata_path: Path to write metadata.yaml (usually .portolan/metadata.yaml).
        overwrite: If False (default), skip if file already exists.
        placeholders: Whether a TODO placeholder may stand in for a license the
            harvest did not find and for the contact. False for a collection-level
            file, which inherits the catalog's license and contact through the
            hierarchical merge: the child wins, so a placeholder there would
            override the real value the catalog carries (issues #686, #1039). A
            license the harvest did find is written either way, since a layer's
            own value beats an inherited one.
        license_override: License to write instead of anything harvested, from a
            command-line flag. An identifier a human named beats a guess.

    Returns:
        True if file was written, False if skipped (file exists and overwrite=False).
    """
    # Ensure parent directory exists first
    metadata_path.parent.mkdir(parents=True, exist_ok=True)

    # Use exclusive create (O_EXCL) to atomically check and create
    # This prevents TOCTOU race conditions
    if not overwrite:
        try:
            # Atomic "create if not exists" using O_CREAT | O_EXCL
            fd = os.open(metadata_path, os.O_CREAT | os.O_EXCL | os.O_WRONLY)
        except FileExistsError:
            logger.debug(
                "Skipping metadata seeding: %s already exists (overwrite=False)",
                metadata_path,
            )
            return False
        else:
            # File created successfully, close it and proceed to write content
            os.close(fd)

    # Build metadata structure
    metadata = _build_metadata_dict(
        extracted,
        placeholders=placeholders,
        license_override=license_override,
    )

    # Write with header comment using atomic write (temp + rename)
    content = _format_metadata_yaml(metadata, extracted.source_type)

    # Write to temp file in same directory (ensures same filesystem for rename)
    temp_fd, temp_name = tempfile.mkstemp(
        dir=metadata_path.parent,
        prefix=".metadata_yaml_",
        suffix=".tmp",
    )
    temp_path = Path(temp_name)
    try:
        os.write(temp_fd, content.encode("utf-8"))
        os.close(temp_fd)
        # Atomic rename (on POSIX systems)
        temp_path.replace(metadata_path)
    except Exception:
        # Clean up temp file on failure
        os.close(temp_fd) if not os.get_inheritable(temp_fd) else None
        if temp_path.exists():
            temp_path.unlink()
        raise

    logger.debug("Seeded metadata.yaml from %s", extracted.source_type)
    return True


def _build_metadata_dict(
    extracted: ExtractedMetadata,
    *,
    placeholders: bool = True,
    license_override: ResolvedLicense | None = None,
) -> dict[str, Any]:
    """Build metadata.yaml dictionary from extracted metadata.

    Maps extracted fields to metadata.yaml structure, adding TODO markers
    for required fields that aren't available.

    Args:
        extracted: Extracted metadata from a data source.
        placeholders: Whether a TODO placeholder may stand in for a license the
            harvest did not find and for the contact. See ``seed_metadata_yaml``.
        license_override: License to write instead of anything harvested.

    Returns:
        Dictionary ready for YAML serialization.
    """
    metadata: dict[str, Any] = {}
    # The contact names whoever maintains this copy. The source cannot know it,
    # so the seed only asks for it (issue #1039).
    if placeholders:
        metadata["contact"] = {"name": TODO_MARKER, "email": TODO_MARKER}

    # A harvested licence is never an SPDX identifier, but when the source published
    # a licence URL, "other" plus a rel="license" link to it is the second shape the
    # spec accepts, and it states only what the source itself states. Without a URL
    # there is nothing honest to write, so the marker stands and 'add' will ask
    # (issue #686).
    if license_override is not None:
        metadata["license"] = license_override.license_id
        if license_override.license_url:
            metadata["license_url"] = license_override.license_url
    elif extracted.license_url:
        metadata["license"] = OTHER_LICENSE
        metadata["license_url"] = extracted.license_url
    elif placeholders:
        metadata["license"] = TODO_MARKER

    providers = _source_providers(
        extracted, licensor=license_override is None and bool(extracted.license_url)
    )
    if providers:
        metadata["providers"] = providers

    # Optional fields - only include if we have data. The source URL is
    # published, so it carries no credential and no local path (issue #981).
    source_url = extracted.source_url and sanitize_published_string(extracted.source_url)
    if source_url:
        metadata["source_url"] = source_url

    if extracted.description:
        metadata["description"] = extracted.description

    if extracted.attribution:
        metadata["attribution"] = extracted.attribution

    if extracted.keywords:
        # Ensure all keywords are strings (defensive - ExtractedMetadata types this as list[str])
        metadata["keywords"] = [str(k) for k in extracted.keywords if k is not None]

    if extracted.processing_notes:
        metadata["processing_notes"] = extracted.processing_notes

    if extracted.known_issues:
        metadata["known_issues"] = extracted.known_issues

    # Include raw license info as a comment/note if available
    if extracted.license_raw:
        metadata["_license_info_from_source"] = extracted.license_raw

    return metadata


def _source_providers(extracted: ExtractedMetadata, *, licensor: bool) -> list[dict[str, Any]]:
    """Name the organizations that the source credits for the data.

    The source names who made the data in its author and in its attribution.
    Each one is a producer. When the license comes from the source, the rights
    holder is also the licensor. The attribution names the rights holder. A
    source without an attribution leaves the author as the rights holder. None
    of them is the contact or the host. The agency does not maintain a mirror
    of its service.

    Args:
        extracted: Extracted metadata from a data source.
        licensor: Whether the license comes from the source.

    Returns:
        One provider per organization, in the order author, attribution.
    """
    names: dict[str, str] = {}
    for name in (extracted.producer, extracted.attribution):
        if name:
            names.setdefault(name.casefold(), name)
    holder = (extracted.attribution or extracted.producer or "").casefold()
    return [
        {
            "name": name,
            "roles": [PRODUCER_ROLE, "licensor"] if licensor and key == holder else [PRODUCER_ROLE],
        }
        for key, name in names.items()
    ]


def _format_metadata_yaml(metadata: dict[str, Any], source_type: str) -> str:
    """Format metadata dictionary as YAML with header comments.

    Args:
        metadata: Metadata dictionary to format.
        source_type: Source type for the header comment.

    Returns:
        Formatted YAML string with header comments.
    """
    # Build header comment
    header = f"""\
# .portolan/metadata.yaml
#
# Auto-seeded from: {source_type}
# Review and complete fields marked with "TODO".
#
# Required fields:
#   - contact.name: Person or team that maintains this copy, not the source
#   - contact.email: Email address of that person or team
#   - license: SPDX identifier (e.g., "CC-BY-4.0", "MIT", "CC0-1.0")
#
# See: https://portolan.dev/docs/metadata for field descriptions

"""

    # Serialize metadata to YAML
    yaml_content = yaml.dump(
        metadata,
        default_flow_style=False,
        allow_unicode=True,
        sort_keys=False,
    )

    return header + yaml_content
