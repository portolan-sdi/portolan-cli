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

if TYPE_CHECKING:
    from portolan_cli.metadata_extraction import ExtractedMetadata

logger = logging.getLogger(__name__)

# TODO marker for fields that need human input
TODO_MARKER = "TODO: Add value"


def seed_metadata_yaml(
    extracted: ExtractedMetadata,
    metadata_path: Path,
    *,
    overwrite: bool = False,
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
    metadata = _build_metadata_dict(extracted)

    # Write with header comment using atomic write (temp + rename)
    content = _format_metadata_yaml(metadata, extracted.source_type)

    # Write to temp file in same directory (ensures same filesystem for rename)
    temp_fd, temp_path = tempfile.mkstemp(
        dir=metadata_path.parent,
        prefix=".metadata_yaml_",
        suffix=".tmp",
    )
    try:
        os.write(temp_fd, content.encode("utf-8"))
        os.close(temp_fd)
        # Atomic rename (on POSIX systems)
        os.replace(temp_path, metadata_path)
    except Exception:
        # Clean up temp file on failure
        os.close(temp_fd) if not os.get_inheritable(temp_fd) else None
        if os.path.exists(temp_path):
            os.unlink(temp_path)
        raise

    logger.debug("Seeded metadata.yaml from %s", extracted.source_type)
    return True


def _build_metadata_dict(extracted: ExtractedMetadata) -> dict[str, Any]:
    """Build metadata.yaml dictionary from extracted metadata.

    Maps extracted fields to metadata.yaml structure, adding TODO markers
    for required fields that aren't available.

    Args:
        extracted: Extracted metadata from a data source.

    Returns:
        Dictionary ready for YAML serialization.
    """
    # Build contact section
    contact: dict[str, str] = {
        "name": extracted.contact_name or TODO_MARKER,
        "email": extracted.contact_email or TODO_MARKER,
    }

    # Build main metadata structure
    metadata: dict[str, Any] = {
        # Required fields
        "contact": contact,
        "license": TODO_MARKER,  # Raw license isn't SPDX, always needs human review
    }

    # Optional fields - only include if we have data
    if extracted.source_url:
        metadata["source_url"] = extracted.source_url

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
#   - contact.name: Person or team responsible
#   - contact.email: Contact email address
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
