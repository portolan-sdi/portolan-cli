"""Metadata YAML seeding from extracted sources."""

from __future__ import annotations

import logging
import os
import tempfile
from pathlib import Path
from typing import TYPE_CHECKING, Any

import yaml

from portolan_cli.extract.common.urls import sanitize_published_string
from portolan_cli.providers import PRODUCER_ROLE
from portolan_core.constants import TODO_MARKER
from portolan_core.licensing import OTHER_LICENSE, ResolvedLicense

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
    """Seed a metadata.yaml file from extracted metadata."""
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
    """Build metadata.yaml dictionary from extracted metadata."""
    metadata: dict[str, Any] = {}
    if placeholders:
        metadata["contact"] = {"name": TODO_MARKER, "email": TODO_MARKER}

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

    source_url = extracted.source_url and sanitize_published_string(extracted.source_url)
    if source_url:
        metadata["source_url"] = source_url

    if extracted.description:
        metadata["description"] = extracted.description

    if extracted.attribution:
        metadata["attribution"] = extracted.attribution

    if extracted.keywords:
        # An extractor can put None in the list at runtime. Drop it and make each keyword a string.
        keywords: list[object] = list(extracted.keywords)
        metadata["keywords"] = [str(k) for k in keywords if k is not None]

    if extracted.processing_notes:
        metadata["processing_notes"] = extracted.processing_notes

    if extracted.known_issues:
        metadata["known_issues"] = extracted.known_issues

    # Include raw license info as a comment/note if available
    if extracted.license_raw:
        metadata["_license_info_from_source"] = extracted.license_raw

    return metadata


def _source_providers(extracted: ExtractedMetadata, *, licensor: bool) -> list[dict[str, Any]]:
    """Name the organizations that the source credits for the data."""
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
    """Format metadata dictionary as YAML with header comments."""
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
