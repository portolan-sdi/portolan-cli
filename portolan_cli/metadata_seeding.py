"""Metadata seeding infrastructure for extraction workflows.

This module provides a common interface for seeding metadata.yaml files
from extraction reports. It defines a unified ExtractedMetadata dataclass
that both FeatureServer and ImageServer extraction can convert to.

The seeding process:
1. Extraction completes and builds a report with service-specific metadata
2. Service-specific metadata (e.g., ImageServerMetadataExtracted) calls .to_extracted()
   to convert to the common ExtractedMetadata format
3. seed_metadata_yaml() writes the metadata.yaml file (if it doesn't exist)

Per ADR-0038, metadata.yaml contains human-enrichable fields. Seeding provides
a starting point with auto-extractable values, but NEVER overwrites existing
files (preserving human edits).

Usage:
    from portolan_cli.metadata_seeding import ExtractedMetadata, seed_metadata_yaml

    # From ImageServer extraction
    extracted = report.metadata_extracted.to_extracted()
    seed_metadata_yaml(extracted, output_dir / ".portolan" / "metadata.yaml")

    # From FeatureServer extraction
    extracted = report.metadata_extracted.to_extracted()
    seed_metadata_yaml(extracted, output_dir / ".portolan" / "metadata.yaml")
"""

from __future__ import annotations

import logging
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import yaml

logger = logging.getLogger(__name__)


@dataclass
class ExtractedMetadata:
    """Common metadata interface for seeding metadata.yaml.

    This dataclass provides a unified representation of metadata extracted
    from various sources (FeatureServer, ImageServer, etc.) for seeding
    the metadata.yaml file.

    All fields are optional except source_url and source_type. Fields with
    None values are omitted from the seeded YAML.

    Attributes:
        source_url: The URL of the source service (required).
        source_type: Type of source ("featureserver", "imageserver", etc.).
        description: Service description.
        attribution: Copyright/attribution text.
        keywords: List of keywords/tags.
        contact_name: Author/contact name.
        processing_notes: Additional context about how data was processed.
        known_issues: Access restrictions or known caveats.
        license_info_raw: Raw license text (not SPDX-mapped).
    """

    source_url: str
    source_type: str
    description: str | None = None
    attribution: str | None = None
    keywords: list[str] | None = None
    contact_name: str | None = None
    processing_notes: str | None = None
    known_issues: str | None = None
    license_info_raw: str | None = None


def seed_metadata_yaml(
    extracted: ExtractedMetadata,
    metadata_path: Path,
) -> bool:
    """Seed a metadata.yaml file with extracted service metadata.

    Creates a new metadata.yaml file at the given path with values
    from the ExtractedMetadata. This function NEVER overwrites existing
    files to preserve human edits.

    The seeded file includes:
    - source_url: Where the data came from
    - processing_notes: Technical details and service description
    - attribution: Copyright text from service
    - keywords: Tags from service metadata
    - Placeholder structure for required fields (contact, license)

    Args:
        extracted: Common metadata extracted from service.
        metadata_path: Path to write metadata.yaml (typically .portolan/metadata.yaml).

    Returns:
        True if file was created, False if file already exists.
    """
    # Never overwrite existing metadata.yaml
    if metadata_path.exists():
        logger.debug(
            "Skipping metadata seeding: %s already exists",
            metadata_path,
        )
        return False

    # Build the YAML content
    content = _build_seeded_content(extracted)

    # Ensure parent directory exists
    metadata_path.parent.mkdir(parents=True, exist_ok=True)

    # Write YAML with readable formatting
    yaml_content = _format_yaml_with_comments(content, extracted)
    metadata_path.write_text(yaml_content)

    logger.info("Seeded metadata.yaml from %s", extracted.source_type)
    return True


def _build_seeded_content(extracted: ExtractedMetadata) -> dict[str, Any]:
    """Build the content dict for metadata.yaml.

    Args:
        extracted: Common metadata extracted from service.

    Returns:
        Dictionary ready for YAML serialization.
    """
    content: dict[str, Any] = {}

    # Always include source_url
    content["source_url"] = extracted.source_url

    # Include attribution if present
    if extracted.attribution:
        content["attribution"] = extracted.attribution

    # Include keywords if present
    if extracted.keywords:
        content["keywords"] = extracted.keywords

    # Include processing_notes if present
    if extracted.processing_notes:
        content["processing_notes"] = extracted.processing_notes

    # Include known_issues if present
    if extracted.known_issues:
        content["known_issues"] = extracted.known_issues

    # Include placeholder for contact (required per ADR-0038)
    # Pre-populate with author if available
    contact: dict[str, str] = {
        "name": extracted.contact_name or "",
        "email": "",
    }
    content["contact"] = contact

    # Include placeholder for license (required per ADR-0038)
    # Note: license_info_raw is not SPDX-compliant, so we leave blank
    # but include a comment to guide the user
    content["license"] = ""

    return content


def _format_yaml_with_comments(
    content: dict[str, Any],
    extracted: ExtractedMetadata,
) -> str:
    """Format the content as YAML with helpful comments.

    Args:
        content: The content dictionary.
        extracted: Original extracted metadata (for context).

    Returns:
        YAML string with comments.
    """
    # Build YAML with comments
    lines = [
        "# Seeded from extraction - edit as needed",
        f"# Source type: {extracted.source_type}",
        "",
    ]

    # Dump the content
    yaml_str = yaml.dump(
        content,
        default_flow_style=False,
        allow_unicode=True,
        sort_keys=False,
    )

    lines.append(yaml_str)

    # Add note about required fields
    lines.append("")
    lines.append("# REQUIRED: contact.name, contact.email, and license must be filled in")
    if extracted.license_info_raw:
        lines.append(f"# Original license text from service: {extracted.license_info_raw[:100]}...")

    return "\n".join(lines)
