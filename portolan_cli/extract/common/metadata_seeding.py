"""Common metadata seeding utilities for extraction backends.

This module provides shared functionality for seeding metadata.yaml files
at the collection level from extracted layer metadata.

Used by both WFS and ArcGIS extraction backends to populate collection-level
.portolan/metadata.yaml with layer-specific info (title, description).
"""

from __future__ import annotations

import re
from pathlib import Path


def _is_technical_name(text: str | None) -> bool:
    """Check if text looks like a technical/internal name rather than description.

    Technical names are typically short identifiers that aren't useful as descriptions:
    - Single words with underscores/dashes (e.g., "bu_building_emprise")
    - Very short (under 20 chars) without spaces
    - Prefixed with common tech patterns (e.g., "ns:LayerName")

    Args:
        text: Text to check.

    Returns:
        True if text looks like a technical name.
    """
    if not text:
        return True

    text = text.strip()

    # Very short without spaces = likely technical
    if len(text) < 20 and " " not in text:
        return True

    # Contains namespace prefix (ns:name pattern)
    if re.match(r"^[a-z_]+:[A-Za-z]", text):
        return True

    # All lowercase with underscores, no spaces
    if re.match(r"^[a-z0-9_]+$", text):
        return True

    return False


def _select_best_description(
    abstract: str | None,
    title: str | None,
    layer_name: str,
) -> str | None:
    """Select the best description from available metadata fields.

    Prefers abstract if it's a real description, otherwise falls back to title.
    Returns None if no good description is available.

    Args:
        abstract: Layer abstract/description.
        title: Layer title.
        layer_name: Layer name (used to detect if title/abstract just echoes it).

    Returns:
        Best available description, or None.
    """
    # If abstract looks like a real description, use it
    if abstract and not _is_technical_name(abstract):
        return abstract

    # If title looks like a real description (not just the layer name), use it
    if title and not _is_technical_name(title) and title.lower() != layer_name.lower():
        return title

    # If title is better than abstract (even if both are technical), prefer title
    if title and (not abstract or _is_technical_name(abstract)):
        # Title with spaces/dashes is better than underscore-only abstract
        if " " in title or "-" in title:
            return title

    return abstract


def seed_collection_metadata(
    collection_dir: Path,
    *,
    source_type: str,
    source_url: str,
    layer_name: str,
    title: str | None = None,
    description: str | None = None,
    keywords: list[str] | None = None,
) -> bool:
    """Seed metadata.yaml for a collection with layer-specific info.

    Creates .portolan/metadata.yaml within the collection directory with
    layer-specific metadata (title, description) plus inherited source info.

    Automatically selects the best description from available fields:
    - Prefers abstract if it's a real description (not just a technical name)
    - Falls back to title if abstract looks like an identifier
    - Uses title in processing_notes if different from layer name

    Args:
        collection_dir: Path to the collection directory.
        source_type: Source type identifier (e.g., "wfs", "arcgis_featureserver").
        source_url: URL of the layer/feature source.
        layer_name: Layer name for processing_notes.
        title: Layer title.
        description: Layer description/abstract (will be intelligently selected).
        keywords: Layer-specific keywords.

    Returns:
        True if metadata.yaml was created, False if skipped (already exists).
    """
    from portolan_cli.metadata_extraction import ExtractedMetadata
    from portolan_cli.metadata_seeding import seed_metadata_yaml

    # Select best description from available fields
    best_description = _select_best_description(description, title, layer_name)

    processing_notes = f"Extracted from {source_type} layer: {layer_name}"
    if title and title != layer_name:
        processing_notes = f"{processing_notes} ({title})"

    extracted = ExtractedMetadata(
        source_type=source_type,
        source_url=source_url,
        description=best_description,
        keywords=keywords,
        processing_notes=processing_notes,
    )

    metadata_path = collection_dir / ".portolan" / "metadata.yaml"
    return seed_metadata_yaml(extracted, metadata_path)
