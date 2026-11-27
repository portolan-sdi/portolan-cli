"""Common metadata seeding utilities for extraction backends."""

from __future__ import annotations

from typing import TYPE_CHECKING

from portolan_cli.stac import is_technical_name as _is_technical_name

if TYPE_CHECKING:
    from pathlib import Path


def _select_best_description(
    abstract: str | None,
    title: str | None,
    layer_name: str,
) -> str | None:
    """Select the best description from available metadata fields."""
    # If abstract looks like a real description, use it
    if abstract and not _is_technical_name(abstract):
        return abstract

    # If title looks like a real description (not just the layer name), use it
    if title and not _is_technical_name(title) and title.lower() != layer_name.lower():
        return title

    # If title is better than abstract (even if both are technical), prefer title
    # Title with spaces/dashes is better than underscore-only abstract
    if title and (not abstract or _is_technical_name(abstract)) and (" " in title or "-" in title):
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
    """Seed metadata.yaml for a collection with layer-specific info."""
    from portolan_cli.metadata_extraction import ExtractedMetadata
    from portolan_cli.metadata_seeding import seed_metadata_yaml
    from portolan_cli.stac import update_stac_metadata

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
    # No placeholder: the child file wins the merge, so a TODO would override
    # the catalog's real license and contact.
    seeded = seed_metadata_yaml(extracted, metadata_path, placeholders=False)

    # Also give collection.json a real title and description.
    collection_path = collection_dir / "collection.json"
    update_stac_metadata(
        collection_path,
        title=title,
        description=best_description,
    )

    return seeded
