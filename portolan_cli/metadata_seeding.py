"""Metadata YAML seeding from extracted metadata (Wave 1).

This module generates .portolan/metadata.yaml files from ExtractedMetadata,
providing a pre-filled starting point for human enrichment. The generated
YAML follows the schema from ADR-0038 and includes TODO markers for
required fields that couldn't be extracted.

Addresses:
- #312: Unified metadata extraction framework
- #316: Author and citation support

Design follows the section-generator pattern from readme.py:
- Each _add_*_section() function builds YAML lines
- Sections are assembled in order with clear separators
- Comments provide context and guidance

Usage:
    from portolan_cli.metadata_extraction import ExtractedMetadata
    from portolan_cli.metadata_seeding import seed_metadata_yaml

    extracted = ExtractedMetadata(
        source_url="https://example.com/api",
        source_type="arcgis",
        extraction_date=date.today(),
        attribution="City of Philadelphia",
    )

    seed_metadata_yaml(extracted, Path(".portolan/metadata.yaml"))
"""

from __future__ import annotations

from pathlib import Path
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from portolan_cli.metadata_extraction import ExtractedMetadata


def seed_metadata_yaml(
    extracted: ExtractedMetadata,
    output_path: Path,
    *,
    overwrite: bool = False,
) -> bool:
    """Generate a metadata.yaml file from extracted metadata.

    Creates a YAML file pre-filled with extracted values and TODO markers
    for required fields (contact.email, license) that need human input.

    Args:
        extracted: ExtractedMetadata instance with extracted values.
        output_path: Path where metadata.yaml should be written.
        overwrite: If True, overwrite existing file. Default False.

    Returns:
        True if file was written, False if file exists and overwrite=False.
    """
    if output_path.exists() and not overwrite:
        return False

    lines: list[str] = []

    # Build sections in order
    _add_header(lines, extracted)
    _add_required_section(lines, extracted)
    _add_discovery_section(lines, extracted)
    _add_lifecycle_section(lines, extracted)
    _add_authors_section(lines, extracted)
    _add_version_section(lines, extracted)

    # Ensure parent directory exists
    output_path.parent.mkdir(parents=True, exist_ok=True)

    # Write content
    output_path.write_text("\n".join(lines))

    return True


# =============================================================================
# Section generators - each adds YAML content to lines list
# =============================================================================


def _add_header(lines: list[str], extracted: ExtractedMetadata) -> None:
    """Add provenance header comment.

    Documents where the data came from and when extraction occurred.
    """
    lines.append("# .portolan/metadata.yaml")
    lines.append("#")
    lines.append(f"# Auto-generated from {extracted.source_type} extraction")
    lines.append(f"# Source: {extracted.source_url}")
    lines.append(f"# Date: {extracted.extraction_date.isoformat()}")
    lines.append("#")
    lines.append("# Review and complete the required fields below.")
    lines.append("# See https://portolan.dev/docs/metadata for field documentation.")
    lines.append("")


def _add_required_section(lines: list[str], extracted: ExtractedMetadata) -> None:
    """Add required section with contact and license.

    Includes TODO markers for fields that need human input.
    """
    lines.append("# -----------------------------------------------------------------------------")
    lines.append("# REQUIRED: Accountability")
    lines.append("# -----------------------------------------------------------------------------")
    lines.append("")
    lines.append("contact:")

    # contact.name - populate if extracted, otherwise TODO
    if extracted.contact_name:
        lines.append(f'  name: "{extracted.contact_name}"')
    else:
        lines.append('  name: ""  # TODO: Person or team name')

    # contact.email - always needs human input
    lines.append('  email: ""  # TODO: Contact email')
    lines.append("")

    # license - populate hint if available, always needs SPDX identifier
    if extracted.license_hint:
        lines.append(f"# Extracted license info: {extracted.license_hint}")
        lines.append('license: ""  # TODO: SPDX identifier (e.g., "CC-BY-4.0", "MIT")')
    else:
        lines.append('license: ""  # TODO: SPDX identifier (e.g., "CC-BY-4.0", "MIT")')

    lines.append("")


def _add_discovery_section(lines: list[str], extracted: ExtractedMetadata) -> None:
    """Add discovery section with citations, DOIs, keywords, attribution.

    Only includes fields that have values or are commonly used.
    """
    # Check if we have any discovery fields to add
    has_discovery = any(
        [
            extracted.citations,
            extracted.doi,
            extracted.related_dois,
            extracted.keywords,
            extracted.attribution,
        ]
    )

    if not has_discovery:
        return

    lines.append("# -----------------------------------------------------------------------------")
    lines.append("# OPTIONAL: Discovery and citation")
    lines.append("# -----------------------------------------------------------------------------")
    lines.append("")

    # Citations
    if extracted.citations:
        lines.append("citations:")
        for citation in extracted.citations:
            # Escape quotes in citation text
            escaped = citation.replace('"', '\\"')
            lines.append(f'  - "{escaped}"')
        lines.append("")

    # DOI
    if extracted.doi:
        lines.append(f'doi: "{extracted.doi}"')
        lines.append("")

    # Related DOIs
    if extracted.related_dois:
        lines.append("related_dois:")
        for related_doi in extracted.related_dois:
            lines.append(f'  - "{related_doi}"')
        lines.append("")

    # Keywords
    if extracted.keywords:
        lines.append("keywords:")
        for keyword in extracted.keywords:
            lines.append(f'  - "{keyword}"')
        lines.append("")

    # Attribution
    if extracted.attribution:
        # Use block scalar for potentially long attribution text
        if "\n" in extracted.attribution:
            lines.append("attribution: |")
            for line in extracted.attribution.split("\n"):
                lines.append(f"  {line}")
        else:
            escaped = extracted.attribution.replace('"', '\\"')
            lines.append(f'attribution: "{escaped}"')
        lines.append("")


def _add_lifecycle_section(lines: list[str], extracted: ExtractedMetadata) -> None:
    """Add lifecycle section with source_url, processing_notes, known_issues.

    Documents data provenance and quality information.
    """
    # Check if we have any lifecycle fields
    has_lifecycle = any(
        [
            extracted.source_url,
            extracted.processing_notes,
            extracted.known_issues,
        ]
    )

    if not has_lifecycle:
        return

    lines.append("# -----------------------------------------------------------------------------")
    lines.append("# OPTIONAL: Data lifecycle")
    lines.append("# -----------------------------------------------------------------------------")
    lines.append("")

    # Source URL (always present in ExtractedMetadata)
    lines.append(f'source_url: "{extracted.source_url}"')
    lines.append("")

    # Processing notes
    if extracted.processing_notes:
        if "\n" in extracted.processing_notes:
            lines.append("processing_notes: |")
            for line in extracted.processing_notes.split("\n"):
                lines.append(f"  {line}")
        else:
            escaped = extracted.processing_notes.replace('"', '\\"')
            lines.append(f'processing_notes: "{escaped}"')
        lines.append("")

    # Known issues
    if extracted.known_issues:
        if "\n" in extracted.known_issues:
            lines.append("known_issues: |")
            for line in extracted.known_issues.split("\n"):
                lines.append(f"  {line}")
        else:
            escaped = extracted.known_issues.replace('"', '\\"')
            lines.append(f'known_issues: "{escaped}"')
        lines.append("")


def _add_authors_section(lines: list[str], extracted: ExtractedMetadata) -> None:
    """Add authors section for academic-style attribution (#316).

    Formats author information with ORCID support.
    """
    if not extracted.has_authors():
        return

    lines.append("# -----------------------------------------------------------------------------")
    lines.append("# OPTIONAL: Authors (#316)")
    lines.append("# -----------------------------------------------------------------------------")
    lines.append("")

    lines.append("authors:")
    for author in extracted.authors:  # type: ignore[union-attr]
        # Build author dict with only non-None fields
        author_dict: dict[str, str] = {"name": author.name}
        if author.email:
            author_dict["email"] = author.email
        if author.orcid:
            author_dict["orcid"] = author.orcid

        # Format as YAML inline if simple, block if complex
        if len(author_dict) == 1:
            lines.append(f'  - name: "{author.name}"')
        else:
            lines.append(f'  - name: "{author.name}"')
            if author.email:
                lines.append(f'    email: "{author.email}"')
            if author.orcid:
                lines.append(f'    orcid: "{author.orcid}"')

    lines.append("")


def _add_version_section(lines: list[str], extracted: ExtractedMetadata) -> None:
    """Add upstream version section (#316).

    Documents source data version for tracking updates.
    """
    has_version = extracted.upstream_version or extracted.upstream_version_url

    if not has_version:
        return

    lines.append("# -----------------------------------------------------------------------------")
    lines.append("# OPTIONAL: Upstream version (#316)")
    lines.append("# -----------------------------------------------------------------------------")
    lines.append("")

    if extracted.upstream_version:
        lines.append(f'upstream_version: "{extracted.upstream_version}"')

    if extracted.upstream_version_url:
        lines.append(f'upstream_version_url: "{extracted.upstream_version_url}"')

    lines.append("")
