# Metadata Extractor Abstraction Design

**Date**: 2026-04-07
**Status**: Draft
**Issues**: #312 (Auto-seed metadata.yaml), #316 (Schema enhancements)

## Problem Statement

When `portolan extract arcgis` runs, it generates a rich `extraction-report.json` with service metadata. Currently, users must manually create `metadata.yaml` and copy relevant fields. Additionally, the metadata.yaml schema needs enhancements for richer documentation (multiple authors, citations, DOIs).

The current implementation has ArcGIS-specific metadata extraction tightly coupled to ArcGIS extractors. Future extractors (WFS, WMS, etc.) would need to reinvent the wheel.

## Goals

1. **Auto-seed metadata.yaml** from extraction reports (reduce manual copying)
2. **Enhance schema** for richer documentation (#316 enhancements)
3. **Create abstraction** that future extractors can plug into
4. **Centralize logic** so it doesn't get duplicated or overlooked

## Design

### Architecture

```
┌─────────────────┐     ┌─────────────────┐     ┌─────────────────┐
│ ArcGIS Service  │     │   WFS Server    │     │   WMS Server    │
│   (raw JSON)    │     │   (raw XML)     │     │   (raw XML)     │
└────────┬────────┘     └────────┬────────┘     └────────┬────────┘
         │                       │                       │
         ▼                       ▼                       ▼
┌─────────────────┐     ┌─────────────────┐     ┌─────────────────┐
│ ArcGISMetadata  │     │  WFSMetadata    │     │  WMSMetadata    │
│ (source-specific)│    │ (source-specific)│    │ (source-specific)│
└────────┬────────┘     └────────┬────────┘     └────────┬────────┘
         │                       │                       │
         │  .to_extracted()      │  .to_extracted()      │  .to_extracted()
         │                       │                       │
         └───────────────────────┼───────────────────────┘
                                 ▼
                    ┌────────────────────────┐
                    │   ExtractedMetadata    │
                    │   (canonical shape)    │
                    └───────────┬────────────┘
                                │
                                ▼
                    ┌────────────────────────┐
                    │  seed_metadata_yaml()  │
                    │                        │
                    │  Writes .portolan/     │
                    │  metadata.yaml with    │
                    │  TODO markers          │
                    └───────────┬────────────┘
                                │
                                ▼
                    ┌────────────────────────┐
                    │  Human edits TODOs     │
                    │  (contact.email,       │
                    │   license SPDX)        │
                    └───────────┬────────────┘
                                │
                                ▼
                    ┌────────────────────────┐
                    │  portolan readme       │
                    │                        │
                    │  Generates README.md   │
                    │  from STAC + metadata  │
                    └────────────────────────┘
```

### Module Organization

```
portolan_cli/
├── metadata_yaml.py          # Existing: validation, template, apply_defaults
├── metadata_extraction.py    # NEW: ExtractedMetadata, Author dataclasses
├── metadata_seeding.py       # NEW: seed_metadata_yaml() + YAML generation
├── readme.py                 # Existing: README generation (update for #316)
└── extract/
    └── arcgis/
        ├── metadata.py       # Update: add .to_extracted() method
        ├── report.py         # Existing: FeatureServer report
        └── imageserver/
            └── report.py     # Existing: ImageServer report
```

### Data Model

#### `metadata_extraction.py`

```python
"""Canonical metadata extraction dataclasses.

These dataclasses represent metadata that can be auto-extracted from
any data source. Each extractor produces an ExtractedMetadata instance
that can be used to seed metadata.yaml files.

The ExtractedMetadata fields map directly to metadata.yaml fields.
Human-required fields (contact.email, license SPDX) are NOT included
here - they cannot be auto-extracted and must be filled in manually.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime


@dataclass(frozen=True)
class Author:
    """An author of the original dataset (not the catalog maintainer).

    Used for academic datasets where original creators should be credited
    separately from the catalog maintainer.
    """
    name: str
    email: str | None = None
    orcid: str | None = None  # e.g., "0000-0001-8454-4301"


@dataclass(frozen=True)
class ExtractedMetadata:
    """Canonical metadata extracted from any source for metadata.yaml seeding.

    This dataclass represents the UNION of what any extractor might provide.
    All fields except source_url and source_type are optional - extractors
    populate what they can.

    Human-required fields (contact.email, license SPDX identifier) are NOT
    here - those can't be auto-extracted and must be filled in manually.

    Attributes:
        source_url: The URL of the data source (always populated).
        source_type: Type of source for provenance tracking.
        extraction_date: When extraction occurred (ISO 8601).

        attribution: Copyright/attribution text (e.g., copyrightText).
        keywords: Discovery tags/keywords.
        contact_name: Author name from source (seeds contact.name).
        processing_notes: How data was processed/transformed.
        known_issues: Known limitations or caveats.
        license_hint: Raw license text from source (NOT SPDX).

        authors: Original dataset authors (for academic datasets).
        citations: List of citation strings.
        doi: Primary dataset DOI.
        related_dois: Related DOIs (papers, not the dataset itself).
        upstream_version: Version from upstream source (e.g., "v2").
        upstream_version_url: URL to upstream version info.
    """

    # === ALWAYS POPULATED ===
    source_url: str
    source_type: str  # 'arcgis_featureserver', 'arcgis_imageserver', 'wfs', etc.
    extraction_date: str = field(default_factory=lambda: datetime.now().isoformat())

    # === COMMONLY EXTRACTED ===
    attribution: str | None = None
    keywords: list[str] | None = None
    contact_name: str | None = None
    processing_notes: str | None = None
    known_issues: str | None = None
    license_hint: str | None = None

    # === #316 STRUCTURED ENHANCEMENTS ===
    authors: list[Author] | None = None
    citations: list[str] | None = None
    doi: str | None = None
    related_dois: list[str] | None = None
    upstream_version: str | None = None
    upstream_version_url: str | None = None
```

#### `metadata_seeding.py`

```python
"""Metadata seeding from extracted metadata.

This module generates .portolan/metadata.yaml files from ExtractedMetadata.
It follows the same section-generator pattern as readme.py.
"""

from __future__ import annotations

from pathlib import Path
from typing import Any

from portolan_cli.metadata_extraction import ExtractedMetadata


def seed_metadata_yaml(
    extracted: ExtractedMetadata,
    output_path: Path,
    *,
    overwrite: bool = False,
) -> bool:
    """Seed a metadata.yaml from extracted metadata.

    Creates a metadata.yaml file with:
    - Auto-populated fields from extraction
    - TODO markers for required human fields (contact.email, license)
    - Comments explaining what was auto-filled vs. needs human review

    Args:
        extracted: Metadata extracted from source.
        output_path: Path to write metadata.yaml (typically .portolan/metadata.yaml).
        overwrite: If True, overwrite existing file. If False, skip if exists.

    Returns:
        True if file was written, False if skipped (exists and not overwrite).
    """
    if output_path.exists() and not overwrite:
        return False

    lines: list[str] = []

    _add_header(lines, extracted)
    _add_required_section(lines, extracted)
    _add_discovery_section(lines, extracted)
    _add_lifecycle_section(lines, extracted)
    _add_authors_section(lines, extracted)  # #316
    _add_version_section(lines, extracted)  # #316

    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text("\n".join(lines))
    return True


def _add_header(lines: list[str], extracted: ExtractedMetadata) -> None:
    """Add file header with provenance info."""
    lines.extend([
        "# .portolan/metadata.yaml",
        "#",
        f"# Auto-seeded from {extracted.source_type} extraction ({extracted.extraction_date[:10]})",
        "# Review and complete the TODO fields below.",
        "#",
        "# Human-enrichable metadata that supplements STAC.",
        "# Only contact.email and license are REQUIRED.",
        "",
    ])


def _add_required_section(lines: list[str], extracted: ExtractedMetadata) -> None:
    """Add required fields section with TODO markers."""
    lines.extend([
        "# -----------------------------------------------------------------------------",
        "# REQUIRED: Accountability",
        "# -----------------------------------------------------------------------------",
        "",
        "contact:",
    ])

    if extracted.contact_name:
        lines.append(f'  name: "{extracted.contact_name}"')
    else:
        lines.append('  name: ""  # TODO: Add maintainer name')

    lines.extend([
        '  email: "" # TODO: Required - add contact email',
        "",
        '# TODO: Required - SPDX identifier (e.g., MIT, CC-BY-4.0, CC0-1.0)',
    ])

    if extracted.license_hint:
        # Escape quotes and truncate if too long
        hint = extracted.license_hint.replace('"', '\\"')[:100]
        lines.append(f'# Hint from source: "{hint}"')

    lines.extend([
        'license: ""',
        "",
    ])


def _add_discovery_section(lines: list[str], extracted: ExtractedMetadata) -> None:
    """Add discovery and citation fields."""
    lines.extend([
        "# -----------------------------------------------------------------------------",
        "# OPTIONAL: Discovery and citation",
        "# -----------------------------------------------------------------------------",
        "",
        'license_url: ""',
    ])

    # Citations (list per #316)
    if extracted.citations:
        lines.append("citations:")
        for citation in extracted.citations:
            # Multi-line citations use YAML block scalar
            if "\n" in citation:
                lines.append("  - |")
                for line in citation.split("\n"):
                    lines.append(f"    {line}")
            else:
                lines.append(f'  - "{citation}"')
    else:
        lines.append('citation: ""')

    # DOIs (primary + related per #316)
    if extracted.doi:
        lines.append(f'doi: "{extracted.doi}"')
    else:
        lines.append('doi: ""')

    if extracted.related_dois:
        lines.append("related_dois:")
        for rdoi in extracted.related_dois:
            lines.append(f'  - "{rdoi}"')

    # Keywords
    if extracted.keywords:
        lines.append("keywords:")
        for kw in extracted.keywords:
            lines.append(f"  - {kw}")
    else:
        lines.append("keywords: []")

    # Attribution
    if extracted.attribution:
        lines.append(f'attribution: "{extracted.attribution}"')
    else:
        lines.append('attribution: ""')

    lines.append("")


def _add_lifecycle_section(lines: list[str], extracted: ExtractedMetadata) -> None:
    """Add data lifecycle fields."""
    lines.extend([
        "# -----------------------------------------------------------------------------",
        "# OPTIONAL: Data lifecycle",
        "# -----------------------------------------------------------------------------",
        "",
        f'source_url: "{extracted.source_url}"',
    ])

    if extracted.processing_notes:
        lines.append("processing_notes: |")
        for line in extracted.processing_notes.split("\n"):
            lines.append(f"  {line}")
    else:
        lines.append('processing_notes: ""')

    if extracted.known_issues:
        lines.append("known_issues: |")
        for line in extracted.known_issues.split("\n"):
            lines.append(f"  {line}")
    else:
        lines.append('known_issues: ""')

    lines.append("")


def _add_authors_section(lines: list[str], extracted: ExtractedMetadata) -> None:
    """Add authors section (#316 enhancement)."""
    if not extracted.authors:
        return

    lines.extend([
        "# -----------------------------------------------------------------------------",
        "# OPTIONAL: Original dataset authors (separate from maintainer contact)",
        "# -----------------------------------------------------------------------------",
        "",
        "authors:",
    ])

    for author in extracted.authors:
        lines.append(f'  - name: "{author.name}"')
        if author.email:
            lines.append(f'    email: "{author.email}"')
        if author.orcid:
            lines.append(f'    orcid: "{author.orcid}"')

    lines.append("")


def _add_version_section(lines: list[str], extracted: ExtractedMetadata) -> None:
    """Add upstream version section (#316 enhancement)."""
    if not extracted.upstream_version:
        return

    lines.extend([
        "# -----------------------------------------------------------------------------",
        "# OPTIONAL: Upstream version (for mirrored datasets)",
        "# -----------------------------------------------------------------------------",
        "",
        f'upstream_version: "{extracted.upstream_version}"',
    ])

    if extracted.upstream_version_url:
        lines.append(f'upstream_version_url: "{extracted.upstream_version_url}"')

    lines.append("")
```

### Extractor Integration

#### Update `extract/arcgis/metadata.py`

Add a `.to_extracted()` method to `ArcGISMetadata`:

```python
def to_extracted(self) -> ExtractedMetadata:
    """Convert to canonical ExtractedMetadata.

    Maps ArcGIS-specific fields to the canonical schema.
    """
    from portolan_cli.metadata_extraction import ExtractedMetadata

    return ExtractedMetadata(
        source_url=self.source_url,
        source_type="arcgis_featureserver",
        attribution=self.attribution,
        keywords=self.keywords,
        contact_name=self.contact_name,
        processing_notes=self.processing_notes,
        known_issues=self.known_issues,
        license_hint=self.license_info_raw,
    )
```

#### Update `extract/arcgis/imageserver/report.py`

Add similar method to `ImageServerMetadataExtracted`:

```python
def to_extracted(self) -> ExtractedMetadata:
    """Convert to canonical ExtractedMetadata."""
    from portolan_cli.metadata_extraction import ExtractedMetadata

    # Build processing notes with technical specs
    notes = self.description or ""
    if self.service_description:
        notes = f"{notes}\n\n{self.service_description}" if notes else self.service_description

    specs = f"Technical specs: {self.band_count} bands, {self.pixel_type} pixel type"
    if self.spatial_reference_wkid:
        specs += f", EPSG:{self.spatial_reference_wkid}"
    notes = f"{notes}\n\n{specs}" if notes else specs

    return ExtractedMetadata(
        source_url=self.source_url,
        source_type="arcgis_imageserver",
        attribution=self.copyright_text,
        keywords=[self.service_name.lower()] if self.service_name else None,
        contact_name=self.author,
        processing_notes=notes,
        known_issues=self.access_information,
        license_hint=self.license_info,
    )
```

### README Updates (#316)

Update `readme.py` section generators for new fields:

```python
def _add_citation_section(sections: list[str], metadata: dict[str, Any]) -> None:
    """Add citation and DOI from metadata."""
    # Support both single citation (backward compat) and citations list (#316)
    citations: list[str] = []
    if metadata.get("citation"):
        citations.append(str(metadata["citation"]))
    citations.extend(metadata.get("citations", []))

    doi = metadata.get("doi")
    related_dois = metadata.get("related_dois", [])

    if not citations and not doi:
        return

    sections.append("## Citation")
    sections.append("")

    for citation in citations:
        sections.append(str(citation))
        sections.append("")

    if doi:
        sections.append(f"**DOI**: [{doi}](https://doi.org/{doi})")
        sections.append("")

    if related_dois:
        sections.append("**Related DOIs**:")
        for rdoi in related_dois:
            sections.append(f"- [{rdoi}](https://doi.org/{rdoi})")
        sections.append("")


def _add_authors_section(sections: list[str], metadata: dict[str, Any]) -> None:
    """Add original dataset authors (#316)."""
    authors = metadata.get("authors", [])
    if not authors:
        return

    sections.append("## Authors")
    sections.append("")

    for author in authors:
        if isinstance(author, dict):
            name = author.get("name", "")
            orcid = author.get("orcid")
            if orcid:
                sections.append(f"- {name} ([ORCID](https://orcid.org/{orcid}))")
            else:
                sections.append(f"- {name}")
        else:
            sections.append(f"- {author}")

    sections.append("")


def _add_version_section(sections: list[str], metadata: dict[str, Any]) -> None:
    """Add upstream version info (#316)."""
    version = metadata.get("upstream_version")
    if not version:
        return

    url = metadata.get("upstream_version_url")
    sections.append("## Version")
    sections.append("")
    if url:
        sections.append(f"Upstream version: [{version}]({url})")
    else:
        sections.append(f"Upstream version: {version}")
    sections.append("")
```

### Validation Updates

Update `metadata_yaml.py` to validate new fields:

```python
# Add to OPTIONAL_FIELDS or create new validation
def _validate_authors(authors: list[Any]) -> list[str]:
    """Validate authors list (#316)."""
    errors = []
    for i, author in enumerate(authors):
        if not isinstance(author, dict):
            errors.append(f"authors[{i}] must be a mapping")
            continue
        if "name" not in author:
            errors.append(f"authors[{i}].name is required")
        orcid = author.get("orcid")
        if orcid and not ORCID_PATTERN.match(orcid):
            errors.append(f"Invalid ORCID format: '{orcid}'")
    return errors

def _validate_related_dois(dois: list[Any]) -> list[str]:
    """Validate related_dois list (#316)."""
    errors = []
    for i, doi in enumerate(dois):
        if not isinstance(doi, str):
            errors.append(f"related_dois[{i}] must be a string")
        elif not DOI_PATTERN.match(doi):
            errors.append(f"Invalid DOI format in related_dois: '{doi}'")
    return errors
```

## Implementation Order

### Phase 1: Core Abstraction
1. Create `metadata_extraction.py` with `ExtractedMetadata`, `Author` dataclasses
2. Create `metadata_seeding.py` with `seed_metadata_yaml()`
3. Add tests for both modules

### Phase 2: ArcGIS Integration
1. Add `.to_extracted()` to `ArcGISMetadata`
2. Add `.to_extracted()` to `ImageServerMetadataExtracted`
3. Update extractors to call `seed_metadata_yaml()` after extraction
4. Add integration tests

### Phase 3: Schema Enhancement (#316)
1. Update `metadata_yaml.py` validation for new fields
2. Update `readme.py` section generators
3. Update `generate_metadata_template()` to include new fields
4. Add tests for new validation and README sections

### Phase 4: Documentation
1. Update extract-arcgis guide
2. Document the extractor abstraction for future implementers
3. Add ADR for the design decision

## Testing Strategy

- **Unit tests**: Each section generator in isolation
- **Integration tests**: Full flow from extraction to README
- **Snapshot tests**: Golden file comparison for generated YAML
- **Property tests**: Hypothesis for validation edge cases

## Backward Compatibility

- Single `citation` field still supported (coerced to `citations[0]`)
- Single `doi` field unchanged (primary dataset DOI)
- New fields are all optional
- Existing metadata.yaml files remain valid

## Future Extensibility

To add a new extractor (e.g., WFS):

1. Create `WFSMetadata` dataclass with source-specific fields
2. Implement `.to_extracted() -> ExtractedMetadata`
3. Call `seed_metadata_yaml()` in the extraction flow
4. Done - seeding and README generation work automatically

## References

- [ADR-0038: metadata.yaml as Human Enrichment Layer](../adr/0038-metadata-yaml-enrichment.md)
- [GitHub Issue #312: Auto-seed metadata.yaml](https://github.com/portolan-sdi/portolan-cli/issues/312)
- [GitHub Issue #316: Schema enhancements](https://github.com/portolan-sdi/portolan-cli/issues/316)
