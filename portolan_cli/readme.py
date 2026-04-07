"""README generation from STAC + metadata.yaml (ADR-0038).

This module generates README.md files from STAC metadata and
.portolan/metadata.yaml content. The README is a pure output - always
generated, never hand-edited.

**Sections auto-filled from STAC:**
- Title, description (from catalog/collection)
- Spatial/temporal coverage (from extent)
- Schema/columns (from table:columns)
- Bands (from eo:bands, raster:bands)
- Files with checksums (from assets)
- STAC links (from links)
- Code examples (based on asset types)

**Sections from metadata.yaml (human):**
- License, contact
- Citation, DOI
- Known issues

Usage:
    from portolan_cli.readme import generate_readme, generate_readme_for_collection

    # Generate from dicts
    readme = generate_readme(stac=collection_json, metadata=metadata_yaml)

    # Generate from collection path
    readme = generate_readme_for_collection(collection_path, catalog_root)
"""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any
from urllib.parse import quote

from portolan_cli.config import load_merged_metadata


def _format_size(size_bytes: int) -> str:
    """Format file size in human-readable format."""
    if size_bytes < 1024:
        return f"{size_bytes} B"
    elif size_bytes < 1024 * 1024:
        return f"{size_bytes / 1024:.1f} KB"
    elif size_bytes < 1024 * 1024 * 1024:
        return f"{size_bytes / (1024 * 1024):.1f} MB"
    else:
        return f"{size_bytes / (1024 * 1024 * 1024):.1f} GB"


def _detect_format(assets: dict[str, Any]) -> str | None:
    """Detect primary data format from assets."""
    for asset in assets.values():
        media_type = asset.get("type", "")
        href = asset.get("href", "")

        if "parquet" in media_type or href.endswith(".parquet"):
            return "geoparquet"
        if "geotiff" in media_type or "cloud-optimized" in media_type or href.endswith(".tif"):
            return "cog"
        if "geojson" in media_type or href.endswith(".geojson"):
            return "geojson"
        if "geopackage" in media_type or href.endswith(".gpkg"):
            return "geopackage"

    return None


def _generate_code_example(data_format: str | None, sample_href: str = "data.parquet") -> str:
    """Generate code example based on data format."""
    if data_format == "geoparquet":
        return f'''```python
import geopandas as gpd

gdf = gpd.read_parquet("{sample_href}")
print(gdf.head())
```'''
    elif data_format == "cog":
        return """```python
import rasterio

with rasterio.open("image.tif") as src:
    data = src.read(1)
    print(f"Shape: {data.shape}, CRS: {src.crs}")
```"""
    elif data_format == "geojson":
        return """```python
import geopandas as gpd

gdf = gpd.read_file("data.geojson")
print(gdf.head())
```"""
    elif data_format == "geopackage":
        return """```python
import geopandas as gpd

gdf = gpd.read_file("data.gpkg")
print(gdf.head())
```"""
    else:
        return ""


# =============================================================================
# Section generators - each adds content to sections list
# =============================================================================


def _add_title_section(sections: list[str], stac: dict[str, Any]) -> None:
    """Add title and description from STAC."""
    title = stac.get("title") or stac.get("id", "Untitled Collection")
    sections.append(f"# {title}")
    sections.append("")

    description = stac.get("description", "")
    if description:
        sections.append(str(description).strip())
        sections.append("")


def _add_spatial_section(sections: list[str], stac: dict[str, Any]) -> None:
    """Add spatial coverage from STAC extent."""
    extent = stac.get("extent", {})
    spatial = extent.get("spatial", {})
    bbox_list = spatial.get("bbox", [])

    if not bbox_list:
        return

    bbox = bbox_list[0]
    if len(bbox) < 4:
        return

    sections.append("## Spatial Coverage")
    sections.append("")
    sections.append(f"- **Bounding Box**: [{bbox[0]}, {bbox[1]}, {bbox[2]}, {bbox[3]}]")

    # Add CRS if available
    proj_code = stac.get("summaries", {}).get("proj:code")
    if proj_code:
        if isinstance(proj_code, list):
            proj_code = proj_code[0]
        sections.append(f"- **CRS**: {proj_code}")
    sections.append("")


def _add_temporal_section(sections: list[str], stac: dict[str, Any]) -> None:
    """Add temporal coverage from STAC extent."""
    extent = stac.get("extent", {})
    temporal = extent.get("temporal", {})
    interval_list = temporal.get("interval", [])

    if not interval_list:
        return

    interval = interval_list[0]
    if len(interval) < 2:
        return

    start = interval[0] or "open"
    end = interval[1] or "ongoing"
    sections.append("## Temporal Coverage")
    sections.append("")
    sections.append(f"- **Start**: {start}")
    sections.append(f"- **End**: {end}")
    sections.append("")


def _add_schema_section(sections: list[str], stac: dict[str, Any]) -> None:
    """Add schema/columns from table:columns extension.

    The Table extension writes table:columns directly on the Collection object
    (via add_table_extension), so we check there first. Fall back to summaries
    for backward compatibility with older catalogs.
    """
    # Primary location: Collection extra_fields (per Table extension spec)
    columns = stac.get("table:columns", [])
    # Fallback: legacy summaries location
    if not columns:
        summaries = stac.get("summaries", {})
        columns = summaries.get("table:columns", [])

    if not columns:
        return

    sections.append("## Schema")
    sections.append("")
    sections.append("| Column | Type | Description |")
    sections.append("|--------|------|-------------|")
    for col in columns:
        name = col.get("name", "")
        col_type = col.get("type", "")
        desc = col.get("description", "")
        sections.append(f"| {name} | {col_type} | {desc} |")
    sections.append("")


def _add_bands_section(sections: list[str], stac: dict[str, Any]) -> None:
    """Add bands from eo:bands or raster:bands."""
    summaries = stac.get("summaries", {})
    bands = summaries.get("eo:bands", []) or summaries.get("raster:bands", [])

    if not bands:
        return

    sections.append("## Bands")
    sections.append("")
    sections.append("| Band | Name | Description |")
    sections.append("|------|------|-------------|")
    for i, band in enumerate(bands):
        band_name = band.get("name", f"band_{i + 1}")
        common_name = band.get("common_name", "")
        desc = band.get("description", "")
        sections.append(f"| {i + 1} | {band_name} ({common_name}) | {desc} |")
    sections.append("")


def _add_files_section(sections: list[str], assets: dict[str, Any]) -> None:
    """Add files table from STAC assets."""
    if not assets:
        return

    sections.append("## Files")
    sections.append("")
    sections.append("| File | Size | Checksum |")
    sections.append("|------|------|----------|")
    for key, asset in assets.items():
        href = asset.get("href", key)
        size = asset.get("file:size")
        checksum = asset.get("file:checksum", "")
        size_str = _format_size(size) if size else "-"
        checksum_str = checksum.split(":")[-1][:12] + "..." if checksum else "-"
        sections.append(f"| {href} | {size_str} | {checksum_str} |")
    sections.append("")


def _add_code_example_section(sections: list[str], assets: dict[str, Any]) -> None:
    """Add code example based on detected format."""
    data_format = _detect_format(assets)
    if not data_format:
        return

    sections.append("## Quick Start")
    sections.append("")
    first_href = next((a.get("href", "data") for a in assets.values()), "data.parquet")
    sections.append(_generate_code_example(data_format, first_href))
    sections.append("")


def _add_stac_links_section(sections: list[str], stac: dict[str, Any]) -> None:
    """Add STAC metadata links."""
    links = stac.get("links", [])
    if not links:
        return

    sections.append("## STAC Metadata")
    sections.append("")
    for link in links:
        rel = link.get("rel", "")
        href = link.get("href", "")
        if rel in ("self", "root", "parent", "collection", "items"):
            sections.append(f"- **{rel}**: `{href}`")
    sections.append("")


def _add_source_section(sections: list[str], metadata: dict[str, Any]) -> None:
    """Add source URL from metadata.

    Renders the data source as a clickable link, helping users find
    the original data or verify provenance.
    """
    source_url = metadata.get("source_url")
    if not source_url or not str(source_url).strip():
        return

    sections.append("## Source")
    sections.append("")
    sections.append(f"[{source_url}]({source_url})")
    sections.append("")


def _add_processing_section(sections: list[str], metadata: dict[str, Any]) -> None:
    """Add processing notes from metadata.

    Documents any transformations, cleaning, or modifications
    applied to the original data.
    """
    notes = metadata.get("processing_notes")
    if not notes or not str(notes).strip():
        return

    sections.append("## Processing Notes")
    sections.append("")
    sections.append(str(notes))
    sections.append("")


def _add_authors_section(sections: list[str], metadata: dict[str, Any]) -> None:
    """Add authors section with ORCID links (#316).

    Renders original dataset authors (separate from contact/maintainer).
    Authors with ORCID IDs are rendered as clickable links.
    """
    authors = metadata.get("authors")
    if not authors or not isinstance(authors, list) or len(authors) == 0:
        return

    sections.append("## Authors")
    sections.append("")

    for author in authors:
        if not isinstance(author, dict):
            continue

        name = author.get("name", "")
        orcid = author.get("orcid")
        affiliation = author.get("affiliation")

        # Build author line
        if orcid:
            author_text = f"[{name}](https://orcid.org/{orcid})"
        else:
            author_text = name

        if affiliation:
            author_text = f"{author_text} ({affiliation})"

        sections.append(f"- {author_text}")

    sections.append("")


def _add_version_section(sections: list[str], metadata: dict[str, Any]) -> None:
    """Add upstream version section (#316).

    Renders upstream_version with optional link to upstream_version_url.
    """
    version = metadata.get("upstream_version")
    if not version:
        return

    version_url = metadata.get("upstream_version_url")

    sections.append("## Version")
    sections.append("")

    if version_url:
        sections.append(f"**Upstream Version**: [{version}]({version_url})")
    else:
        sections.append(f"**Upstream Version**: {version}")

    sections.append("")


def _add_keywords_section(sections: list[str], metadata: dict[str, Any]) -> None:
    """Add keywords as shield.io badges.

    Renders keywords as visual badges for quick scanning and
    potential use in search/filtering.
    """
    keywords = metadata.get("keywords")
    if not keywords or not isinstance(keywords, list) or len(keywords) == 0:
        return

    badges = []
    for keyword in keywords:
        # Shield.io badge format requires:
        # - Spaces become underscores (or %20)
        # - Hyphens become double hyphens (--)
        # - Other special chars need URL encoding
        keyword_str = str(keyword)
        # First handle shield.io-specific escaping
        safe_keyword = keyword_str.replace("-", "--")
        # Then URL-encode the rest (safe='' encodes everything except alphanumerics)
        safe_keyword = quote(safe_keyword, safe="")
        # Replace %20 (encoded space) with underscore for better readability
        safe_keyword = safe_keyword.replace("%20", "_")
        badge = f"![{keyword_str}](https://img.shields.io/badge/{safe_keyword}-blue)"
        badges.append(badge)

    sections.append(" ".join(badges))
    sections.append("")


def _add_attribution_section(sections: list[str], metadata: dict[str, Any]) -> None:
    """Add attribution from metadata.

    Credits the data provider or source organization.
    Appears near the footer but before license.
    """
    attribution = metadata.get("attribution")
    if not attribution or not str(attribution).strip():
        return

    sections.append("## Attribution")
    sections.append("")
    sections.append(str(attribution))
    sections.append("")


def _add_citation_section(sections: list[str], metadata: dict[str, Any]) -> None:
    """Add citation and DOI from metadata.

    Supports both single citation (backward compat) and citations list (#316).
    Also supports related_dois in addition to primary doi.
    """
    # Support both single citation (backward compat) and citations list (#316)
    citations: list[str] = []
    if metadata.get("citation"):
        citations.append(str(metadata["citation"]))
    citations.extend(metadata.get("citations", []))

    doi = metadata.get("doi")
    related_dois = metadata.get("related_dois", [])

    if not citations and not doi and not related_dois:
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


def _add_license_section(sections: list[str], metadata: dict[str, Any]) -> None:
    """Add license from metadata."""
    license_id = metadata.get("license")
    if not license_id:
        return

    license_url = metadata.get("license_url")
    sections.append("## License")
    sections.append("")
    if license_url:
        sections.append(f"[{license_id}]({license_url})")
    else:
        sections.append(str(license_id))
    sections.append("")


def _add_contact_section(sections: list[str], metadata: dict[str, Any]) -> None:
    """Add contact from metadata."""
    contact = metadata.get("contact", {})
    if not isinstance(contact, dict):
        return

    name = contact.get("name")
    email = contact.get("email")
    if not name and not email:
        return

    sections.append("## Contact")
    sections.append("")
    if name and email:
        sections.append(f"{name} <{email}>")
    elif name:
        sections.append(str(name))
    elif email:
        sections.append(str(email))
    sections.append("")


def _add_known_issues_section(sections: list[str], metadata: dict[str, Any]) -> None:
    """Add known issues from metadata."""
    known_issues = metadata.get("known_issues")
    if not known_issues:
        return

    sections.append("## Known Issues")
    sections.append("")
    sections.append(str(known_issues))
    sections.append("")


def _add_footer_section(sections: list[str]) -> None:
    """Add Portolan attribution footer."""
    sections.append("---")
    sections.append("")
    sections.append(
        "*Generated by [Portolan](https://github.com/portolan-sdi/portolan-cli) "
        "from STAC metadata and .portolan/metadata.yaml*"
    )
    sections.append("")


# =============================================================================
# Public API
# =============================================================================


def generate_readme(
    stac: dict[str, Any],
    metadata: dict[str, Any],
) -> str:
    """Generate README markdown from STAC and metadata.yaml.

    Combines STAC metadata (machine-extracted) with metadata.yaml (human enrichment)
    into a comprehensive README with columns, code examples, checksums, and links.

    Args:
        stac: STAC Collection/Catalog JSON as dict.
        metadata: Merged metadata.yaml as dict.

    Returns:
        README markdown string.
    """
    sections: list[str] = []

    # Aggregate assets from collection and items
    # Collection-level assets (vector data per ADR-0031)
    assets = dict(stac.get("assets", {}))
    # Item-level assets (raster/temporal data)
    for item in stac.get("items", []):
        item_id = item.get("id", "")
        for asset_key, asset_value in item.get("assets", {}).items():
            # Namespace item assets to avoid collisions: "item_id/asset_key"
            namespaced_key = f"{item_id}/{asset_key}" if item_id else asset_key
            # Only add if not already present (collection-level takes precedence)
            if namespaced_key not in assets and asset_key not in assets:
                assets[namespaced_key] = asset_value

    # STAC-sourced sections
    _add_title_section(sections, stac)
    _add_keywords_section(sections, metadata)  # Visual badges after title
    _add_spatial_section(sections, stac)
    _add_temporal_section(sections, stac)
    _add_schema_section(sections, stac)
    _add_bands_section(sections, stac)
    _add_files_section(sections, assets)
    _add_code_example_section(sections, assets)
    _add_stac_links_section(sections, stac)

    # Metadata-sourced sections (human enrichment)
    _add_source_section(sections, metadata)
    _add_processing_section(sections, metadata)
    _add_version_section(sections, metadata)  # #316: upstream version
    _add_authors_section(sections, metadata)  # #316: authors before citation
    _add_citation_section(sections, metadata)
    _add_attribution_section(sections, metadata)
    _add_license_section(sections, metadata)
    _add_contact_section(sections, metadata)
    _add_known_issues_section(sections, metadata)

    # Footer
    _add_footer_section(sections)

    return "\n".join(sections)


def check_readme_freshness(
    readme_path: Path,
    stac: dict[str, Any],
    metadata: dict[str, Any],
) -> bool:
    """Check if a README file is up-to-date.

    Generates the expected README and compares it to the existing file.

    Args:
        readme_path: Path to the README.md file.
        stac: STAC Collection JSON as dict.
        metadata: Merged metadata.yaml as dict.

    Returns:
        True if README exists and matches generated content, False otherwise.
    """
    if not readme_path.exists():
        return False

    expected = generate_readme(stac=stac, metadata=metadata)
    actual = readme_path.read_text()

    return expected == actual


def generate_readme_for_collection(
    collection_path: Path,
    catalog_root: Path,
) -> str:
    """Generate README for a collection by loading STAC and metadata from disk.

    High-level function that:
    1. Loads collection.json (STAC) from collection_path
    2. Loads merged metadata.yaml from hierarchy
    3. Generates README from both sources

    Args:
        collection_path: Path to the collection directory.
        catalog_root: Path to the catalog root.

    Returns:
        README markdown string.
    """
    # Load STAC collection.json if it exists
    stac: dict[str, Any] = {}
    collection_json_path = collection_path / "collection.json"
    if collection_json_path.exists():
        stac = json.loads(collection_json_path.read_text())

    # Load merged metadata from hierarchy
    metadata = load_merged_metadata(collection_path, catalog_root)

    return generate_readme(stac=stac, metadata=metadata)


def _extract_collection_extent(
    data: dict[str, Any],
) -> tuple[list[float] | None, str | None, str | None]:
    """Extract bbox and temporal extent from a collection dict.

    Returns:
        Tuple of (bbox, temporal_start, temporal_end).
    """
    extent = data.get("extent", {})

    # Extract spatial
    spatial = extent.get("spatial", {})
    bbox_list = spatial.get("bbox", [])
    bbox = bbox_list[0] if bbox_list and len(bbox_list[0]) >= 4 else None

    # Extract temporal
    temporal = extent.get("temporal", {})
    intervals = temporal.get("interval", [])
    start, end = None, None
    if intervals and len(intervals) > 0 and len(intervals[0]) >= 2:
        start = intervals[0][0] if intervals[0][0] else None
        end = intervals[0][1] if intervals[0][1] else None

    return bbox, start, end


def _compute_bbox_envelope(bboxes: list[list[float]]) -> list[float] | None:
    """Compute bounding box envelope (union) from multiple bboxes."""
    if not bboxes:
        return None
    return [
        min(b[0] for b in bboxes),
        min(b[1] for b in bboxes),
        max(b[2] for b in bboxes),
        max(b[3] for b in bboxes),
    ]


def aggregate_catalog_extent(catalog_path: Path) -> dict[str, Any]:
    """Aggregate extent information from all collections in a catalog.

    Computes the bounding box envelope (union) and temporal extent span
    across all child collections.

    Args:
        catalog_path: Path to the catalog root directory.

    Returns:
        Dict with aggregated extent info:
        - bbox: [min_x, min_y, max_x, max_y] or None if no collections
        - temporal_start: Earliest start datetime (ISO string) or None
        - temporal_end: Latest end datetime (ISO string) or None
        - collections: List of collection IDs
    """
    collections: list[str] = []
    bboxes: list[list[float]] = []
    temporal_starts: list[str] = []
    temporal_ends: list[str] = []

    # Find all collection.json files in immediate subdirectories
    for subdir in catalog_path.iterdir():
        if not subdir.is_dir() or subdir.name.startswith("."):
            continue

        collection_json = subdir / "collection.json"
        if not collection_json.exists():
            continue

        try:
            data = json.loads(collection_json.read_text())
        except (json.JSONDecodeError, OSError):
            continue

        collections.append(data.get("id", subdir.name))
        bbox, start, end = _extract_collection_extent(data)

        if bbox:
            bboxes.append(bbox)
        if start:
            temporal_starts.append(start)
        if end:
            temporal_ends.append(end)

    return {
        "bbox": _compute_bbox_envelope(bboxes),
        "temporal_start": min(temporal_starts) if temporal_starts else None,
        "temporal_end": max(temporal_ends) if temporal_ends else None,
        "collections": collections,
    }


def _add_collections_section(
    sections: list[str],
    catalog_path: Path,
    aggregation: dict[str, Any],
) -> None:
    """Add collections listing section for catalog README."""
    collections = aggregation.get("collections", [])
    if not collections:
        return

    sections.append("## Collections")
    sections.append("")

    for coll_id in sorted(collections):
        coll_json = catalog_path / coll_id / "collection.json"
        title = coll_id
        description = ""

        if coll_json.exists():
            try:
                data = json.loads(coll_json.read_text())
                title = data.get("title", coll_id)
                description = data.get("description", "")
            except (json.JSONDecodeError, OSError):
                pass

        # Link to collection directory
        sections.append(f"### [{title}](./{coll_id}/)")
        sections.append("")
        if description:
            # Truncate long descriptions
            if len(description) > 200:
                description = description[:197] + "..."
            sections.append(description)
            sections.append("")


def _add_aggregated_extent_section(
    sections: list[str],
    aggregation: dict[str, Any],
) -> None:
    """Add aggregated spatial/temporal extent section for catalog README."""
    bbox = aggregation.get("bbox")
    temporal_start = aggregation.get("temporal_start")
    temporal_end = aggregation.get("temporal_end")

    if not bbox and not temporal_start and not temporal_end:
        return

    sections.append("## Coverage")
    sections.append("")

    if bbox:
        sections.append("**Spatial Extent**")
        sections.append("")
        sections.append(
            f"- West: {bbox[0]:.4f}, South: {bbox[1]:.4f}, "
            f"East: {bbox[2]:.4f}, North: {bbox[3]:.4f}"
        )
        sections.append("")

    if temporal_start or temporal_end:
        sections.append("**Temporal Extent**")
        sections.append("")
        start_str = temporal_start[:10] if temporal_start else "open"
        end_str = temporal_end[:10] if temporal_end else "open"
        sections.append(f"- {start_str} to {end_str}")
        sections.append("")


def generate_catalog_readme(catalog_path: Path) -> str:
    """Generate README for a catalog with aggregated collection info.

    Creates a catalog-level README that:
    - Shows catalog title and description
    - Lists all collections with links
    - Shows aggregated spatial/temporal extent

    Args:
        catalog_path: Path to the catalog root directory.

    Returns:
        README markdown string.
    """
    sections: list[str] = []

    # Load catalog.json
    catalog_json = catalog_path / "catalog.json"
    catalog: dict[str, Any] = {}
    if catalog_json.exists():
        try:
            catalog = json.loads(catalog_json.read_text())
        except (json.JSONDecodeError, OSError):
            pass

    # Load merged metadata
    metadata = load_merged_metadata(catalog_path, catalog_path)

    # Title and description
    title = catalog.get("title", catalog.get("id", "Data Catalog"))
    description = catalog.get("description", "")

    sections.append(f"# {title}")
    sections.append("")

    # Keywords right after title
    _add_keywords_section(sections, metadata)

    if description:
        sections.append(description)
        sections.append("")

    # Aggregate from collections
    aggregation = aggregate_catalog_extent(catalog_path)

    # Collections listing
    _add_collections_section(sections, catalog_path, aggregation)

    # Aggregated extent
    _add_aggregated_extent_section(sections, aggregation)

    # Metadata sections (from catalog-level metadata.yaml)
    _add_source_section(sections, metadata)
    _add_processing_section(sections, metadata)
    _add_version_section(sections, metadata)  # #316: upstream version
    _add_authors_section(sections, metadata)  # #316: authors before citation
    _add_citation_section(sections, metadata)
    _add_attribution_section(sections, metadata)
    _add_license_section(sections, metadata)
    _add_contact_section(sections, metadata)

    # Footer
    _add_footer_section(sections)

    return "\n".join(sections)
