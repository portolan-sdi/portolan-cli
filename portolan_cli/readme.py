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
    """Add schema/columns from table:columns extension."""
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


def _add_citation_section(sections: list[str], metadata: dict[str, Any]) -> None:
    """Add citation and DOI from metadata."""
    citation = metadata.get("citation")
    doi = metadata.get("doi")

    if not citation and not doi:
        return

    sections.append("## Citation")
    sections.append("")
    if citation:
        sections.append(str(citation))
        sections.append("")
    if doi:
        sections.append(f"**DOI**: [{doi}](https://doi.org/{doi})")
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
    assets = stac.get("assets", {})

    # STAC-sourced sections
    _add_title_section(sections, stac)
    _add_spatial_section(sections, stac)
    _add_temporal_section(sections, stac)
    _add_schema_section(sections, stac)
    _add_bands_section(sections, stac)
    _add_files_section(sections, assets)
    _add_code_example_section(sections, assets)
    _add_stac_links_section(sections, stac)

    # Metadata-sourced sections
    _add_citation_section(sections, metadata)
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
