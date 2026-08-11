"""README generation from STAC + metadata.yaml.

This module generates README.md files from STAC metadata and
.portolan/metadata.yaml content. The README is a pure output - always
generated, never hand-edited.

**Sections auto-filled from STAC:**
- Title, description (metadata.yaml override > catalog/collection STAC > id, #534)
- Spatial/temporal coverage (from extent)
- Schema/columns (from table:columns)
- Bands (from the unified bands array on the data asset)
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
import re
from pathlib import Path, PurePosixPath
from typing import Any
from urllib.parse import quote

from portolan_cli.agents_md import markdown_link_gap
from portolan_cli.agents_md import visible_stac_files as _visible_stac_files
from portolan_cli.config import load_merged_metadata
from portolan_cli.errors import ConfigInvalidStructureError
from portolan_cli.json_io import write_json_atomic
from portolan_cli.stac_parquet import owned_item_hrefs

# Keyword-badge rendering limits (#515). A junk-dominated list is a machine dump
# (e.g. WFS layer ids seeded into metadata.yaml at extraction) and is suppressed;
# an otherwise-curated list is filtered then truncated to a readable count.
MAX_KEYWORD_BADGES = 12  # cap on rendered badges (curated lists are truncated)
TECHNICAL_KEYWORD_RATIO = 0.6  # >60% technical entries -> non-curated dump -> omit


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


def _metadata_override(metadata: dict[str, Any], field: str) -> str | None:
    """Return a non-blank human override for ``field`` from metadata.yaml (#534).

    ``metadata.yaml`` may carry optional ``title``/``description`` keys as the
    highest-precedence human override (Issue #502, mirrored by
    :func:`portolan_cli.stac.apply_human_titles`). A blank/whitespace value is
    treated as "not provided" so the auto-derived STAC value is used instead.
    """
    if not isinstance(metadata, dict):
        return None
    value = metadata.get(field)
    if isinstance(value, str) and value.strip():
        return value.strip()
    return None


def _resolve_title_description(
    stac: dict[str, Any],
    metadata: dict[str, Any],
) -> tuple[str, str]:
    """Resolve title/description with metadata.yaml override precedence (#534).

    Precedence: metadata.yaml human override > STAC value > humanized id.
    """
    # `or` chains (not stac.get defaults) so an explicit null in STAC
    # (e.g. {"id": null} or {"description": null}) falls through to the literal
    # fallback instead of becoming the string "None".
    title = (
        _metadata_override(metadata, "title")
        or stac.get("title")
        or stac.get("id")
        or "Untitled Collection"
    )
    description = _metadata_override(metadata, "description") or stac.get("description") or ""
    return str(title), str(description)


def _add_title_section(
    sections: list[str],
    stac: dict[str, Any],
    metadata: dict[str, Any],
) -> None:
    """Add title and description, preferring metadata.yaml overrides (#534)."""
    title, description = _resolve_title_description(stac, metadata)
    sections.append(f"# {title}")
    sections.append("")

    if description:
        sections.append(description.strip())
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

    from portolan_cli.bbox import to_2d_bbox

    # Reduce to 2D so a 6-element extent shows [west, south, east, north], not
    # its [west, south, min_z, east] slice (issue #592).
    west, south, east, north = to_2d_bbox(bbox)

    sections.append("## Spatial Coverage")
    sections.append("")
    sections.append(f"- **Bounding Box**: [{west}, {south}, {east}, {north}]")

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


# Statistic keys live inside a band's `statistics` object (STAC v1.1.0
# Statistics Object), not on the band itself.
_BAND_STAT_KEYS = frozenset({"minimum", "maximum", "mean", "stddev", "valid_percent"})

# Bands table columns in render order, as (header, band key). A column is
# dropped when no band carries a value for it, so a bands array without
# statistics renders as a narrow identity table.
_BAND_COLUMNS: tuple[tuple[str, str], ...] = (
    ("Name", "name"),
    ("Common Name", "eo:common_name"),
    ("Data Type", "data_type"),
    ("Unit", "unit"),
    ("Nodata", "nodata"),
    ("Min", "minimum"),
    ("Max", "maximum"),
    ("Mean", "mean"),
    ("Std Dev", "stddev"),
    ("Valid %", "valid_percent"),
    ("Description", "description"),
)


def _band_field(band: dict[str, Any], key: str) -> Any:
    """Read one band field, reaching into ``statistics`` for statistic keys."""
    if key in _BAND_STAT_KEYS:
        statistics = band.get("statistics")
        return statistics.get(key) if isinstance(statistics, dict) else None
    if key == "eo:common_name":
        # STAC v1.1.0 renamed eo:bands' common_name; read both.
        return band.get("eo:common_name", band.get("common_name"))
    return band.get(key)


def _find_bands(assets: dict[str, Any], stac: dict[str, Any]) -> list[dict[str, Any]]:
    """Find the unified bands array, preferring the primary data asset.

    STAC v1.1.0 makes ``bands`` an asset-level field, and the CLI writes it
    there (``stac._set_bands_on_data_assets``), item-level for rasters. The
    ``eo:bands`` / ``raster:bands`` summaries are read only as a fallback, for
    catalogs hand-authored before the v1.1.0 migration (issue #713).
    """
    banded = [
        asset
        for asset in assets.values()
        if isinstance(asset, dict) and isinstance(asset.get("bands"), list) and asset["bands"]
    ]
    preferred = next((a for a in banded if "data" in (a.get("roles") or [])), None)
    if preferred is None and banded:
        preferred = banded[0]
    if preferred is not None:
        bands: list[Any] = preferred["bands"]
    else:
        summaries = stac.get("summaries", {})
        bands = summaries.get("eo:bands", []) or summaries.get("raster:bands", [])
    return [band for band in bands if isinstance(band, dict)]


def _add_bands_section(sections: list[str], assets: dict[str, Any], stac: dict[str, Any]) -> None:
    """Add the bands table from the unified bands array on the data asset."""
    bands = _find_bands(assets, stac)
    if not bands:
        return

    columns = [
        (header, key)
        for header, key in _BAND_COLUMNS
        if any(_band_field(band, key) is not None for band in bands)
    ]
    headers = ["Band", *(header for header, _ in columns)]

    sections.append("## Bands")
    sections.append("")
    sections.append(f"| {' | '.join(headers)} |")
    sections.append(f"|{'|'.join('-' * max(len(h) + 2, 6) for h in headers)}|")
    for index, band in enumerate(bands, start=1):
        values = [str(index)]
        for _, key in columns:
            value = _band_field(band, key)
            values.append("-" if value is None else str(value))
        sections.append(f"| {' | '.join(values)} |")
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

    Renders original data authors (separate from contact/maintainer).
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


def _is_meaningful_keyword(keyword: str) -> bool:
    """Return True if a keyword is a meaningful discovery term, not a slug (#515).

    Filters out the technical junk that extraction seeds verbatim into
    ``metadata.yaml`` (WFS layer ids, FACC codes, STAC summary values). The junk
    is characterised by structure, not case: lowercase common nouns (``census``,
    ``roads``) are valid keywords and must be kept, so this deliberately does
    *not* reuse :func:`is_technical_name`, which is title-oriented and treats
    any lowercase single word as a slug.

    Dropped: anything with an underscore (``vial_nacional``,
    ``lineas_de_geomorfologia_CA010``), a colon (``orden:30``, ``ns:Name``), or a
    short letters-then-digits code (``AP010``, ``DB120``). Kept: ``census``,
    ``land use``, ``Provincia``, ``COVID19``.
    """
    text = keyword.strip()
    if not text:
        return False
    # snake_case slugs and WFS layer ids.
    if "_" in text:
        return False
    # field:value summary entries (orden:30) and namespace prefixes (ns:Name).
    if ":" in text:
        return False
    # Short alphanumeric codes (FACC: AP010, DB120, BH020, CA010).
    if len(text) <= 8 and re.fullmatch(r"[A-Za-z]{1,3}\d{1,4}[A-Za-z0-9]*", text):
        return False
    return True


def _add_keywords_section(sections: list[str], metadata: dict[str, Any]) -> None:
    """Add curated keywords as shield.io badges.

    Renders keywords as visual badges for quick scanning. Technical junk is
    filtered out; a junk-dominated list (a machine dump rather than curated
    keywords) is omitted entirely so it cannot bury the description, and an
    otherwise-curated list is truncated to a readable count (#515).
    """
    keywords = metadata.get("keywords")
    if not keywords or not isinstance(keywords, list):
        return
    kws = [str(k).strip() for k in keywords if str(k).strip()]
    if not kws:
        return

    meaningful = [k for k in kws if _is_meaningful_keyword(k)]
    if not meaningful:
        return

    # A junk-dominated list is a machine dump, not curation -> omit. The
    # proportion (not the length) is the signal: it tells a real curated list
    # from one where a few clean tokens survived by coincidence.
    technical_ratio = (len(kws) - len(meaningful)) / len(kws)
    if technical_ratio > TECHNICAL_KEYWORD_RATIO:
        return

    badges = []
    for keyword_str in meaningful[:MAX_KEYWORD_BADGES]:
        # Shield.io badge format requires:
        # - Spaces become underscores (or %20)
        # - Hyphens become double hyphens (--)
        # - Other special chars need URL encoding
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
    # Collection-level assets (vector data)
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

    # STAC-sourced sections (title/description honor metadata.yaml overrides, #534)
    _add_title_section(sections, stac, metadata)
    _add_keywords_section(sections, metadata)  # Visual badges after title
    _add_spatial_section(sections, stac)
    _add_temporal_section(sections, stac)
    _add_schema_section(sections, stac)
    _add_bands_section(sections, assets, stac)
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
    actual = readme_path.read_text(encoding="utf-8")

    return expected == actual


def _collection_relative_href(href: str, item_dir: str) -> str:
    """Rebase an item-relative asset href onto the collection directory.

    Item assets are written relative to the item JSON (``./scene-a.tif``), but
    the README sits at the collection root, so the item directory has to be
    prepended (``scene-a/scene-a.tif``). URLs and absolute paths already
    resolve on their own and pass through untouched.
    """
    if not href or "://" in href or href.startswith("/"):
        return href
    cleaned = href[2:] if href.startswith("./") else href
    if not item_dir or item_dir == ".":
        return cleaned
    return f"{item_dir}/{cleaned}"


def _read_item(item_path: Path) -> dict[str, Any] | None:
    """Parse an item JSON, returning None when it is missing or unreadable.

    A stale ``rel="item"`` link is reported by ``portolan check``, so README
    generation skips the item rather than failing the whole catalog.
    """
    try:
        data = json.loads(item_path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return None
    return data if isinstance(data, dict) else None


def _rebase_item_assets(item: dict[str, Any], item_dir: str) -> dict[str, Any]:
    """Copy an item with its asset hrefs rewritten relative to the collection."""
    assets = item.get("assets")
    if not isinstance(assets, dict):
        return item

    rebased: dict[str, Any] = {}
    for key, asset in assets.items():
        if isinstance(asset, dict) and isinstance(asset.get("href"), str):
            asset = {**asset, "href": _collection_relative_href(asset["href"], item_dir)}
        rebased[key] = asset
    return {**item, "assets": rebased}


def load_collection_stac(collection_path: Path) -> dict[str, Any]:
    """Load ``collection.json`` with the collection's items attached.

    STAC keeps items in sibling files behind ``rel="item"`` links, but the
    README renders asset-level metadata that exists only there: the unified
    ``bands`` array (issue #713) plus each data file's size and checksum. The
    items are attached under ``items`` so ``generate_readme`` walks one dict.

    Args:
        collection_path: Path to the collection directory.

    Returns:
        The collection dict, empty when there is no collection.json.
    """
    collection_json_path = collection_path / "collection.json"
    if not collection_json_path.exists():
        return {}

    stac: dict[str, Any] = json.loads(collection_json_path.read_text(encoding="utf-8"))

    items: list[dict[str, Any]] = []
    for href, item_path in owned_item_hrefs(collection_json_path):
        item = _read_item(item_path)
        if item is not None:
            items.append(_rebase_item_assets(item, str(PurePosixPath(href).parent)))
    if items:
        stac["items"] = items

    return stac


def generate_readme_for_collection(
    collection_path: Path,
    catalog_root: Path,
) -> str:
    """Generate README for a collection by loading STAC and metadata from disk.

    High-level function that:
    1. Loads collection.json (STAC) and its items from collection_path
    2. Loads merged metadata.yaml from hierarchy
    3. Generates README from both sources

    Args:
        collection_path: Path to the collection directory.
        catalog_root: Path to the catalog root.

    Returns:
        README markdown string.
    """
    stac = load_collection_stac(collection_path)

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
    """Compute bounding box envelope (union) from multiple bboxes.

    Filters out invalid bboxes (inf/nan/out-of-range) with warnings (issue #516).
    """
    from portolan_cli.bbox import compute_bbox_union

    if not bboxes:
        return None

    result = compute_bbox_union(bboxes)
    return result.bbox  # None if all invalid


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
            data = json.loads(collection_json.read_text(encoding="utf-8"))
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


# Default threshold for making collections list collapsible (#424)
# Catalogs with >= this many collections will use <details> tags
COLLAPSIBLE_COLLECTIONS_THRESHOLD = 10


def _add_collections_section(
    sections: list[str],
    catalog_path: Path,
    aggregation: dict[str, Any],
) -> None:
    """Add collections listing section for catalog README.

    For large catalogs (>= COLLAPSIBLE_COLLECTIONS_THRESHOLD), wraps the
    collections list in an HTML <details> tag to make it collapsible.
    This improves README navigability for catalogs with many collections (#424).
    """
    collections = aggregation.get("collections", [])
    if not collections:
        return

    collection_count = len(collections)
    use_collapsible = collection_count >= COLLAPSIBLE_COLLECTIONS_THRESHOLD

    sections.append("## Collections")
    sections.append("")

    if use_collapsible:
        sections.append("<details>")
        sections.append(f"<summary>📁 {collection_count} collections (click to expand)</summary>")
        sections.append("")

    for coll_id in sorted(collections):
        coll_dir = catalog_path / coll_id
        coll_json = coll_dir / "collection.json"
        stac: dict[str, Any] = {"id": coll_id}

        if coll_json.exists():
            try:
                stac = json.loads(coll_json.read_text(encoding="utf-8"))
            except (json.JSONDecodeError, OSError):
                stac = {"id": coll_id}

        # metadata.yaml title/description override the STAC values (#534). A
        # malformed metadata.yaml in one collection must not abort the catalog
        # README, so fall back to STAC-only for that entry.
        try:
            coll_metadata = load_merged_metadata(coll_dir, catalog_path)
        except (ConfigInvalidStructureError, OSError):
            coll_metadata = {}
        title, description = _resolve_title_description(stac, coll_metadata)

        # Link to collection directory. Prefix with the catalog directory name
        # rather than using a bare "./" relative link (#549): static hosts like
        # source.coop serve the catalog README at a URL with no trailing slash
        # (e.g. /tyler/colombia-ecosystems-map), so "./ecosistemas/" resolves
        # against /tyler/ and drops the catalog name. Including the catalog dir
        # name makes it resolve to /tyler/colombia-ecosystems-map/ecosistemas/.
        sections.append(f"### [{title}]({catalog_path.name}/{coll_id}/)")
        sections.append("")
        if description:
            # Truncate long descriptions
            if len(description) > 200:
                description = description[:197] + "..."
            sections.append(description)
            sections.append("")

    if use_collapsible:
        sections.append("</details>")
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
        from portolan_cli.bbox import to_2d_bbox

        # Reduce to 2D so east/north are read from the correct indices for a
        # 6-element bbox (issue #592).
        west, south, east, north = to_2d_bbox(bbox)
        sections.append("**Spatial Extent**")
        sections.append("")
        sections.append(
            f"- West: {west:.4f}, South: {south:.4f}, East: {east:.4f}, North: {north:.4f}"
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
            catalog = json.loads(catalog_json.read_text(encoding="utf-8"))
        except (json.JSONDecodeError, OSError):
            pass

    # Load merged metadata
    metadata = load_merged_metadata(catalog_path, catalog_path)

    # Title and description (metadata.yaml overrides catalog.json, #534)
    title = (
        _metadata_override(metadata, "title")
        or catalog.get("title")
        or catalog.get("id", "Data Catalog")
    )
    description = _metadata_override(metadata, "description") or catalog.get("description", "")

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


#: Canonical filename for the human-readable documentation file.
README_FILENAME = "README.md"

#: STAC link relation that references the human-readable README.
README_LINK_REL = "describedby"

#: Media type the README link MUST declare.
README_MEDIA_TYPE = "text/markdown"

#: Relative href used when README.md sits next to the STAC JSON.
README_LINK_HREF = "./README.md"

#: Human-readable title for the README link.
README_LINK_TITLE = "Human-readable documentation"


def _build_readme_link() -> dict[str, str]:
    """Build a well-formed ``rel="describedby"`` link pointing at README.md."""
    return {
        "rel": README_LINK_REL,
        "href": README_LINK_HREF,
        "type": README_MEDIA_TYPE,
        "title": README_LINK_TITLE,
    }


def _href_targets_readme(directory: Path, href: str) -> bool:
    """True when ``href`` (relative to ``directory``) points at the sibling README.

    Path equality is the answer whenever resolution succeeds — including for a
    README that does not exist yet, since ``resolve()`` is non-strict. The
    basename check is a fallback for the resolution *failing* (an href the OS
    cannot express as a path); using it after a successful resolve matched any
    href merely ending in ``README.md``, so a publisher's link to another
    directory's README was mistaken for this object's own and overwritten.
    """
    if not href:
        return False
    try:
        resolved = (directory / href).resolve()
    except (OSError, ValueError):
        return PurePosixPath(href).name == README_FILENAME
    return resolved == (directory / README_FILENAME).resolve()


def _ensure_readme_link(directory: Path, data: dict[str, Any]) -> bool:
    """Insert or normalize the README's ``rel="describedby"`` link. True when changed.

    A STAC object may carry several ``describedby`` links (a data dictionary, a
    methodology PDF, ...). Only the one pointing at the sibling ``README.md`` is
    normalized; every other link is left untouched, and the README link is
    appended when none of them targets it. Overwriting the first ``describedby``
    link destroyed publisher-authored documentation pointers on every ``add``.
    """
    links = data.setdefault("links", [])
    if not isinstance(links, list):
        return False
    expected = _build_readme_link()
    for link in links:
        if not isinstance(link, dict) or link.get("rel") != README_LINK_REL:
            continue
        if not _href_targets_readme(directory, str(link.get("href") or "")):
            continue
        if all(link.get(key) == value for key, value in expected.items()):
            return False
        link.update(expected)
        return True
    links.append(expected)
    return True


def readme_link_gap(stac_path: Path, data: dict[str, Any]) -> bool:
    """True when ``data``'s README link does not satisfy rashid PTL-FIL-003.

    Replicates the four cases rashid's ``_check_markdown_link`` flags for
    ``rel="describedby"`` / ``README.md``: no link with the rel; a link whose
    ``type`` is not ``text/markdown``; an href that is missing, empty, or
    absolute; and an href that does not resolve to the sibling ``README.md`` (or
    resolves to one that does not exist). Every case is repaired by
    :func:`ensure_readmes`, so ``check --fix`` can act on the answer.

    Replicated rather than imported because rashid keeps ``_check_markdown_link``
    private. rashid#57 exported the COG predicate, the structural relations, and
    the multihash helpers, so those now come from ``rashid.api``; this one still
    has no public counterpart. A change to PTL-FIL-003 must land here too.

    Args:
        stac_path: Path of the ``catalog.json``/``collection.json`` (its parent
            directory is where ``README.md`` must sit).
        data: The parsed STAC object.

    Returns:
        True when the object needs repair.
    """
    return markdown_link_gap(stac_path, data, rel=README_LINK_REL, target=README_FILENAME)


def ensure_readmes(catalog_root: Path) -> bool:
    """Scaffold README.md and its ``describedby`` link across a catalog tree.

    Every catalog and collection directory carries a README.md referenced by a
    ``rel="describedby"`` markdown link (issue #654). The file is generated from
    the STAC object and metadata.yaml when absent; an existing README is never
    overwritten, so a human-authored or hand-edited one survives ``add``.
    Refreshing a stale README stays the job of ``portolan readme``.

    Idempotent: a tree that already conforms is not rewritten.

    Args:
        catalog_root: Root directory of the catalog.

    Returns:
        True if any file was written or modified.
    """
    changed_any = False
    stac_files = _visible_stac_files(catalog_root)

    for stac_file in stac_files:
        try:
            data = json.loads(stac_file.read_text(encoding="utf-8"))
        except (OSError, UnicodeDecodeError, json.JSONDecodeError):
            continue
        if not isinstance(data, dict):
            continue

        directory = stac_file.parent
        readme_path = directory / README_FILENAME
        if not readme_path.exists():
            content = (
                generate_readme_for_collection(directory, catalog_root)
                if stac_file.name == "collection.json"
                else generate_catalog_readme(directory)
            )
            readme_path.write_text(content, encoding="utf-8")
            changed_any = True

        if _ensure_readme_link(directory, data):
            write_json_atomic(stac_file, data)
            changed_any = True

    return changed_any
