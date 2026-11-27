"""Framework-free PMTiles asset/link constants and classifiers."""

from __future__ import annotations

from typing import Any

# MIME type for PMTiles (matches add.py).
PMTILES_MEDIA_TYPE = "application/vnd.pmtiles"

# Suffix of a generated tile archive; the writer and push both read it.
PMTILES_SUFFIX = ".pmtiles"

# web-map-links extension (v1.3.0) for the rel="pmtiles" collection link.
WEB_MAP_LINKS_EXTENSION = "https://stac-extensions.github.io/web-map-links/v1.3.0/schema.json"


def pmtiles_asset_key(parquet_key: str) -> str:
    """Return the asset key of the PMTiles derivative of a GeoParquet asset."""
    return f"{parquet_key}-tiles"


def pmtiles_asset_hrefs(assets: dict[str, Any]) -> list[str]:
    """Return the hrefs of all PMTiles assets in a collection's asset dict."""
    from rashid_core.api import is_pmtiles_asset

    return [
        str(asset["href"])
        for asset in assets.values()
        if isinstance(asset, dict) and is_pmtiles_asset(asset) and asset.get("href")
    ]


def pmtiles_link_hrefs(links: list[Any]) -> set[str]:
    """Return the hrefs of all ``rel='pmtiles'`` links in a collection's links list."""
    return {
        str(link["href"])
        for link in links
        if isinstance(link, dict) and link.get("rel") == "pmtiles" and link.get("href")
    }
