"""Framework-free PMTiles asset/link constants and classifiers.

These helpers are extracted from :mod:`portolan_cli.viz.pmtiles` so the validation
layer can classify PMTiles assets and links **without** importing
``pmtiles.py``, which pulls in the ``output``/``thumbnail``/``style`` layers
(and transitively ``click``/``rich``/``config``).

Keeping them in a stdlib-only leaf preserves the rashid extraction seam
(issue #563; the ``validation-seam-for-rashid`` / ``validation-no-framework-leakage``
import-linter contracts): ``portolan_cli.validation`` must stay free of the
CLI/output/config framework layers. ``pmtiles.py`` re-imports these names so its
public API is unchanged.
"""

from __future__ import annotations

from typing import Any

# MIME type for PMTiles (matches add.py).
PMTILES_MEDIA_TYPE = "application/vnd.pmtiles"

# Filename suffix of a generated tile archive, and the single source of that
# name. ``pmtiles.py`` writes ``parquet_path.with_suffix(PMTILES_SUFFIX)``.
# ``derived_assets.py`` reads the same constant, so push and the writer agree on
# which files Portolan generated (Issue #735).
PMTILES_SUFFIX = ".pmtiles"

# web-map-links STAC extension declared for the rel="pmtiles" collection link
# (Issue #569). v1.3.0 defines the pmtiles rel, the application/vnd.pmtiles media
# type, and the pmtiles:layers field for default-visible vector layers.
WEB_MAP_LINKS_EXTENSION = "https://stac-extensions.github.io/web-map-links/v1.3.0/schema.json"


def pmtiles_asset_key(parquet_key: str) -> str:
    """Return the asset key of the PMTiles derivative of a GeoParquet asset.

    The PMTiles writer and the ``add`` key assignment both call this. A key
    derived any other way can equal the GeoParquet key and replace that asset
    (Issue #933).
    """
    return f"{parquet_key}-tiles"


def pmtiles_asset_hrefs(assets: dict[str, Any]) -> list[str]:
    """Return the hrefs of all PMTiles assets in a collection's asset dict.

    The PMTiles-link check (rashid PTL-VIZ-003) and its ``--fix`` repair both
    classify an asset with ``is_pmtiles_asset``, so they agree.
    """
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
