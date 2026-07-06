"""Framework-free PMTiles asset/link constants and classifiers.

These helpers are extracted from :mod:`portolan_cli.pmtiles` so the validation
layer can classify PMTiles assets and links **without** importing
``pmtiles.py``, which pulls in the ``output``/``thumbnail``/``style`` layers
(and transitively ``click``/``rich``/``config``).

Keeping them in a stdlib-only leaf preserves the reis extraction seam
(issue #563; the ``validation-seam-for-reis`` / ``validation-no-framework-leakage``
import-linter contracts): ``portolan_cli.validation`` must stay free of the
CLI/output/config framework layers. ``pmtiles.py`` re-imports these names so its
public API is unchanged.
"""

from __future__ import annotations

from typing import Any

# MIME type for PMTiles (matches add.py).
PMTILES_MEDIA_TYPE = "application/vnd.pmtiles"

# web-map-links STAC extension declared for the rel="pmtiles" collection link
# (Issue #569). v1.3.0 defines the pmtiles rel, the application/vnd.pmtiles media
# type, and the pmtiles:layers field for default-visible vector layers.
WEB_MAP_LINKS_EXTENSION = "https://stac-extensions.github.io/web-map-links/v1.3.0/schema.json"


def pmtiles_asset_hrefs(assets: dict[str, Any]) -> list[str]:
    """Return the hrefs of all PMTiles assets in a collection's asset dict.

    An asset is a PMTiles asset when its ``type`` is ``application/vnd.pmtiles``
    or its ``href`` ends in ``.pmtiles``. Shared by the RULE-0061 check and its
    ``--fix`` repair so both classify assets identically.
    """
    return [
        str(asset["href"])
        for asset in assets.values()
        if isinstance(asset, dict)
        and (
            asset.get("type") == PMTILES_MEDIA_TYPE
            or str(asset.get("href", "")).endswith(".pmtiles")
        )
        and asset.get("href")
    ]


def pmtiles_link_hrefs(links: list[Any]) -> set[str]:
    """Return the hrefs of all ``rel='pmtiles'`` links in a collection's links list."""
    return {
        str(link["href"])
        for link in links
        if isinstance(link, dict) and link.get("rel") == "pmtiles" and link.get("href")
    }
