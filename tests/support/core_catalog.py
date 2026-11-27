"""The input of the golden catalog that the portolan-core writer tests share.
It carries a stale ``child`` link and a mistyped ``describedby`` link on purpose.
"""

from __future__ import annotations

from pathlib import Path
from typing import Any

GOLDEN_DIR = Path(__file__).resolve().parents[1] / "fixtures" / "core" / "golden_catalog"
PROFILE = "https://schemas.portolan-sdi.org/portolan/v0.3.0/schema.json"
PROVIDER = {"name": "Example Agency", "roles": ["producer", "host"], "url": "https://example.org"}
SOUTH = "environment/air-quality/Estación Sur"


def _extent() -> dict[str, Any]:
    return {
        "spatial": {"bbox": [[-5.0, 36.0, -4.0, 37.0]]},
        "temporal": {"interval": [["2024-01-01T00:00:00Z", "2024-12-31T23:59:59Z"]]},
    }


def _thumbnail() -> dict[str, Any]:
    return {
        "href": "./thumbnail.png",
        "type": "image/png",
        "roles": ["thumbnail"],
        "title": "Thumbnail",
        "description": "A preview image of the data.",
    }


def _item(item_id: str, properties: dict[str, Any]) -> dict[str, Any]:
    return {
        "type": "Feature",
        "stac_version": "1.1.0",
        "id": item_id,
        "geometry": {
            "type": "Polygon",
            "coordinates": [[[-5.0, 36.0], [-4.0, 36.0], [-4.0, 37.0], [-5.0, 37.0], [-5.0, 36.0]]],
        },
        "bbox": [-5.0, 36.0, -4.0, 37.0],
        "properties": {"datetime": "2024-06-01T00:00:00Z", **properties},
        "assets": {
            "pm25": {
                "href": "./pm25.tif",
                "type": "image/tiff; application=geotiff; profile=cloud-optimized",
                "roles": ["data"],
                "title": "PM2.5 concentration",
                "description": "Mean PM2.5 concentration in micrograms per cubic metre.",
            }
        },
        "links": [],
    }


def golden_documents() -> dict[str, dict[str, Any]]:
    """The STAC documents of the golden catalog, keyed by root-relative path."""
    return {
        "catalog.json": {
            "type": "Catalog",
            "stac_version": "1.1.0",
            "stac_extensions": [PROFILE],
            "id": "example-open-data",
            "title": "Example Open Data",
            "description": "A small catalog that tests the portolan-core writer.",
            "links": [
                {"rel": "child", "href": "./removed/collection.json", "type": "application/json"},
            ],
        },
        "boundaries/collection.json": {
            "type": "Collection",
            "stac_version": "1.1.0",
            "stac_extensions": [PROFILE],
            "id": "boundaries",
            "title": "Administrative Boundaries",
            "description": "Municipal boundaries as one GeoParquet file.",
            "license": "CC-BY-4.0",
            "providers": [PROVIDER],
            "extent": _extent(),
            "assets": {
                "boundaries": {
                    "href": "./boundaries.parquet",
                    "type": "application/vnd.apache.parquet",
                    "roles": ["data"],
                    "title": "Boundaries",
                    "description": "Municipal boundary polygons.",
                },
                "thumbnail": _thumbnail(),
            },
            "links": [
                {
                    "rel": "describedby",
                    "href": "./README.md",
                    "type": "text/plain",
                    "title": "Read me",
                },
                {
                    "rel": "via",
                    "href": "https://example.org/boundaries",
                    "type": "text/html",
                    "title": "Source page",
                },
            ],
        },
        "environment/catalog.json": {
            "type": "Catalog",
            "stac_version": "1.1.0",
            "stac_extensions": [PROFILE],
            "id": "environment",
            "title": "Environment",
            "description": "Environmental collections.",
            "links": [],
        },
        "environment/air-quality/collection.json": {
            "type": "Collection",
            "stac_version": "1.1.0",
            "stac_extensions": [PROFILE],
            "id": "environment/air-quality",
            "title": "Air Quality",
            "description": "PM2.5 rasters from two monitoring stations.",
            "license": "CC-BY-4.0",
            "providers": [PROVIDER],
            "extent": _extent(),
            "assets": {"thumbnail": _thumbnail()},
            "links": [],
        },
        "environment/air-quality/station-north/station-north.json": _item(
            "station-north", {"title": "North Station"}
        ),
        f"{SOUTH}/Estación Sur.json": _item("Estación Sur", {}),
    }


def golden_documentation() -> dict[str, str]:
    """The README.md and AGENTS.md text of the golden catalog, read from the fixture."""
    return {
        path.relative_to(GOLDEN_DIR).as_posix(): path.read_text(encoding="utf-8")
        for path in sorted(GOLDEN_DIR.rglob("*.md"))
    }


def golden_files() -> dict[str, bytes]:
    """Every expected file of the golden catalog, keyed by root-relative path."""
    return {
        path.relative_to(GOLDEN_DIR).as_posix(): path.read_bytes()
        for path in sorted(GOLDEN_DIR.rglob("*"))
        if path.is_file()
    }
