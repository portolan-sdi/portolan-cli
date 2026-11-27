"""Catalog logo: the ``rel="icon"`` link on the root catalog (PORTO-CORE-074..077)."""

from __future__ import annotations

import json
import re
import shutil
from dataclasses import dataclass, field
from pathlib import Path, PurePosixPath
from typing import Any

from portolan_core.errors import (
    CatalogNotFoundError,
    LogoSourceNotFoundError,
    RemoteLogoSourceError,
    UnsupportedLogoFormatError,
)
from portolan_core.json_io import write_json_atomic

#: STAC link relation that carries the catalog logo (PORTO-CORE-074).
LOGO_LINK_REL = "icon"

#: Directory beside the root ``catalog.json`` that holds the image
#: (PORTO-CORE-077). Leading underscore keeps it out of collection-id space.
LOGO_ASSETS_DIRNAME = "_assets"

#: The one permitted media type that STAC Browser will not render.
SVG_MEDIA_TYPE = "image/svg+xml"

#: File extension to media type, covering exactly the seven types
#: PORTO-CORE-075 permits. Extensions are lowercase; lookup lowercases first.
LOGO_MEDIA_TYPES: dict[str, str] = {
    ".apng": "image/apng",
    ".avif": "image/avif",
    ".gif": "image/gif",
    ".jpeg": "image/jpeg",
    ".jpg": "image/jpeg",
    ".png": "image/png",
    ".svg": SVG_MEDIA_TYPE,
    ".webp": "image/webp",
}

#: Warning emitted for an SVG logo, which conforms but renders nowhere in
#: STAC Browser.
SVG_WARNING = (
    "SVG logos conform to the spec, but stac-js rejects image/svg+xml, "
    "so STAC Browser will not display this logo. Use PNG or WebP for it to render."
)

_SCHEME_RE = re.compile(r"^[a-zA-Z][a-zA-Z0-9+.-]*://")


@dataclass
class LogoResult:
    """Outcome of writing a catalog logo."""

    href: str
    media_type: str
    title: str
    path: Path
    changed: bool
    warnings: list[str] = field(default_factory=list)

    def to_dict(self) -> dict[str, Any]:
        """Return the envelope data of ``portolan logo``."""
        return {
            "href": self.href,
            "type": self.media_type,
            "title": self.title,
            "path": f"{LOGO_ASSETS_DIRNAME}/{self.path.name}",
            "warnings": self.warnings,
        }


def logo_media_type(source: Path) -> str:
    """Return the media type for ``source``, from its extension."""
    media_type = LOGO_MEDIA_TYPES.get(source.suffix.lower())
    if media_type is None:
        raise UnsupportedLogoFormatError(str(source), sorted(set(LOGO_MEDIA_TYPES.values())))
    return media_type


def build_logo_link(filename: str, media_type: str, title: str) -> dict[str, str]:
    """Build the canonical ``rel="icon"`` link for an image in ``_assets/``."""
    return {
        "rel": LOGO_LINK_REL,
        "href": f"./{LOGO_ASSETS_DIRNAME}/{filename}",
        "type": media_type,
        "title": title,
    }


def find_logo_link(links: list[Any]) -> dict[str, Any] | None:
    """Return the first ``rel="icon"`` link in a STAC ``links`` array, if any."""
    for link in links:
        if isinstance(link, dict) and link.get("rel") == LOGO_LINK_REL:
            return link
    return None


def default_logo_title(data: dict[str, Any]) -> str:
    """Derive the link title from the catalog's own title, then its id."""
    for key in ("title", "id"):
        value = data.get(key)
        if isinstance(value, str) and value.strip():
            return value.strip()
    return "Catalog logo"


def validate_logo_source(source: str | Path) -> tuple[Path, str]:
    """Validate a logo source without writing anything."""
    raw = str(source)
    if _SCHEME_RE.match(raw):
        raise RemoteLogoSourceError(raw)
    path = Path(raw).expanduser()
    if not path.is_file():
        raise LogoSourceNotFoundError(str(path))
    resolved = path.resolve()
    return resolved, logo_media_type(resolved)


def _existing_logo_file(catalog_root: Path, link: dict[str, Any]) -> Path | None:
    """Return the image a prior ``icon`` link points at, when we may delete it."""
    href = str(link.get("href") or "")
    if not href or _SCHEME_RE.match(href) or href.startswith("/"):
        return None
    assets_dir = (catalog_root / LOGO_ASSETS_DIRNAME).resolve()
    try:
        resolved = (catalog_root / PurePosixPath(href)).resolve()
    except (OSError, ValueError):
        return None
    if resolved.parent != assets_dir:
        return None
    return resolved


def _read_root_catalog(catalog_root: Path) -> dict[str, Any]:
    """Parse the root ``catalog.json``, or report the directory as no catalog."""
    catalog_json = catalog_root / "catalog.json"
    try:
        data = json.loads(catalog_json.read_text(encoding="utf-8"))
    except (OSError, UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise CatalogNotFoundError(str(catalog_root)) from exc
    if not isinstance(data, dict):
        raise CatalogNotFoundError(str(catalog_root))
    return data


def _reconcile_logo_links(
    catalog_root: Path,
    data: dict[str, Any],
    expected: dict[str, str],
    destination: Path,
) -> tuple[bool, list[Path]]:
    """Leave exactly one ``rel="icon"`` link, the expected one."""
    links = data.get("links")
    if not isinstance(links, list):
        links = []
        data["links"] = links

    existing = [
        link for link in links if isinstance(link, dict) and link.get("rel") == LOGO_LINK_REL
    ]
    stale = [
        previous
        for previous in (_existing_logo_file(catalog_root, link) for link in existing)
        if previous is not None and previous != destination.resolve()
    ]
    if len(existing) == 1 and existing[0] == expected:
        return False, stale
    for link in existing:
        links.remove(link)
    links.append(expected)
    return True, stale


def set_catalog_logo(
    catalog_root: Path,
    source: str | Path,
    *,
    title: str | None = None,
) -> LogoResult:
    """Publish ``source`` as the catalog logo on the root ``catalog.json``."""
    image, media_type = validate_logo_source(source)

    data = _read_root_catalog(catalog_root)

    link_title = title.strip() if title and title.strip() else default_logo_title(data)
    destination = catalog_root / LOGO_ASSETS_DIRNAME / image.name
    expected = build_logo_link(image.name, media_type, link_title)
    json_changed, stale = _reconcile_logo_links(catalog_root, data, expected, destination)

    destination.parent.mkdir(parents=True, exist_ok=True)
    image_changed = not destination.exists() or destination.read_bytes() != image.read_bytes()
    if image_changed:
        shutil.copy2(image, destination)
    # Drop the image a superseded link owned, after the new one is in place.
    for path in stale:
        path.unlink(missing_ok=True)

    if json_changed:
        write_json_atomic(catalog_root / "catalog.json", data)

    warnings = [SVG_WARNING] if media_type == SVG_MEDIA_TYPE else []
    return LogoResult(
        href=expected["href"],
        media_type=media_type,
        title=link_title,
        path=destination,
        changed=image_changed or json_changed or bool(stale),
        warnings=warnings,
    )
