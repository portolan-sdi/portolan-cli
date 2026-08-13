"""Catalog logo: the ``rel="icon"`` link on the root catalog (PORTO-CORE-074..077).

A registry lists many catalogs side by side, and a logo makes each one
recognizable before the reader has read a word. The root catalog MAY publish one
as a link with ``rel="icon"`` (PORTO-CORE-074). STAC defines no ``logo``
relation, and ``preview`` means a preview of the data itself, so ``icon`` — IANA
registered, already read by stac-js and STAC Browser — is the relation.

The link is a **link**, not an asset: it points at the catalog's identity, not at
its data. It lives on the root ``catalog.json`` only. A collection publishes its
data preview as a ``thumbnail`` asset instead, which
``portolan_cli.collection_thumbnail`` owns.

This module is the single writer for that link, shared by ``portolan init
--logo`` and ``portolan logo``. It is deliberately dependency-light — stdlib plus
the shared atomic JSON writer — for the same reason ``agents_md.py`` is: leaf
modules stay importable from every layer without breaking the import-linter
contracts.

Two spec details drive the shape of the code:

- The media type is a closed enum of seven (PORTO-CORE-075). Anything else is
  rejected outright rather than guessed at, because a client drops an icon whose
  media type it does not recognize, so a wrong or absent ``type`` renders
  nowhere. ``mimetypes`` is not used: it is platform-dependent and would happily
  return ``image/bmp``.
- ``image/svg+xml`` is inside the enum but stac-js refuses to render it, so an
  SVG logo is accepted with a warning rather than blocked. The publisher may be
  targeting something other than STAC Browser.
"""

from __future__ import annotations

import json
import re
import shutil
from dataclasses import dataclass, field
from pathlib import Path, PurePosixPath
from typing import Any

from portolan_cli.errors import (
    CatalogNotFoundError,
    LogoSourceNotFoundError,
    RemoteLogoSourceError,
    UnsupportedLogoFormatError,
)
from portolan_cli.json_io import write_json_atomic

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
    """Outcome of writing a catalog logo.

    Attributes:
        href: Relative href written into the link (``./_assets/<filename>``).
        media_type: The link's ``type``.
        title: The link's ``title``.
        path: Absolute path of the copied image inside the catalog.
        changed: True when the catalog JSON or the image on disk was modified.
        warnings: Non-fatal notes for the caller to surface (SVG, ...).
    """

    href: str
    media_type: str
    title: str
    path: Path
    changed: bool
    warnings: list[str] = field(default_factory=list)


def logo_media_type(source: Path) -> str:
    """Return the media type for ``source``, from its extension.

    Args:
        source: Path whose suffix names the image format. Only the suffix is
            read, so the file need not exist.

    Returns:
        One of the seven media types PORTO-CORE-075 permits.

    Raises:
        UnsupportedLogoFormatError: If the extension is missing or outside the
            enum. Guessing would produce a link no client renders.
    """
    media_type = LOGO_MEDIA_TYPES.get(source.suffix.lower())
    if media_type is None:
        raise UnsupportedLogoFormatError(str(source), sorted(set(LOGO_MEDIA_TYPES.values())))
    return media_type


def build_logo_link(filename: str, media_type: str, title: str) -> dict[str, str]:
    """Build the canonical ``rel="icon"`` link for an image in ``_assets/``.

    Args:
        filename: Basename of the image inside ``_assets/``.
        media_type: One of the permitted media types.
        title: Accessible label for the image (PORTO-CORE-076).

    Returns:
        The link dict, with a relative href (PORTO-CORE-077).
    """
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
    """Derive the link title from the catalog's own title, then its id.

    Only the publisher knows what the logo shows (PORTO-CORE-076), so this is a
    fallback for callers that pass no title, not a guess at the artwork.
    """
    for key in ("title", "id"):
        value = data.get(key)
        if isinstance(value, str) and value.strip():
            return value.strip()
    return "Catalog logo"


def validate_logo_source(source: str | Path) -> tuple[Path, str]:
    """Validate a logo source without writing anything.

    Callers that mutate a catalog check the logo first, so a rejected image
    never leaves a half-built catalog behind: ``init`` calls this before it
    creates any file, then :func:`set_catalog_logo` once ``catalog.json`` exists.

    Args:
        source: Local path to the image. URLs are rejected.

    Returns:
        Tuple of (resolved path, media type).

    Raises:
        RemoteLogoSourceError: If ``source`` carries a URL scheme.
        LogoSourceNotFoundError: If it is not an existing file.
        UnsupportedLogoFormatError: If the extension is outside PORTO-CORE-075.
    """
    raw = str(source)
    if _SCHEME_RE.match(raw):
        raise RemoteLogoSourceError(raw)
    path = Path(raw).expanduser()
    if not path.is_file():
        raise LogoSourceNotFoundError(str(path))
    resolved = path.resolve()
    return resolved, logo_media_type(resolved)


def _existing_logo_file(catalog_root: Path, link: dict[str, Any]) -> Path | None:
    """Return the image a prior ``icon`` link points at, when we may delete it.

    Only a relative href resolving inside ``_assets/`` qualifies. An absolute
    href to an external host stays valid under PORTO-CORE-077, and a publisher
    who points at a shared logo elsewhere must not have that file removed.
    """
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
    """Leave exactly one ``rel="icon"`` link, the expected one.

    Returns (whether the links array changed, images the removed links owned).
    A link already equal to ``expected`` is left in place rather than removed and
    re-appended, so a re-run rewrites nothing.
    """
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
    """Publish ``source`` as the catalog logo on the root ``catalog.json``.

    Copies the image to ``_assets/<filename>`` beside the root catalog and writes
    a single ``rel="icon"`` link pointing at it with a relative href. Re-running
    with a different image replaces both the link and the superseded file, so
    exactly one icon link and at most one logo image ever exist.

    Every step that can fail (URL source, missing file, unpermitted media type,
    missing catalog) is checked before anything is written, so a rejected logo
    leaves the catalog byte-identical.

    Nothing else in the tree is touched: a logo belongs to the catalog's
    identity, and collections carry a ``thumbnail`` asset instead.

    Args:
        catalog_root: Directory holding the root ``catalog.json``.
        source: Local path to the image. A URL is rejected — Portolan copies the
            file so the published href stays relative, and downloading is the
            caller's job.
        title: Accessible label for the link. Defaults to the catalog's title,
            then its id.

    Returns:
        A :class:`LogoResult` describing what was written, including warnings
        (an SVG logo conforms but STAC Browser will not render it).

    Raises:
        RemoteLogoSourceError: If ``source`` is a URL.
        LogoSourceNotFoundError: If ``source`` is not an existing file.
        UnsupportedLogoFormatError: If the extension is outside PORTO-CORE-075.
        CatalogNotFoundError: If ``catalog_root`` holds no readable
            ``catalog.json``.
    """
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
