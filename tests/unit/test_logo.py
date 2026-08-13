"""Unit tests for catalog logo support (PORTO-CORE-074..077).

Covers the leaf writer ``portolan_cli.logo``: media-type inference from the
extension, the ``rel="icon"`` link shape, the ``_assets/`` copy, replacement
semantics, and the errors raised for a source the spec's enum rejects.
"""

from __future__ import annotations

import json
from pathlib import Path

import pytest

from portolan_cli.errors import (
    LogoSourceNotFoundError,
    RemoteLogoSourceError,
    UnsupportedLogoFormatError,
)
from portolan_cli.logo import (
    LOGO_ASSETS_DIRNAME,
    LOGO_LINK_REL,
    LOGO_MEDIA_TYPES,
    SVG_MEDIA_TYPE,
    build_logo_link,
    find_logo_link,
    logo_media_type,
    set_catalog_logo,
)

pytestmark = pytest.mark.unit


def _catalog(root: Path, **overrides: object) -> Path:
    data: dict[str, object] = {
        "type": "Catalog",
        "stac_version": "1.1.0",
        "id": "demo",
        "title": "Demo Catalog",
        "description": "d",
        "links": [{"rel": "root", "href": "./catalog.json", "type": "application/json"}],
    }
    data.update(overrides)
    path = root / "catalog.json"
    path.write_text(json.dumps(data, indent=2), encoding="utf-8")
    return path


def _image(root: Path, name: str = "logo.png") -> Path:
    path = root / name
    path.write_bytes(b"\x89PNG\r\n\x1a\n")
    return path


def _links(root: Path) -> list[dict[str, str]]:
    data = json.loads((root / "catalog.json").read_text(encoding="utf-8"))
    return [link for link in data["links"] if isinstance(link, dict)]


def _icon(root: Path) -> dict[str, str]:
    link = find_logo_link(_links(root))
    assert link is not None, "expected a rel=icon link"
    return link


class TestMediaTypes:
    def test_enum_matches_the_spec(self) -> None:
        """PORTO-CORE-075 names exactly seven permitted media types."""
        assert set(LOGO_MEDIA_TYPES.values()) == {
            "image/apng",
            "image/avif",
            "image/gif",
            "image/jpeg",
            "image/png",
            "image/svg+xml",
            "image/webp",
        }

    @pytest.mark.parametrize(
        ("name", "expected"),
        [
            ("logo.png", "image/png"),
            ("logo.PNG", "image/png"),
            ("logo.jpg", "image/jpeg"),
            ("logo.jpeg", "image/jpeg"),
            ("logo.gif", "image/gif"),
            ("logo.webp", "image/webp"),
            ("logo.avif", "image/avif"),
            ("logo.apng", "image/apng"),
            ("logo.svg", "image/svg+xml"),
        ],
    )
    def test_inferred_from_extension(self, name: str, expected: str) -> None:
        assert logo_media_type(Path(name)) == expected

    @pytest.mark.parametrize("name", ["logo.bmp", "logo.tif", "logo.ico", "logo.pdf", "logo"])
    def test_rejects_types_outside_the_enum(self, name: str) -> None:
        with pytest.raises(UnsupportedLogoFormatError):
            logo_media_type(Path(name))


class TestBuildLogoLink:
    def test_canonical_shape(self) -> None:
        assert build_logo_link("portolan-logo.png", "image/png", "Portolan SDI") == {
            "rel": "icon",
            "href": "./_assets/portolan-logo.png",
            "type": "image/png",
            "title": "Portolan SDI",
        }

    def test_find_logo_link(self) -> None:
        link = build_logo_link("a.png", "image/png", "A")
        assert find_logo_link([{"rel": "root", "href": "./catalog.json"}, link]) is link
        assert find_logo_link([{"rel": "root", "href": "./catalog.json"}]) is None


class TestSetCatalogLogo:
    def test_copies_file_and_writes_link(self, tmp_path: Path) -> None:
        _catalog(tmp_path)
        source = _image(tmp_path, "brand.png")

        result = set_catalog_logo(tmp_path, source, title="Portolan SDI")

        copied = tmp_path / LOGO_ASSETS_DIRNAME / "brand.png"
        assert copied.exists()
        assert copied.read_bytes() == source.read_bytes()
        assert result.href == "./_assets/brand.png"
        assert result.media_type == "image/png"
        assert result.warnings == []
        assert _links(tmp_path)[-1] == {
            "rel": "icon",
            "href": "./_assets/brand.png",
            "type": "image/png",
            "title": "Portolan SDI",
        }

    def test_title_defaults_to_catalog_title(self, tmp_path: Path) -> None:
        _catalog(tmp_path)
        set_catalog_logo(tmp_path, _image(tmp_path))
        assert _icon(tmp_path)["title"] == "Demo Catalog"

    def test_title_falls_back_to_catalog_id(self, tmp_path: Path) -> None:
        _catalog(tmp_path, title="")
        set_catalog_logo(tmp_path, _image(tmp_path))
        assert _icon(tmp_path)["title"] == "demo"

    def test_replacement_leaves_exactly_one_icon_link(self, tmp_path: Path) -> None:
        _catalog(tmp_path)
        first = _image(tmp_path, "old.png")
        second = tmp_path / "new.webp"
        second.write_bytes(b"RIFF")

        set_catalog_logo(tmp_path, first)
        set_catalog_logo(tmp_path, second)

        icons = [link for link in _links(tmp_path) if link.get("rel") == LOGO_LINK_REL]
        assert len(icons) == 1
        assert icons[0]["href"] == "./_assets/new.webp"
        assert icons[0]["type"] == "image/webp"
        # The superseded image is removed, so `_assets/` never accumulates strays.
        assert not (tmp_path / LOGO_ASSETS_DIRNAME / "old.png").exists()
        assert (tmp_path / LOGO_ASSETS_DIRNAME / "new.webp").exists()

    def test_idempotent_for_the_same_source(self, tmp_path: Path) -> None:
        _catalog(tmp_path)
        source = _image(tmp_path)
        set_catalog_logo(tmp_path, source)
        before = (tmp_path / "catalog.json").read_text(encoding="utf-8")

        set_catalog_logo(tmp_path, source)

        assert (tmp_path / "catalog.json").read_text(encoding="utf-8") == before
        assert (tmp_path / LOGO_ASSETS_DIRNAME / "logo.png").exists()

    def test_svg_warns_but_is_accepted(self, tmp_path: Path) -> None:
        _catalog(tmp_path)
        source = tmp_path / "mark.svg"
        source.write_text("<svg/>", encoding="utf-8")

        result = set_catalog_logo(tmp_path, source)

        assert result.media_type == SVG_MEDIA_TYPE
        assert result.warnings and "svg" in result.warnings[0].lower()
        assert _icon(tmp_path)["type"] == SVG_MEDIA_TYPE

    def test_rejects_unsupported_extension_without_touching_the_catalog(
        self, tmp_path: Path
    ) -> None:
        _catalog(tmp_path)
        source = tmp_path / "logo.bmp"
        source.write_bytes(b"BM")
        before = (tmp_path / "catalog.json").read_text(encoding="utf-8")

        with pytest.raises(UnsupportedLogoFormatError):
            set_catalog_logo(tmp_path, source)

        assert (tmp_path / "catalog.json").read_text(encoding="utf-8") == before
        assert not (tmp_path / LOGO_ASSETS_DIRNAME).exists()

    @pytest.mark.parametrize(
        "url",
        [
            "https://example.org/logo.png",
            "http://example.org/logo.png",
            "s3://bucket/logo.png",
        ],
    )
    def test_rejects_remote_sources(self, tmp_path: Path, url: str) -> None:
        _catalog(tmp_path)
        with pytest.raises(RemoteLogoSourceError):
            set_catalog_logo(tmp_path, url)

    def test_missing_source(self, tmp_path: Path) -> None:
        _catalog(tmp_path)
        with pytest.raises(LogoSourceNotFoundError):
            set_catalog_logo(tmp_path, tmp_path / "absent.png")

    def test_directory_source_is_not_a_file(self, tmp_path: Path) -> None:
        _catalog(tmp_path)
        directory = tmp_path / "logo.png"
        directory.mkdir()
        with pytest.raises(LogoSourceNotFoundError):
            set_catalog_logo(tmp_path, directory)

    def test_writes_only_the_root_catalog(self, tmp_path: Path) -> None:
        """PORTO-CORE-074 scopes the logo to the root catalog, never a collection."""
        _catalog(tmp_path)
        collection_dir = tmp_path / "roads"
        collection_dir.mkdir()
        collection = collection_dir / "collection.json"
        collection.write_text(json.dumps({"type": "Collection", "links": []}), encoding="utf-8")

        set_catalog_logo(tmp_path, _image(tmp_path))

        assert json.loads(collection.read_text(encoding="utf-8"))["links"] == []

    def test_href_is_relative(self, tmp_path: Path) -> None:
        """PORTO-CORE-077: no absolute filesystem path may leak into the href."""
        _catalog(tmp_path)
        result = set_catalog_logo(tmp_path, _image(tmp_path))
        assert result.href.startswith("./")
        assert str(tmp_path) not in (tmp_path / "catalog.json").read_text(encoding="utf-8")

    def test_missing_catalog_json(self, tmp_path: Path) -> None:
        from portolan_cli.errors import CatalogNotFoundError

        with pytest.raises(CatalogNotFoundError):
            set_catalog_logo(tmp_path, _image(tmp_path))
