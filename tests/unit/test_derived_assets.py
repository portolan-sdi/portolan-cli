"""Unit tests for optional-derivative classification (Issue #735).

The spec supplies the vocabulary. Section Assets in specs/portolan/core.md gives
every asset at least one role. Role `data` marks the primary file. Role `visual`
marks the PMTiles derivative a client draws. Role `thumbnail` marks the preview,
role `style` a MapLibre style, role `collection-mirror` the items.parquet copy.
Section Single-File Collections then says a collection "may optionally carry a
`.pmtiles`, a `thumbnail.png`, and a `styles/` directory". So an absent optional
derivative is no reason to fail a push. A `data` or `source` asset stays a hard
error.

The role comes first. The filename patterns run only when no STAC object claims
the href. Each pattern comes from the module that writes the file.
"""

from __future__ import annotations

import json
from pathlib import Path, PurePosixPath
from typing import Any

import pytest

from portolan_cli.derived_assets import (
    is_derived_asset,
    is_optional_derivative,
    resolve_asset_roles,
)

pytestmark = pytest.mark.unit


def _write_json(path: Path, data: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(data), encoding="utf-8")


def _asset(href: str, roles: list[str]) -> dict[str, Any]:
    return {"href": href, "type": "application/octet-stream", "roles": roles}


@pytest.fixture
def catalog(tmp_path: Path) -> Path:
    """A catalog whose STAC objects register one asset per spec role."""
    collection = tmp_path / "imagery"
    _write_json(
        collection / "collection.json",
        {
            "type": "Collection",
            "id": "imagery",
            "assets": {
                # A publisher's own PMTiles, published as the primary data.
                "data": _asset("./data.pmtiles", ["data"]),
                "geoparquet-items": _asset("./items.parquet", ["collection-mirror"]),
                "visual": _asset("./render.pmtiles", ["visual"]),
                "thumbnail": _asset("./thumbnail.png", ["thumbnail"]),
                "style-labeled": _asset("./styles/style.json", ["style", "default"]),
                "upstream": _asset("./original.gpkg", ["source"]),
                # An image that is the data itself, not a preview.
                "photo": _asset("./photo.png", ["data"]),
                "remote": _asset("https://example.com/elsewhere.tif", ["visual"]),
            },
        },
    )
    _write_json(
        collection / "scene1" / "scene1.json",
        {
            "type": "Feature",
            "id": "scene1",
            "assets": {
                "data": _asset("./scene1.tif", ["data"]),
                "thumbnail": _asset("./scene1.thumb.jpg", ["thumbnail"]),
            },
        },
    )
    return tmp_path


class TestResolveAssetRoles:
    """The href resolves to the roles its owning STAC object records."""

    def test_collection_level_asset(self, catalog: Path) -> None:
        assert resolve_asset_roles(catalog, "imagery/items.parquet") == {"collection-mirror"}

    def test_item_level_asset(self, catalog: Path) -> None:
        assert resolve_asset_roles(catalog, "imagery/scene1/scene1.tif") == {"data"}

    def test_collection_asset_inside_an_item_directory(self, catalog: Path) -> None:
        """A collection thumbnail may point into an item directory."""
        assert resolve_asset_roles(catalog, "imagery/scene1/scene1.thumb.jpg") == {"thumbnail"}

    def test_style_carries_both_roles(self, catalog: Path) -> None:
        assert resolve_asset_roles(catalog, "imagery/styles/style.json") == {"style", "default"}

    def test_unclaimed_href_resolves_to_nothing(self, catalog: Path) -> None:
        assert resolve_asset_roles(catalog, "imagery/orphan.parquet") == set()

    def test_absolute_href_never_matches_a_local_path(self, catalog: Path) -> None:
        assert resolve_asset_roles(catalog, "imagery/elsewhere.tif") == set()

    def test_unreadable_stac_object_resolves_to_nothing(self, tmp_path: Path) -> None:
        (tmp_path / "imagery").mkdir()
        (tmp_path / "imagery" / "collection.json").write_text("{not json", encoding="utf-8")
        assert resolve_asset_roles(tmp_path, "imagery/items.parquet") == set()


class TestOptionalByRole:
    """The role decides, per specs/portolan/core.md."""

    @pytest.mark.parametrize(
        "href",
        [
            "imagery/items.parquet",
            "imagery/render.pmtiles",
            "imagery/thumbnail.png",
            "imagery/styles/style.json",
            "imagery/scene1/scene1.thumb.jpg",
        ],
    )
    def test_optional_derivative_is_skippable(self, catalog: Path, href: str) -> None:
        assert is_optional_derivative(catalog, href) is True

    @pytest.mark.parametrize(
        "href",
        [
            # `data` beats the filename. A publisher may publish PMTiles as the
            # primary asset, and the extension registry gives .pmtiles role data.
            "imagery/data.pmtiles",
            "imagery/photo.png",
            "imagery/scene1/scene1.tif",
            # The publisher need not rehost an upstream original, but a local
            # copy that vanished is still unrecoverable data.
            "imagery/original.gpkg",
        ],
    )
    def test_required_asset_is_not_skippable(self, catalog: Path, href: str) -> None:
        assert is_optional_derivative(catalog, href) is False


class TestFilenameFallback:
    """With no role on record, the generated filenames decide."""

    @pytest.mark.parametrize(
        "href",
        ["imagery/orphan.thumb.jpg", "imagery/items.parquet", "imagery/orphan.pmtiles"],
    )
    def test_generated_name_is_skippable(self, tmp_path: Path, href: str) -> None:
        assert is_optional_derivative(tmp_path, href) is True

    @pytest.mark.parametrize(
        "href",
        ["imagery/thumbnail.png", "imagery/scene1/scene1.tif", "imagery/data.parquet"],
    )
    def test_unknown_name_is_not_skippable(self, tmp_path: Path, href: str) -> None:
        assert is_optional_derivative(tmp_path, href) is False


class TestDerivedArtifacts:
    """The fallback calls every generated filename derived."""

    @pytest.mark.parametrize(
        "href",
        [
            "items.parquet",
            "imagery/items.parquet",
            "imagery/nested/items.parquet",
            "imagery/data.pmtiles",
            "imagery/nested/data.pmtiles",
            "imagery/scene1/scene1.thumb.jpg",
            "imagery/data.thumb.jpg",
            "imagery/SCENE1/SCENE1.THUMB.JPG",
            "imagery/data.thumb.png",
        ],
    )
    def test_generated_artifact_is_derived(self, href: str) -> None:
        assert is_derived_asset(href) is True

    def test_items_parquet_name_comes_from_the_generator(self) -> None:
        """The mirror name is read from stac_parquet, not copied."""
        from portolan_cli.stac_parquet import PARQUET_FILENAME

        assert is_derived_asset(f"imagery/{PARQUET_FILENAME}") is True

    def test_pmtiles_suffix_comes_from_the_generator(self) -> None:
        """The PMTiles suffix is read from the viz layer, not copied."""
        from portolan_cli.viz.pmtiles_links import PMTILES_SUFFIX

        generated = PurePosixPath("imagery/data.parquet").with_suffix(PMTILES_SUFFIX)
        assert is_derived_asset(str(generated)) is True

    def test_thumbnail_name_comes_from_the_generator(self) -> None:
        """The ``{stem}.thumb.jpg`` convention is read from the viz layer."""
        from portolan_cli.viz.thumbnail import thumbnail_path_for

        generated = thumbnail_path_for(Path("imagery/scene1/scene1.tif"))
        assert is_derived_asset(generated.as_posix()) is True


class TestSourceArtifacts:
    """The fallback never calls an unknown filename derived."""

    @pytest.mark.parametrize(
        "href",
        [
            "imagery/scene1/scene1.tif",
            "imagery/data.parquet",
            "imagery/data.geojson",
            # A collection thumbnail Portolan adopted rather than drew. With no
            # role on record the conservative answer is a hard error (#735).
            "imagery/thumbnail.png",
            "imagery/thumbnail.jpg",
            "imagery/preview.png",
            # A photo collection: plain images that are the data itself.
            "photos/2024/portrait.jpg",
            "photos/2024/thumb.jpg",
            # Near misses on the generated names.
            "imagery/my-items.parquet",
            "imagery/items.parquet.bak",
            "imagery/pmtiles",
            "imagery/thumb.parquet",
        ],
    )
    def test_source_artifact_is_not_derived(self, href: str) -> None:
        assert is_derived_asset(href) is False
