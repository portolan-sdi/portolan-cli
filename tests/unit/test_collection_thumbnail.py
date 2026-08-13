"""Unit tests for the collection-level thumbnail orchestrator (Issue #683).

``portolan init`` followed by ``portolan add`` used to produce a catalog that
failed its own ``portolan check``: rashid's ``PTL-VIZ-001`` requires every
geospatial collection to carry a ``thumbnail``-role asset, and the default add
pipeline registered none. These tests pin the ladder that closes the gap —
opt-out, already-registered, adopt, reference, render — and the guarantee that a
render failure never fails the add.
"""

from __future__ import annotations

import json
import shutil
from pathlib import Path
from typing import Any
from unittest.mock import patch

import pytest

from portolan_cli.collection_thumbnail import (
    ensure_collection_thumbnails,
    register_collection_thumbnail,
)

JPEG_BYTES = b"\xff\xd8\xff fake-jpeg-bytes"
PNG_BYTES = b"\x89PNG\r\n\x1a\n fake-png-bytes"

#: A real GeoParquet. Geospatial status is derived by reading the ``geo`` schema
#: metadata, so a collection built from placeholder bytes reads as tabular and is
#: skipped — the same way ``PTL-VIZ-001`` skips it.
GEOPARQUET_FIXTURE = Path(__file__).parent.parent / "fixtures" / "simple.parquet"


def _write_collection(collection_dir: Path, assets: dict[str, Any] | None = None) -> Path:
    """Write a minimal collection.json and return its path."""
    collection_dir.mkdir(parents=True, exist_ok=True)
    data: dict[str, Any] = {
        "type": "Collection",
        "id": collection_dir.name,
        "assets": assets if assets is not None else {},
        "links": [],
    }
    path = collection_dir / "collection.json"
    path.write_text(json.dumps(data), encoding="utf-8")
    return path


def _vector_collection(catalog_root: Path, name: str = "roads") -> Path:
    """A single-file vector collection: one collection-level GeoParquet asset."""
    collection_dir = catalog_root / name
    collection_dir.mkdir(parents=True, exist_ok=True)
    shutil.copy(GEOPARQUET_FIXTURE, collection_dir / f"{name}.parquet")
    _write_collection(
        collection_dir,
        {
            name: {
                "href": f"./{name}.parquet",
                "type": "application/vnd.apache.parquet",
                "roles": ["data"],
            }
        },
    )
    return collection_dir


def _read_assets(collection_dir: Path) -> dict[str, Any]:
    data = json.loads((collection_dir / "collection.json").read_text(encoding="utf-8"))
    assets: dict[str, Any] = data.get("assets", {})
    return assets


def _thumbnail_assets(collection_dir: Path) -> dict[str, Any]:
    return {
        key: asset
        for key, asset in _read_assets(collection_dir).items()
        if "thumbnail" in asset.get("roles", [])
    }


class TestRegisterCollectionThumbnail:
    """The single writer for a collection-level thumbnail asset."""

    @pytest.mark.unit
    def test_writes_canonical_key_roles_and_media_type(self, tmp_path: Path) -> None:
        collection_dir = tmp_path / "roads"
        _write_collection(collection_dir)
        thumb = collection_dir / "roads.thumb.jpg"
        thumb.write_bytes(JPEG_BYTES)

        register_collection_thumbnail(collection_dir, thumb)

        assets = _read_assets(collection_dir)
        assert "thumbnail" in assets, "The canonical asset key is 'thumbnail'"
        assert assets["thumbnail"]["href"] == "./roads.thumb.jpg"
        assert assets["thumbnail"]["type"] == "image/jpeg"
        assert assets["thumbnail"]["roles"] == ["thumbnail"]

    @pytest.mark.unit
    def test_carries_file_size_and_multihash_checksum(self, tmp_path: Path) -> None:
        """PTL-AST-003 wants file:size and file:checksum; the checksum is a multihash."""
        collection_dir = tmp_path / "roads"
        _write_collection(collection_dir)
        thumb = collection_dir / "roads.thumb.jpg"
        thumb.write_bytes(JPEG_BYTES)

        register_collection_thumbnail(collection_dir, thumb)

        asset = _read_assets(collection_dir)["thumbnail"]
        assert asset["file:size"] == len(JPEG_BYTES)
        # sha2-256 multihash: 0x12 (fn code), 0x20 (32-byte length), then the digest.
        assert asset["file:checksum"].startswith("1220")
        assert len(asset["file:checksum"]) == 68

    @pytest.mark.unit
    def test_png_gets_png_media_type(self, tmp_path: Path) -> None:
        """Media type comes from the extension registry, not a hardcoded jpeg."""
        collection_dir = tmp_path / "roads"
        _write_collection(collection_dir)
        thumb = collection_dir / "preview.png"
        thumb.write_bytes(PNG_BYTES)

        register_collection_thumbnail(collection_dir, thumb)

        assert _read_assets(collection_dir)["thumbnail"]["type"] == "image/png"

    @pytest.mark.unit
    def test_href_for_an_item_thumbnail_stays_collection_relative(self, tmp_path: Path) -> None:
        """A raster collection points at an item's sidecar, one directory down."""
        collection_dir = tmp_path / "imagery"
        _write_collection(collection_dir)
        item_dir = collection_dir / "scene1"
        item_dir.mkdir()
        thumb = item_dir / "scene1.thumb.jpg"
        thumb.write_bytes(JPEG_BYTES)

        register_collection_thumbnail(collection_dir, thumb)

        assert _read_assets(collection_dir)["thumbnail"]["href"] == "./scene1/scene1.thumb.jpg"

    @pytest.mark.unit
    def test_missing_collection_json_is_a_no_op(self, tmp_path: Path) -> None:
        collection_dir = tmp_path / "roads"
        collection_dir.mkdir()
        thumb = collection_dir / "roads.thumb.jpg"
        thumb.write_bytes(JPEG_BYTES)

        register_collection_thumbnail(collection_dir, thumb)

        assert not (collection_dir / "collection.json").exists()


class TestOptOut:
    """Two ways to switch generation off, and neither may write anything."""

    @pytest.mark.unit
    def test_config_disabled_writes_nothing(self, tmp_path: Path) -> None:
        collection_dir = _vector_collection(tmp_path)
        (tmp_path / ".portolan").mkdir()
        (tmp_path / ".portolan" / "config.yaml").write_text(
            "thumbnails:\n  enabled: false\n", encoding="utf-8"
        )

        with patch("portolan_cli.collection_thumbnail.generate_vector_thumbnail") as render:
            ensure_collection_thumbnails(tmp_path, {"roads"})

        render.assert_not_called()
        assert _thumbnail_assets(collection_dir) == {}

    @pytest.mark.unit
    def test_generate_false_overrides_an_enabled_config(self, tmp_path: Path) -> None:
        """``--no-thumbnails`` wins over the catalog default for a single run."""
        collection_dir = _vector_collection(tmp_path)

        with patch("portolan_cli.collection_thumbnail.generate_vector_thumbnail") as render:
            ensure_collection_thumbnails(tmp_path, {"roads"}, generate=False)

        render.assert_not_called()
        assert _thumbnail_assets(collection_dir) == {}


class TestAdoption:
    """An image already on disk is adopted rather than overwritten."""

    @pytest.mark.unit
    def test_existing_thumbnail_asset_is_left_alone(self, tmp_path: Path) -> None:
        """A registered thumbnail — hand-made or from the PMTiles render — wins."""
        collection_dir = tmp_path / "roads"
        _write_collection(
            collection_dir,
            {
                "thumbnail": {
                    "href": "./curated.png",
                    "type": "image/png",
                    "roles": ["thumbnail"],
                    "title": "Hand-picked",
                }
            },
        )

        with patch("portolan_cli.collection_thumbnail.generate_vector_thumbnail") as render:
            ensure_collection_thumbnails(tmp_path, {"roads"})

        render.assert_not_called()
        assert _read_assets(collection_dir)["thumbnail"]["title"] == "Hand-picked"

    @pytest.mark.unit
    @pytest.mark.parametrize("filename", ["roads.thumb.jpg", "thumbnail.png", "preview.jpeg"])
    def test_adopts_a_conventional_image(self, tmp_path: Path, filename: str) -> None:
        """The names the CLI and the MapLibre skill actually write."""
        collection_dir = _vector_collection(tmp_path)
        (collection_dir / filename).write_bytes(JPEG_BYTES)

        with patch("portolan_cli.collection_thumbnail.generate_vector_thumbnail") as render:
            ensure_collection_thumbnails(tmp_path, {"roads"})

        render.assert_not_called()
        assert _read_assets(collection_dir)["thumbnail"]["href"] == f"./{filename}"

    @pytest.mark.unit
    def test_does_not_adopt_an_unconventional_image(self, tmp_path: Path) -> None:
        """A legend or logo PNG must never be promoted to the collection thumbnail."""
        collection_dir = _vector_collection(tmp_path)
        (collection_dir / "legend.png").write_bytes(PNG_BYTES)

        rendered = collection_dir / "roads.thumb.jpg"

        def _render(**_kwargs: Any) -> Path:
            rendered.write_bytes(JPEG_BYTES)
            return rendered

        with patch(
            "portolan_cli.collection_thumbnail.generate_vector_thumbnail", side_effect=_render
        ):
            ensure_collection_thumbnails(tmp_path, {"roads"})

        assert _read_assets(collection_dir)["thumbnail"]["href"] == "./roads.thumb.jpg"

    @pytest.mark.unit
    def test_does_not_adopt_an_image_another_asset_claims(self, tmp_path: Path) -> None:
        """A conventional name already registered under another role is off limits."""
        collection_dir = tmp_path / "roads"
        collection_dir.mkdir(parents=True)
        shutil.copy(GEOPARQUET_FIXTURE, collection_dir / "roads.parquet")
        (collection_dir / "preview.png").write_bytes(PNG_BYTES)
        _write_collection(
            collection_dir,
            {
                "roads": {
                    "href": "./roads.parquet",
                    "type": "application/vnd.apache.parquet",
                    "roles": ["data"],
                },
                "legend": {
                    "href": "./preview.png",
                    "type": "image/png",
                    "roles": ["legend"],
                },
            },
        )

        rendered = collection_dir / "roads.thumb.jpg"

        def _render(**_kwargs: Any) -> Path:
            rendered.write_bytes(JPEG_BYTES)
            return rendered

        with patch(
            "portolan_cli.collection_thumbnail.generate_vector_thumbnail", side_effect=_render
        ):
            ensure_collection_thumbnails(tmp_path, {"roads"})

        assets = _read_assets(collection_dir)
        assert assets["legend"]["roles"] == ["legend"], "The claimed image keeps its own role"
        assert assets["thumbnail"]["href"] == "./roads.thumb.jpg"


class TestRasterCollections:
    """Rasters reference the item sidecar that add already generates (#657)."""

    @pytest.mark.unit
    def test_references_an_item_thumbnail_without_rendering(self, tmp_path: Path) -> None:
        collection_dir = tmp_path / "imagery"
        item_dir = collection_dir / "scene1"
        item_dir.mkdir(parents=True)
        (item_dir / "scene1.tif").write_bytes(b"II*\x00 fake-tiff")
        (item_dir / "scene1.thumb.jpg").write_bytes(JPEG_BYTES)
        (item_dir / "scene1.json").write_text(
            json.dumps(
                {
                    "type": "Feature",
                    "id": "scene1",
                    "geometry": {"type": "Point", "coordinates": [0, 0]},
                    "assets": {
                        "data": {"href": "./scene1.tif", "type": "image/tiff"},
                        "thumbnail": {
                            "href": "./scene1.thumb.jpg",
                            "type": "image/jpeg",
                            "roles": ["thumbnail"],
                        },
                    },
                }
            ),
            encoding="utf-8",
        )
        _write_collection(collection_dir)
        data = json.loads((collection_dir / "collection.json").read_text(encoding="utf-8"))
        data["links"] = [{"rel": "item", "href": "./scene1/scene1.json"}]
        (collection_dir / "collection.json").write_text(json.dumps(data), encoding="utf-8")

        with patch("portolan_cli.collection_thumbnail.generate_vector_thumbnail") as render:
            ensure_collection_thumbnails(tmp_path, {"imagery"})

        render.assert_not_called()
        assert _read_assets(collection_dir)["thumbnail"]["href"] == "./scene1/scene1.thumb.jpg"


class TestVectorRendering:
    """The render path, and the promise that it can never fail an add."""

    @pytest.mark.unit
    def test_renders_and_registers(self, tmp_path: Path) -> None:
        collection_dir = _vector_collection(tmp_path)
        rendered = collection_dir / "roads.thumb.jpg"

        def _render(**_kwargs: Any) -> Path:
            rendered.write_bytes(JPEG_BYTES)
            return rendered

        with patch(
            "portolan_cli.collection_thumbnail.generate_vector_thumbnail", side_effect=_render
        ) as render:
            ensure_collection_thumbnails(tmp_path, {"roads"})

        render.assert_called_once()
        assert _read_assets(collection_dir)["thumbnail"]["href"] == "./roads.thumb.jpg"

    @pytest.mark.unit
    def test_tracks_the_rendered_file_in_versions_json(self, tmp_path: Path) -> None:
        """An untracked derived asset breaks push (#519, #735)."""
        from portolan_cli.versions import read_versions

        collection_dir = _vector_collection(tmp_path)
        rendered = collection_dir / "roads.thumb.jpg"

        def _render(**_kwargs: Any) -> Path:
            rendered.write_bytes(JPEG_BYTES)
            return rendered

        with patch(
            "portolan_cli.collection_thumbnail.generate_vector_thumbnail", side_effect=_render
        ):
            ensure_collection_thumbnails(tmp_path, {"roads"})

        versions = read_versions(collection_dir / "versions.json")
        assert "roads.thumb.jpg" in versions.versions[-1].assets

    @pytest.mark.unit
    def test_adoption_records_one_version(self, tmp_path: Path) -> None:
        """Adopting a file still records it: versions.json is the source of truth.

        This asserted the opposite until the adopted image was found to reach the
        remote through push's directory walk while staying invisible to every
        check that reads ``current_version.assets``.
        """
        collection_dir = _vector_collection(tmp_path)
        (collection_dir / "preview.png").write_bytes(PNG_BYTES)

        ensure_collection_thumbnails(tmp_path, {"roads"})

        versions = json.loads((collection_dir / "versions.json").read_text(encoding="utf-8"))
        assert len(versions["versions"]) == 1
        assert sorted(versions["versions"][-1]["assets"]) == ["preview.png"]

    @pytest.mark.unit
    def test_render_failure_warns_and_does_not_raise(self, tmp_path: Path) -> None:
        collection_dir = _vector_collection(tmp_path)

        with patch(
            "portolan_cli.collection_thumbnail.generate_vector_thumbnail",
            side_effect=RuntimeError("matplotlib exploded"),
        ):
            ensure_collection_thumbnails(tmp_path, {"roads"})

        assert _thumbnail_assets(collection_dir) == {}

    @pytest.mark.unit
    def test_render_returning_none_registers_nothing(self, tmp_path: Path) -> None:
        collection_dir = _vector_collection(tmp_path)

        with patch(
            "portolan_cli.collection_thumbnail.generate_vector_thumbnail", return_value=None
        ):
            ensure_collection_thumbnails(tmp_path, {"roads"})

        assert _thumbnail_assets(collection_dir) == {}


class TestNonGeospatialCollections:
    """PTL-VIZ-001 skips tabular collections, so generation must skip them too."""

    @pytest.mark.unit
    def test_tabular_collection_gets_no_thumbnail(self, tmp_path: Path) -> None:
        import pyarrow as pa
        import pyarrow.parquet as pq

        collection_dir = tmp_path / "demographics"
        collection_dir.mkdir(parents=True)
        pq.write_table(pa.table({"population": [1, 2, 3]}), collection_dir / "census.parquet")
        _write_collection(
            collection_dir,
            {
                "census": {
                    "href": "./census.parquet",
                    "type": "application/vnd.apache.parquet",
                    "roles": ["data"],
                }
            },
        )

        with patch("portolan_cli.collection_thumbnail.generate_vector_thumbnail") as render:
            ensure_collection_thumbnails(tmp_path, {"demographics"})

        render.assert_not_called()
        assert _thumbnail_assets(collection_dir) == {}

    @pytest.mark.unit
    def test_missing_collection_is_skipped(self, tmp_path: Path) -> None:
        """A collection id with no collection.json on disk must not raise."""
        ensure_collection_thumbnails(tmp_path, {"ghost"})


@pytest.mark.unit
class TestSymlinkedCatalogRoot:
    """A symlink anywhere in the path must not silently drop the registration.

    ``_item_paths`` resolves its candidates. Comparing one resolved path against
    an unresolved collection directory raised ValueError, so the raster branch
    wrote nothing while still reporting a thumbnail. macOS hits this on every
    temp directory, where ``/var`` is a symlink to ``/private/var``.
    """

    def test_item_sidecar_registers_through_a_symlink(self, tmp_path: Path) -> None:
        real = tmp_path / "real"
        collection_dir = real / "imagery"
        item_dir = collection_dir / "dem-2020"
        item_dir.mkdir(parents=True)
        (item_dir / "dem.thumb.jpg").write_bytes(JPEG_BYTES)
        (item_dir / "item.json").write_text(
            json.dumps(
                {
                    "type": "Feature",
                    "id": "dem-2020",
                    "geometry": {"type": "Point", "coordinates": [0, 0]},
                    "assets": {
                        "thumbnail": {"href": "./dem.thumb.jpg", "roles": ["thumbnail"]},
                    },
                }
            ),
            encoding="utf-8",
        )
        _write_collection(
            collection_dir,
            {"cog": {"href": "./dem-2020/dem.tif", "roles": ["data"]}},
        )
        (item_dir / "dem.tif").write_bytes(b"II*\x00 fake-tif")
        data = json.loads((collection_dir / "collection.json").read_text(encoding="utf-8"))
        data["links"] = [{"rel": "item", "href": "./dem-2020/item.json"}]
        (collection_dir / "collection.json").write_text(json.dumps(data), encoding="utf-8")

        link = tmp_path / "catalog"
        link.symlink_to(real, target_is_directory=True)

        ensure_collection_thumbnails(link, {"imagery"})

        thumbnails = _thumbnail_assets(collection_dir)
        assert thumbnails, "A symlinked catalog root must still register the sidecar"
        assert thumbnails["thumbnail"]["href"] == "./dem-2020/dem.thumb.jpg"

    def test_an_href_escaping_the_collection_is_still_rejected(self, tmp_path: Path) -> None:
        """Resolving both sides must not weaken the containment check."""
        outside = tmp_path / "outside.png"
        outside.write_bytes(PNG_BYTES)
        collection_dir = tmp_path / "roads"
        _write_collection(collection_dir)

        assert register_collection_thumbnail(collection_dir, outside) is False
        assert _thumbnail_assets(collection_dir) == {}


@pytest.mark.unit
class TestConfigOptOutOnEveryEntryPoint:
    """``thumbnails.enabled: false`` binds the fixer path too.

    ``check --fix`` calls the singular entry point. Only the plural wrapper read
    the setting, so a catalog that opted out got thumbnails anyway.
    """

    def test_disabled_config_blocks_the_singular_entry_point(self, tmp_path: Path) -> None:
        """The adoption branch is the reachable hole.

        ``generate_vector_thumbnail`` already refuses when the config is off, so
        the render branch was covered by accident. Adoption never reaches the
        renderer, so a disabled catalog still got a thumbnail asset from it.
        """
        from portolan_cli.collection_thumbnail import ensure_collection_thumbnail

        (tmp_path / ".portolan").mkdir()
        (tmp_path / ".portolan" / "config.yaml").write_text(
            "thumbnails:\n  enabled: false\n", encoding="utf-8"
        )
        collection_dir = _vector_collection(tmp_path)
        (collection_dir / "thumbnail.png").write_bytes(PNG_BYTES)

        assert ensure_collection_thumbnail(collection_dir, tmp_path) is None
        assert _thumbnail_assets(collection_dir) == {}

    def test_an_explicit_generate_overrides_a_disabled_config(self, tmp_path: Path) -> None:
        from portolan_cli.collection_thumbnail import ensure_collection_thumbnail

        (tmp_path / ".portolan").mkdir()
        (tmp_path / ".portolan" / "config.yaml").write_text(
            "thumbnails:\n  enabled: false\n", encoding="utf-8"
        )
        collection_dir = _vector_collection(tmp_path)
        (collection_dir / "thumbnail.png").write_bytes(PNG_BYTES)

        assert ensure_collection_thumbnail(collection_dir, tmp_path, generate=True) is not None
        assert _thumbnail_assets(collection_dir)


@pytest.mark.unit
class TestForceRefresh:
    """Without a force, the first render is the catalog's thumbnail forever."""

    def test_force_redraws_a_generated_thumbnail(self, tmp_path: Path) -> None:
        collection_dir = _vector_collection(tmp_path)
        ensure_collection_thumbnails(tmp_path, {"roads"})
        rendered = collection_dir / "roads.thumb.jpg"
        assert rendered.exists()
        rendered.write_bytes(b"stale")

        ensure_collection_thumbnails(tmp_path, {"roads"}, force=True)

        assert rendered.read_bytes() != b"stale", "force must redraw, not re-adopt"
        asset = _thumbnail_assets(collection_dir)["thumbnail"]
        assert asset["file:size"] == rendered.stat().st_size

    def test_without_force_a_registered_thumbnail_is_left_alone(self, tmp_path: Path) -> None:
        collection_dir = _vector_collection(tmp_path)
        ensure_collection_thumbnails(tmp_path, {"roads"})
        rendered = collection_dir / "roads.thumb.jpg"
        rendered.write_bytes(b"stale")

        ensure_collection_thumbnails(tmp_path, {"roads"})

        assert rendered.read_bytes() == b"stale"

    def test_force_keeps_a_thumbnail_the_user_supplied(self, tmp_path: Path) -> None:
        collection_dir = _vector_collection(tmp_path)
        (collection_dir / "thumbnail.png").write_bytes(PNG_BYTES)
        ensure_collection_thumbnails(tmp_path, {"roads"})

        ensure_collection_thumbnails(tmp_path, {"roads"}, force=True)

        assert (collection_dir / "thumbnail.png").read_bytes() == PNG_BYTES
        assert _thumbnail_assets(collection_dir)["thumbnail"]["href"] == "./thumbnail.png"


@pytest.mark.unit
class TestEveryRegisteredThumbnailIsTracked:
    """versions.json is the source of truth, so an adopted image belongs in it."""

    def test_an_adopted_image_lands_in_versions_json(self, tmp_path: Path) -> None:
        collection_dir = _vector_collection(tmp_path)
        (collection_dir / "thumbnail.png").write_bytes(PNG_BYTES)

        ensure_collection_thumbnails(tmp_path, {"roads"})

        versions = json.loads((collection_dir / "versions.json").read_text(encoding="utf-8"))
        assert "thumbnail.png" in versions["versions"][-1]["assets"]

    def test_a_second_add_creates_no_further_version(self, tmp_path: Path) -> None:
        collection_dir = _vector_collection(tmp_path)
        (collection_dir / "thumbnail.png").write_bytes(PNG_BYTES)
        ensure_collection_thumbnails(tmp_path, {"roads"})
        first = json.loads((collection_dir / "versions.json").read_text(encoding="utf-8"))

        ensure_collection_thumbnails(tmp_path, {"roads"})

        second = json.loads((collection_dir / "versions.json").read_text(encoding="utf-8"))
        assert first["current_version"] == second["current_version"]


@pytest.mark.unit
class TestFileExtensionDeclaration:
    """``file:`` fields oblige the collection to declare the file extension.

    The spec extension registry makes the declaration a conditional MUST once an
    asset carries ``file:size`` or ``file:checksum`` (Issue #654). The two writers
    that already sync it — ``declare_file_extension`` and
    ``stac_parquet.sync_file_extension`` — both run before the thumbnail
    side-step, so a collection whose only ``file:``-bearing asset is the
    thumbnail used to end up with the fields and no declaration.
    """

    FILE_EXT = "https://stac-extensions.github.io/file/v2.1.0/schema.json"

    def _extensions(self, collection_dir: Path) -> list[str]:
        data = json.loads((collection_dir / "collection.json").read_text(encoding="utf-8"))
        declared: list[str] = data.get("stac_extensions", [])
        return declared

    def test_a_thumbnail_only_collection_declares_the_extension(self, tmp_path: Path) -> None:
        collection_dir = tmp_path / "roads"
        _write_collection(collection_dir)
        thumb = collection_dir / "roads.thumb.jpg"
        thumb.write_bytes(JPEG_BYTES)

        register_collection_thumbnail(collection_dir, thumb)

        assert self._extensions(collection_dir).count(self.FILE_EXT) == 1

    def test_registering_twice_declares_it_once(self, tmp_path: Path) -> None:
        collection_dir = tmp_path / "roads"
        _write_collection(collection_dir)
        thumb = collection_dir / "roads.thumb.jpg"
        thumb.write_bytes(JPEG_BYTES)

        register_collection_thumbnail(collection_dir, thumb)
        register_collection_thumbnail(collection_dir, thumb)

        assert self._extensions(collection_dir).count(self.FILE_EXT) == 1

    def test_an_existing_declaration_is_not_duplicated(self, tmp_path: Path) -> None:
        """The mirror asset already declared it; the thumbnail must not re-add it."""
        collection_dir = tmp_path / "roads"
        _write_collection(
            collection_dir,
            {
                "items-parquet": {
                    "href": "./items.parquet",
                    "type": "application/vnd.apache.parquet",
                    "roles": ["stac-items", "collection-mirror"],
                    "file:size": 12,
                    "file:checksum": "1220" + "ab" * 32,
                }
            },
        )
        data = json.loads((collection_dir / "collection.json").read_text(encoding="utf-8"))
        data["stac_extensions"] = [self.FILE_EXT]
        (collection_dir / "collection.json").write_text(json.dumps(data), encoding="utf-8")
        thumb = collection_dir / "roads.thumb.jpg"
        thumb.write_bytes(JPEG_BYTES)

        register_collection_thumbnail(collection_dir, thumb)

        assert self._extensions(collection_dir) == [self.FILE_EXT]

    def test_the_full_ladder_leaves_the_declaration_in_place(self, tmp_path: Path) -> None:
        """The wired entry point, not just the writer, produces a valid collection."""
        collection_dir = _vector_collection(tmp_path)

        ensure_collection_thumbnails(tmp_path, {"roads"})

        assert "file:size" in _thumbnail_assets(collection_dir)["thumbnail"]
        assert self._extensions(collection_dir).count(self.FILE_EXT) == 1


@pytest.mark.unit
class TestOneAddIsOneVersion:
    """A side-step's derived asset folds into the snapshot ``add`` just wrote.

    Thumbnails joined the default pipeline, so a second version per add would
    double every collection's history. PMTiles never showed this because it is
    gated on ``--pmtiles`` (Issue #519).
    """

    def _seed_version(self, collection_dir: Path, catalog_root: Path) -> None:
        """Stand in for the snapshot ``add`` writes before the side-steps run."""
        from portolan_cli.versions import track_generated_assets

        track_generated_assets(
            collection_dir,
            [collection_dir / "roads.parquet"],
            catalog_root,
            message="Added roads.parquet",
        )

    def test_a_versioned_collection_gains_no_second_version(self, tmp_path: Path) -> None:
        collection_dir = _vector_collection(tmp_path)
        self._seed_version(collection_dir, tmp_path)

        ensure_collection_thumbnails(tmp_path, {"roads"}, versioned_collections={"roads"})

        versions = json.loads((collection_dir / "versions.json").read_text(encoding="utf-8"))
        assert len(versions["versions"]) == 1
        assert versions["current_version"] == "1.0.0"
        latest = versions["versions"][-1]
        assert sorted(latest["assets"]) == ["roads.parquet", "roads.thumb.jpg"]
        assert "roads.thumb.jpg" in latest["changes"]

    def test_a_skipped_collection_gets_its_own_version(self, tmp_path: Path) -> None:
        """Backfill must not edit a snapshot that may already be published."""
        collection_dir = _vector_collection(tmp_path)
        self._seed_version(collection_dir, tmp_path)

        ensure_collection_thumbnails(tmp_path, {"roads"}, versioned_collections=set())

        versions = json.loads((collection_dir / "versions.json").read_text(encoding="utf-8"))
        assert [v["version"] for v in versions["versions"]] == ["1.0.0", "1.0.1"]
        assert sorted(versions["versions"][0]["assets"]) == ["roads.parquet"]
