"""Unit tests for the shared atomic JSON writer (``portolan_cli.json_io``).

Every STAC/Portolan JSON write funnels through :func:`write_json_atomic`, so the
guarantees tested here — atomicity, unescaped unicode, trailing newline — hold
for ``catalog.json``, ``collection.json``, ``item.json``, and ``versions.json``
alike.
"""

from __future__ import annotations

import json
import os
from datetime import datetime, timezone
from pathlib import Path

import pytest

from portolan_cli.json_io import write_json_atomic, write_text_atomic

pytestmark = pytest.mark.unit


def _temp_siblings(directory: Path) -> list[Path]:
    """Temp files the writer may have left behind in ``directory``."""
    return [p for p in directory.iterdir() if p.name.endswith(".tmp")]


class TestWriteJsonAtomic:
    def test_writes_json_with_indent_and_trailing_newline(self, tmp_path: Path) -> None:
        target = tmp_path / "collection.json"

        write_json_atomic(target, {"id": "roads", "links": []})

        text = target.read_text(encoding="utf-8")
        assert text.endswith("\n")
        assert text == json.dumps({"id": "roads", "links": []}, indent=2) + "\n"

    def test_preserves_non_ascii_literally(self, tmp_path: Path) -> None:
        target = tmp_path / "collection.json"

        write_json_atomic(target, {"title": "Córdoba"})

        raw = target.read_bytes().decode("utf-8")
        assert "Córdoba" in raw
        assert "\\u00f3" not in raw

    def test_creates_parent_directories(self, tmp_path: Path) -> None:
        target = tmp_path / "deep" / "nested" / "catalog.json"

        write_json_atomic(target, {"id": "root"})

        assert json.loads(target.read_text(encoding="utf-8")) == {"id": "root"}

    def test_leaves_no_temp_file_after_success(self, tmp_path: Path) -> None:
        target = tmp_path / "catalog.json"

        write_json_atomic(target, {"id": "root"})

        assert _temp_siblings(tmp_path) == []

    def test_failed_write_leaves_no_temp_file_and_preserves_prior_content(
        self, tmp_path: Path
    ) -> None:
        target = tmp_path / "catalog.json"
        write_json_atomic(target, {"id": "root"})
        before = target.read_text(encoding="utf-8")

        with pytest.raises(TypeError):
            write_json_atomic(target, {"bad": object()})

        assert target.read_text(encoding="utf-8") == before
        assert _temp_siblings(tmp_path) == []

    def test_temp_file_lives_in_the_destination_directory(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """Same-directory temp file is what makes ``os.replace`` atomic."""
        target = tmp_path / "catalog.json"
        seen: list[Path] = []
        real_fdopen = os.fdopen

        # Serialization happens before the temp file exists, so probe at
        # os.fdopen — the moment the freshly created temp file is opened.
        def _spy_fdopen(fd: int, *args: object, **kwargs: object) -> object:
            seen.extend(_temp_siblings(tmp_path))
            return real_fdopen(fd, *args, **kwargs)  # type: ignore[arg-type]

        monkeypatch.setattr("portolan_cli.json_io.os.fdopen", _spy_fdopen)
        write_json_atomic(target, {"id": "root"})

        assert len(seen) == 1, "expected exactly one temp file to exist mid-write"
        assert seen[0].parent == tmp_path


class TestWriteTextAtomic:
    """The text variant behind config.yaml and other non-JSON writes."""

    @pytest.mark.unit
    def test_writes_content_verbatim_utf8(self, tmp_path: Path) -> None:
        target = tmp_path / "config.yaml"
        write_text_atomic(target, "# Configuración\nbackend: file\n")

        raw = target.read_bytes().decode("utf-8")
        assert raw == "# Configuración\nbackend: file\n"

    @pytest.mark.unit
    def test_leaves_no_temp_file_behind(self, tmp_path: Path) -> None:
        target = tmp_path / "config.yaml"
        write_text_atomic(target, "a: 1\n")

        assert [p.name for p in tmp_path.iterdir()] == ["config.yaml"]

    @pytest.mark.unit
    def test_prior_content_survives_a_failed_write(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        target = tmp_path / "config.yaml"
        target.write_text("original\n", encoding="utf-8")

        def _boom(src: str, dst: Path) -> None:
            raise OSError("disk full")

        monkeypatch.setattr("portolan_cli.json_io.os.replace", _boom)
        with pytest.raises(OSError, match="disk full"):
            write_text_atomic(target, "replacement\n")

        assert target.read_text(encoding="utf-8") == "original\n"
        assert [p.name for p in tmp_path.iterdir()] == ["config.yaml"]


def _collection_dict(collection_id: str = "roads", title: str = "Roads") -> dict[str, object]:
    return {
        "type": "Collection",
        "stac_version": "1.1.0",
        "id": collection_id,
        "title": title,
        "description": f"{title} collection.",
        "license": "CC-BY-4.0",
        "extent": {
            "spatial": {"bbox": [[-1.0, -1.0, 1.0, 1.0]]},
            "temporal": {"interval": [[None, None]]},
        },
        "links": [],
        "assets": {},
        "stac_extensions": [],
    }


class TestAdoptedWriteSites:
    """Round-trip proof that adopting the helper keeps non-ASCII literal."""

    def test_ensure_readmes_preserves_accented_titles(self, tmp_path: Path) -> None:
        from portolan_cli.readme import ensure_readmes

        collection = {
            "type": "Collection",
            "stac_version": "1.1.0",
            "id": "cordoba",
            "title": "Córdoba",
            "description": "Municipios de Córdoba.",
            "license": "CC-BY-4.0",
            "extent": {
                "spatial": {"bbox": [[-1.0, -1.0, 1.0, 1.0]]},
                "temporal": {"interval": [[None, None]]},
            },
            "links": [],
            "assets": {},
        }
        target = tmp_path / "cordoba" / "collection.json"
        target.parent.mkdir(parents=True)
        target.write_text(json.dumps(collection), encoding="utf-8")

        assert ensure_readmes(tmp_path) is True

        raw = target.read_bytes().decode("utf-8")
        assert '"title": "Córdoba"' in raw
        assert "\\u00f3" not in raw
        assert raw.endswith("\n")


def _fail_replace(monkeypatch: pytest.MonkeyPatch) -> None:
    """Make the rename half of every atomic write fail, as a full disk would."""

    def _boom(src: str, dst: object) -> None:
        raise OSError("disk full")

    monkeypatch.setattr("portolan_cli.json_io.os.replace", _boom)


class TestSitesAdoptedInIssue687:
    """The write sites converted in #687 cannot truncate what was already there.

    Each test kills the write at ``os.replace`` — the last step, after the new
    bytes are on disk — because that is the window an in-place writer cannot
    survive: it has already truncated the destination by then.
    """

    def test_pmtiles_extension_write_preserves_the_prior_collection(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        from portolan_cli.viz.pmtiles import ensure_web_map_links_extension

        collection_dir = tmp_path / "roads"
        collection_dir.mkdir()
        target = collection_dir / "collection.json"
        write_json_atomic(target, _collection_dict())
        before = target.read_text(encoding="utf-8")

        _fail_replace(monkeypatch)
        with pytest.raises(OSError, match="disk full"):
            ensure_web_map_links_extension(collection_dir)

        assert target.read_text(encoding="utf-8") == before
        assert _temp_siblings(collection_dir) == []

    def test_style_file_write_preserves_the_prior_style(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        from portolan_cli.viz.style import write_style_file

        style_dir = tmp_path / "styles"
        style_dir.mkdir()
        target = style_dir / "default.json"
        write_json_atomic(target, {"version": 8, "layers": []})
        before = target.read_text(encoding="utf-8")

        _fail_replace(monkeypatch)
        with pytest.raises(OSError, match="disk full"):
            write_style_file(style_dir, "default", {"version": 8, "layers": [{"id": "roads"}]})

        assert target.read_text(encoding="utf-8") == before
        assert _temp_siblings(style_dir) == []

    def test_resume_state_write_preserves_the_prior_state(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """The extractor kept its own temp-and-rename; it now shares the helper."""
        from portolan_cli.extract.arcgis.imageserver.extractor import _save_resume_state
        from portolan_cli.extract.arcgis.imageserver.resume import ImageServerResumeState

        target = tmp_path / "resume.json"
        started = datetime(2026, 7, 30, 9, 0, tzinfo=timezone.utc)
        _save_resume_state(
            ImageServerResumeState(
                succeeded_tiles={(0, 0)},
                failed_tiles=set(),
                service_url="https://example.org/ImageServer",
                started_at=started,
            ),
            target,
        )
        before = target.read_text(encoding="utf-8")

        _fail_replace(monkeypatch)
        with pytest.raises(OSError, match="disk full"):
            _save_resume_state(
                ImageServerResumeState(
                    succeeded_tiles={(0, 0), (0, 1)},
                    failed_tiles=set(),
                    service_url="https://example.org/ImageServer",
                    started_at=started,
                ),
                target,
            )

        assert json.loads(target.read_text(encoding="utf-8"))["tiles"]["succeeded"] == [[0, 0]]
        assert target.read_text(encoding="utf-8") == before
        assert _temp_siblings(tmp_path) == []

    def test_collection_json_write_preserves_the_prior_collection(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        from portolan_cli.collection import write_collection_json
        from portolan_cli.models.collection import (
            CollectionModel,
            ExtentModel,
            SpatialExtent,
            TemporalExtent,
        )

        target = tmp_path / "collection.json"
        write_json_atomic(target, _collection_dict())
        before = target.read_text(encoding="utf-8")

        replacement = CollectionModel(
            id="roads",
            description="Replacement.",
            extent=ExtentModel(
                spatial=SpatialExtent(bbox=[[-1.0, -1.0, 1.0, 1.0]]),
                temporal=TemporalExtent(interval=[[None, None]]),
            ),
        )
        _fail_replace(monkeypatch)
        with pytest.raises(OSError, match="disk full"):
            write_collection_json(replacement, tmp_path)

        assert target.read_text(encoding="utf-8") == before
        assert _temp_siblings(tmp_path) == []
