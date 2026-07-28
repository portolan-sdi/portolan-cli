"""Unit tests for the shared atomic JSON writer (``portolan_cli.json_io``).

Every STAC/Portolan JSON write funnels through :func:`write_json_atomic`, so the
guarantees tested here — atomicity, unescaped unicode, trailing newline — hold
for ``catalog.json``, ``collection.json``, ``item.json``, and ``versions.json``
alike.
"""

from __future__ import annotations

import json
import os
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
