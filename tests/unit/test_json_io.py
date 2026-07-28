"""Unit tests for the shared atomic JSON writer (``portolan_cli.json_io``).

Every STAC/Portolan JSON write funnels through :func:`write_json_atomic`, so the
guarantees tested here — atomicity, unescaped unicode, trailing newline — hold
for ``catalog.json``, ``collection.json``, ``item.json``, and ``versions.json``
alike.
"""

from __future__ import annotations

import json
from pathlib import Path

import pytest

from portolan_cli.json_io import write_json_atomic

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
        real_dumps = json.dumps

        def _spy_dumps(obj: object, **kwargs: object) -> str:
            seen.extend(_temp_siblings(tmp_path))
            return real_dumps(obj, **kwargs)  # type: ignore[arg-type]

        monkeypatch.setattr("portolan_cli.json_io.json.dumps", _spy_dumps)
        write_json_atomic(target, {"id": "root"})

        assert len(seen) == 1, "expected exactly one temp file to exist mid-write"
        assert seen[0].parent == tmp_path


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
