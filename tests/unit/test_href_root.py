"""Unit tests for the pystac href-root helper."""

from __future__ import annotations

import re
from pathlib import Path

import pytest

from portolan_cli.utils import href_root
from tests.source_roots import CORE_ROOT, REPO_ROOT, SOURCE_ROOTS


def _sources() -> list[Path]:
    """The Python files of both source folders."""
    return sorted(path for root in SOURCE_ROOTS for path in root.rglob("*.py"))


class TestHrefRoot:
    """The two properties pystac needs, on every input shape."""

    @pytest.mark.unit
    def test_ends_with_a_trailing_slash(self, tmp_path: Path) -> None:
        """Without the slash, pystac reads a dotted directory as a file."""
        assert href_root(tmp_path / "my.catalog").endswith("/")

    @pytest.mark.unit
    def test_absolutizes_a_relative_path(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        working_dir = tmp_path / "tmp.relative"
        working_dir.mkdir()
        monkeypatch.chdir(working_dir)

        assert href_root(Path()) == f"{working_dir.resolve()}/"

    @pytest.mark.unit
    def test_preserves_a_dotted_final_component(self, tmp_path: Path) -> None:
        """Resolving must not strip the dotted name pystac trips over."""
        dotted = tmp_path / "tmp.ckdIXs70TT"

        assert href_root(dotted) == f"{dotted.resolve()}/"

    @pytest.mark.unit
    def test_does_not_double_the_slash(self, tmp_path: Path) -> None:
        """A caller that already passes a trailing slash gets one, not two."""
        result = href_root(Path(f"{tmp_path}/"))

        assert result == f"{tmp_path.resolve()}/"
        assert not result.endswith("//")


class TestEveryCallSiteUsesTheHelper:
    """The 'fix every parallel call site' rule, made executable."""

    @pytest.mark.unit
    def test_no_normalize_hrefs_call_bypasses_href_root(self) -> None:
        offenders: list[str] = []

        for source in _sources():
            for lineno, line in enumerate(source.read_text(encoding="utf-8").splitlines(), start=1):
                if not re.search(r"\.normalize_hrefs\(", line):
                    continue
                if "href_root(" not in line:
                    rel = source.relative_to(REPO_ROOT)
                    offenders.append(f"{rel}:{lineno}: {line.strip()}")

        assert offenders == [], (
            "normalize_hrefs must take its root from href_root(), which "
            "guarantees the absolute path and trailing slash pystac needs:\n" + "\n".join(offenders)
        )

    @pytest.mark.unit
    def test_the_guard_sees_the_known_call_sites(self) -> None:
        call_sites = [
            f"{source.relative_to(REPO_ROOT)}:{lineno}"
            for source in _sources()
            for lineno, line in enumerate(source.read_text(encoding="utf-8").splitlines(), start=1)
            if re.search(r"\.normalize_hrefs\(", line)
        ]

        assert len(call_sites) == 2, f"expected 2 normalize_hrefs call sites, found {call_sites}"

    @pytest.mark.unit
    def test_the_scan_reads_both_packages(self) -> None:
        assert CORE_ROOT / "stac_links.py" in _sources()
