"""Unit tests for the pystac href-root helper (issues #401, #731).

``normalize_hrefs`` decides between a file and a directory by looking for a dot
in the final path component, so it needs two things from every caller: a
trailing slash, and an already-absolute path. Miss either one and pystac writes
``catalog.json`` or ``collection.json`` into the parent directory.

``href_root`` is the single writer for that string. These tests pin its contract
and check that no call site bypasses it.
"""

from __future__ import annotations

import re
from pathlib import Path

import pytest

from portolan_cli.utils import href_root

SOURCE_ROOT = Path(__file__).resolve().parents[2] / "portolan_cli"


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
        """pystac drops the trailing slash while absolutizing a relative root.

        This is the issue #731 defect. Resolving first is what keeps the slash.
        """
        working_dir = tmp_path / "tmp.relative"
        working_dir.mkdir()
        monkeypatch.chdir(working_dir)

        assert href_root(Path(".")) == f"{working_dir.resolve()}/"

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
    """The 'fix every parallel call site' rule, made executable.

    .claude/rules/stac-assets.md records that the original trailing-slash fix
    shipped for one call site and missed its twins. This test fails when a new
    normalize_hrefs call builds its root string by hand.
    """

    @pytest.mark.unit
    def test_no_normalize_hrefs_call_bypasses_href_root(self) -> None:
        offenders: list[str] = []

        for source in SOURCE_ROOT.rglob("*.py"):
            for lineno, line in enumerate(source.read_text(encoding="utf-8").splitlines(), start=1):
                if not re.search(r"\.normalize_hrefs\(", line):
                    continue
                if "href_root(" not in line:
                    rel = source.relative_to(SOURCE_ROOT.parent)
                    offenders.append(f"{rel}:{lineno}: {line.strip()}")

        assert offenders == [], (
            "normalize_hrefs must take its root from href_root(), which "
            "guarantees the absolute path and trailing slash pystac needs:\n" + "\n".join(offenders)
        )

    @pytest.mark.unit
    def test_the_guard_sees_the_known_call_sites(self) -> None:
        """Guard against the scan silently matching nothing (issues #401, #731)."""
        call_sites = [
            f"{source.relative_to(SOURCE_ROOT.parent)}:{lineno}"
            for source in SOURCE_ROOT.rglob("*.py")
            for lineno, line in enumerate(source.read_text(encoding="utf-8").splitlines(), start=1)
            if re.search(r"\.normalize_hrefs\(", line)
        ]

        assert len(call_sites) == 4, f"expected 4 normalize_hrefs call sites, found {call_sites}"
