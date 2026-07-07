"""Discovery must prune FileGDB subtrees during traversal, not post-filter.

Issue #590: ``iter_geospatial_files`` used ``Path.rglob("*")``, which descends
into and enumerates every internal file of a ``.gdb`` directory before a
post-filter discards them. FileGDBs hold thousands of internal files, so on
catalogs with large geodatabases this wasted walk dominates discovery time.

The traversal must stop at the ``.gdb`` boundary: once a FileGDB directory is
recognized as an asset, its contents must never be enumerated. These tests
assert the pruning behaviorally (the ``.gdb`` directory is never listed) and
confirm the discovery results are unchanged.
"""

from __future__ import annotations

import os
from collections import Counter
from pathlib import Path

import pytest

from portolan_cli.discovery import iter_files_with_sidecars, iter_geospatial_files


def _make_filegdb(parent: Path, name: str, *, internal_files: int = 5) -> Path:
    """Create a minimal but internally-populated FileGDB directory."""
    gdb = parent / name
    gdb.mkdir()
    (gdb / "gdb").write_bytes(b"\x00")  # FileGDB marker file
    for i in range(internal_files):
        (gdb / f"a{i:08d}.gdbtable").write_bytes(b"\x00")
        (gdb / f"a{i:08d}.gdbtablx").write_bytes(b"\x00")
        (gdb / f"a{i:08d}.spx").write_bytes(b"\x00")
    return gdb


def _count_scandir_calls(monkeypatch: pytest.MonkeyPatch) -> Counter[Path]:
    """Record how many times each directory is opened via ``os.scandir``.

    ``is_filegdb`` opens a candidate ``.gdb`` exactly once to classify it (it
    early-returns at the first marker, so that scan is O(1), not O(internals)).
    A traversal that also *descends* into the FileGDB opens it a second time to
    walk its children -- that second open is the wasted work issue #590 removes.
    So the discriminating signal is: a FileGDB must be scandir'd at most once.
    """
    calls: Counter[Path] = Counter()
    real_scandir = os.scandir

    def spy(path=".", /):  # type: ignore[no-untyped-def]
        calls[Path(path)] += 1
        return real_scandir(path)  # preserve the context-manager return

    monkeypatch.setattr(os, "scandir", spy)
    return calls


@pytest.mark.unit
def test_filegdb_scanned_once_not_descended_into(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """A .gdb is opened only to classify it, never a second time to walk it."""
    gdb = _make_filegdb(tmp_path, "big.gdb", internal_files=25)
    (tmp_path / "roads.shp").write_bytes(b"\x00")  # sibling, so we still walk

    calls = _count_scandir_calls(monkeypatch)

    iter_geospatial_files(tmp_path)

    # Exactly one open (classification). Zero would mean an inert spy; two would
    # mean the traversal descended into the FileGDB to walk its internals.
    assert calls[gdb] == 1, f"FileGDB was opened {calls[gdb]}x (expected 1: classify-only)"


@pytest.mark.unit
def test_nested_filegdb_scanned_once_not_descended_into(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Pruning also applies to a .gdb nested under a subdirectory."""
    sub = tmp_path / "sub"
    sub.mkdir()
    gdb = _make_filegdb(sub, "nested.gdb", internal_files=25)

    calls = _count_scandir_calls(monkeypatch)

    iter_geospatial_files(tmp_path)

    assert calls[gdb] == 1, f"nested FileGDB was opened {calls[gdb]}x (expected 1: classify-only)"


@pytest.mark.unit
def test_filegdb_scan_cost_independent_of_internal_count(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Discovery cost over a .gdb must not grow with its internal file count.

    Honors the issue's "many internal files" acceptance criterion: a tiny and a
    large FileGDB must be opened the same number of times (once). Under the old
    ``rglob`` walk the descent materialized every internal entry, so cost scaled
    with the internal count; pruning makes it constant.
    """
    small_root = tmp_path / "s"
    small_root.mkdir()
    small_gdb = _make_filegdb(small_root, "small.gdb", internal_files=2)

    large_root = tmp_path / "l"
    large_root.mkdir()
    large_gdb = _make_filegdb(large_root, "large.gdb", internal_files=500)

    calls_small = _count_scandir_calls(monkeypatch)
    iter_geospatial_files(small_root)
    small_opens = calls_small[small_gdb]

    calls_large = _count_scandir_calls(monkeypatch)
    iter_geospatial_files(large_root)
    large_opens = calls_large[large_gdb]

    assert small_opens == large_opens == 1, (
        f"scan cost scaled with internals: small={small_opens}, large={large_opens}"
    )


@pytest.mark.unit
def test_results_unchanged_with_nested_filegdb(tmp_path: Path) -> None:
    """Pruning must not change which assets are discovered, at any depth."""
    top_gdb = _make_filegdb(tmp_path, "top.gdb")
    sub = tmp_path / "sub"
    sub.mkdir()
    nested_gdb = _make_filegdb(sub, "nested.gdb")
    shp = sub / "roads.shp"
    shp.write_bytes(b"\x00")

    result = iter_geospatial_files(tmp_path)

    assert result == sorted([top_gdb, nested_gdb, shp])


@pytest.mark.unit
def test_sidecars_unchanged_with_filegdb(tmp_path: Path) -> None:
    """iter_files_with_sidecars still yields sibling sidecars, FileGDB pruned."""
    gdb = _make_filegdb(tmp_path, "data.gdb")
    shp = tmp_path / "roads.shp"
    shp.write_bytes(b"\x00")
    dbf = tmp_path / "roads.dbf"
    dbf.write_bytes(b"\x00")

    result = iter_files_with_sidecars(tmp_path)

    assert result == sorted([gdb, shp, dbf])
