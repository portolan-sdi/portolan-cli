"""The conversion notices print as on f7f8214, with one worker and with several (#1133).

With ``--workers 2``, ``check --fix`` converts in spawned worker processes, and
``add`` in threads. Each notice keeps its text, its marker, and its stream. A
pool finishes the files in any order, so these tests sort the notices of a
parallel run. In JSON mode the notices go to stderr. On f7f8214 a spawned
worker wrote them to stdout before the envelope, which broke the JSON (#1005).
"""

from __future__ import annotations

import json
import shutil
from typing import TYPE_CHECKING

import pytest
from click.testing import CliRunner

from portolan_cli.cli import cli

if TYPE_CHECKING:
    from pathlib import Path

pytestmark = [pytest.mark.integration, pytest.mark.slow]

SIDECAR = "→ Encoding sidecar {}.cpg written: DBF records are not valid UTF-8"
REWRITE = "→ Rewriting {}.parquet (142.4KB): it carries no bbox covering column"
MODES = {
    "default": ([], []),
    "quiet": (["--quiet"], []),
    "verbose": ([], ["-v"]),
    "json": ([], ["--json"]),
}


def _shapefiles(directory: Path, names: list[str]) -> None:
    """Write one Latin-1 shapefile with no .cpg sidecar for each name."""
    import geopandas
    from shapely.geometry import Point

    directory.mkdir(parents=True, exist_ok=True)
    for name in names:
        shp = directory / f"{name}.shp"
        frame = geopandas.GeoDataFrame(
            {"name": ["Zürich"]}, geometry=[Point(8.5, 47.4)], crs="EPSG:4326"
        )
        frame.to_file(shp, encoding="latin1")
        shp.with_suffix(".cpg").unlink(missing_ok=True)


def _notices(text: str) -> list[str]:
    return [
        line for line in text.splitlines() if line.startswith("→ Encoding") or " Rewriting " in line
    ]


def _run(mode: str, *args: str) -> tuple[str, str]:
    root_flags, flags = MODES[mode]
    result = CliRunner().invoke(cli, [*root_flags, *args, *flags])
    assert result.exit_code == 0, result.output
    if mode == "json":
        assert json.loads(result.stdout)["success"] is True
    return result.stdout, result.stderr


@pytest.mark.parametrize("workers", ["1", "2"])
@pytest.mark.parametrize("mode", list(MODES))
def test_check_fix_prints_each_sidecar_notice(tmp_path: Path, mode: str, workers: str) -> None:
    """Each worker's sidecar notice reaches the terminal before the conversion summary."""
    names = ["alpha", "beta", "gamma"]
    _shapefiles(tmp_path / "data", names)

    out, err = _run(
        mode, "check", str(tmp_path / "data"), "--fix", "--geo-assets", "--workers", workers
    )

    expected = [SIDECAR.format(name) for name in names]
    shown, other = (err, out) if mode == "json" else (out, err)
    notices = _notices(shown)
    assert (notices if workers == "1" else sorted(notices)) == expected
    assert _notices(other) == []
    if mode != "json":
        assert out.index(expected[-1] if workers == "1" else sorted(notices)[-1]) < out.index(
            "✓ Converted 3 file(s)"
        )


@pytest.mark.parametrize("workers", ["1", "2"])
@pytest.mark.parametrize("mode", list(MODES))
def test_add_prints_each_conversion_notice(
    conformant_catalog: Path, open_buildings_path: Path, mode: str, workers: str
) -> None:
    """Each rewrite and sidecar notice of a parallel add reaches the terminal."""
    root = conformant_catalog
    (root / "buildings").mkdir()
    for name in ("one", "two"):
        shutil.copy(open_buildings_path, root / "buildings" / f"{name}.parquet")
    _shapefiles(root / "roads", ["alpha", "beta"])

    out, err = _run(mode, "add", "--portolan-dir", str(root), "--workers", workers, str(root))

    expected = [REWRITE.format("one"), REWRITE.format("two")] + [
        SIDECAR.format(name) for name in ("alpha", "beta")
    ]
    shown, other = (err, out) if mode == "json" else (out, err)
    notices = _notices(shown)
    assert (notices if workers == "1" else sorted(notices, key=expected.index)) == expected
    assert _notices(other) == []
