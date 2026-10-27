"""COG writers must invalidate reads even when a filesystem reuses its stat key."""

from __future__ import annotations

import hashlib
from functools import partial
from typing import TYPE_CHECKING

import numpy as np
import pytest
import rasterio
from rasterio.transform import from_origin

from portolan_cli.conversion_config import CogSettings
from portolan_cli.convert import place_cog, write_cog
from portolan_cli.formats import in_run
from portolan_cli.run_state import RunState

if TYPE_CHECKING:
    from pathlib import Path

pytestmark = pytest.mark.integration


def _cog(root: Path, value: int) -> Path:
    source = root / "src/scene.tif"
    source.parent.mkdir(parents=True)
    with rasterio.open(
        source, "w", driver="GTiff", width=17, height=7, count=1, dtype="uint16",
        crs="EPSG:4326", transform=from_origin(value, 1, .01, .01),
    ) as dst:  # fmt: skip
        dst.write(np.full((1, 7, 17), value, dtype="uint16"))
    return write_cog(source, root, CogSettings(compression="RAW"))


@pytest.mark.parametrize("writer", ["place", "copy"])
def test_cog_replacement_invalidates_destination(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, writer: str
) -> None:
    from portolan_cli import trace
    from portolan_cli.metadata.cog import extract_cog_metadata
    from portolan_cli.metadata.statistics import extract_band_statistics
    from portolan_cli.preparation import convert_raster

    output = _cog(tmp_path / "old", 10)
    replacement = _cog(tmp_path / "new", 20)
    assert output.stat().st_size == replacement.stat().st_size
    state = RunState.open(tmp_path)
    # Pin a reused key; normal ext4 timestamps would hide missing invalidations.
    monkeypatch.setattr("portolan_cli.run_state._stat_key", lambda st: (0, st.st_size, 0, 0))
    with in_run(state):
        old_checksum = state.checksum(output)
        assert state.memo("cog.metadata", output, extract_cog_metadata).bbox[0] == 10
        for mode in ("cached", "approx"):
            assert (
                state.memo(
                    f"cog.statistics.{mode}",
                    output,
                    partial(extract_band_statistics, mode=mode),
                )[0].mean
                == 10
            )
        if writer == "place":
            place_cog(replacement, output)
        else:
            assert convert_raster(replacement, output.parent) == output
        expected_checksum = hashlib.sha256(output.read_bytes()).hexdigest()
        assert expected_checksum != old_checksum
        assert state.checksum(output) == expected_checksum, "COG replacement retained old checksum"
        with trace.tracing():
            assert state.memo("cog.metadata", output, extract_cog_metadata).bbox[0] == 20
            for mode in ("cached", "approx"):
                assert (
                    state.memo(
                        f"cog.statistics.{mode}",
                        output,
                        partial(extract_band_statistics, mode=mode),
                    )[0].mean
                    == 20
                )
            assert trace.counters()["raster.opens"] == 0
