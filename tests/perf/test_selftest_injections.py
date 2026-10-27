"""Exercise working-tree fault injections without gate_selftest's HEAD archive."""

from __future__ import annotations

import shutil
import subprocess
from pathlib import Path
from unittest.mock import Mock

import pytest

from scripts import gate_selftest as selftest

pytestmark = pytest.mark.integration
REPO = Path(__file__).resolve().parents[2]


@pytest.mark.parametrize("name", [f.name for f in selftest.FAULTS if f.name.endswith("reopen")])
def test_reopen_injections_compile_and_fail_the_open_gate(tmp_path: Path, name: str) -> None:
    shutil.copytree(REPO / "portolan_cli", tmp_path / "portolan_cli")
    from tests.perf.test_open_sites import uncounted_sites

    assert uncounted_sites(tmp_path / "portolan_cli") == []
    fault = selftest.FAULTS_BY_NAME[name]
    fault.apply(tmp_path, True)
    for path in (tmp_path / "portolan_cli").rglob("*.py"):
        compile(path.read_bytes(), str(path), "exec")
    expected = {
        "thumbnail-parquet-reopen": (
            "portolan_cli/viz/thumbnail.py:_read_geoparquet_for_thumbnail: "
            "parquet.opens opened 2, counted 1"
        ),
        "cog-defaults-reopen": (
            "portolan_cli/conversion_config.py:derive_cog_defaults: "
            "raster.opens opened 2, counted 1"
        ),
        "cog-write-reopen": "portolan_cli/convert.py:write_cog: raster.opens opened 2, counted 1",
    }
    assert uncounted_sites(tmp_path / "portolan_cli") == [expected[name]]


@pytest.mark.parametrize("faulty", [True, False], ids=["fault", "control"])
def test_uncompilable_patch_is_broken_before_running_gates(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, faulty: bool
) -> None:
    def copy(_repo: Path, dest: Path) -> Path:
        dest.mkdir()
        (dest / "valid.py").write_text("pass\n")
        return dest

    def apply(root: Path, enabled: bool) -> None:
        if enabled == faulty:
            # Include a patched test: compilation must cover more than the library.
            (root / "test_bad.py").write_text("def broken(:\n")

    run = Mock(
        side_effect=[
            subprocess.CompletedProcess([], 0, "clean", ""),
            subprocess.CompletedProcess([], 1, "failed", ""),
        ]
    )
    monkeypatch.setattr(selftest, "copy_head", copy)
    monkeypatch.setattr(selftest, "run", run)
    result = selftest.run_fault(
        selftest.Fault("bad-syntax", apply, (selftest.Gate(("gate",)),)), REPO, tmp_path
    )
    assert result.status == "BROKEN"
    assert "test_bad.py" in result.detail
    run.assert_not_called()


def test_only_patched_python_is_compiled(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    """An existing invalid fixture is not evidence that the fault is broken."""

    def copy(_repo: Path, dest: Path) -> Path:
        dest.mkdir()
        (dest / "invalid_fixture.py").write_text("def broken(:\n")
        return dest

    def apply(root: Path, _enabled: bool) -> None:
        (root / "valid.py").write_text("pass\n")

    run = Mock(return_value=subprocess.CompletedProcess([], 1, "fault caught", ""))
    monkeypatch.setattr(selftest, "copy_head", copy)
    monkeypatch.setattr(selftest, "run", run)
    result = selftest.run_fault(
        selftest.Fault("valid", apply, (selftest.Gate(("gate",)),), control=False), REPO, tmp_path
    )
    assert result.status == "CAUGHT"
    run.assert_called_once()
