#!/usr/bin/env python3
"""Profile one perf scenario and save the report under the commit SHA (#992).

The script builds the scenario with the seeded generator of
``tests/perf/scenarios.py`` and runs its command through
``tests/perf/runner.py``, so the profile shows the same work as the gates::

    uv run python scripts/profile.py add-many-collections --mode time
    uv run python scripts/profile.py add-large-vector --mode mem --size 100000
    uv run python scripts/profile.py add-many-collections --mode import

Modes:

- ``time`` runs pyinstrument and writes ``profile.html`` and ``profile.txt``.
- ``mem`` runs ``memray run --native`` and writes ``memray.bin``, ``memray.html``
  (a flame graph), and ``memray-stats.txt``. Native mode records the
  allocations in geoparquet-io, DuckDB, and Arrow.
- ``import`` runs ``python -X importtime`` and writes ``importtime.txt``.

Each mode also writes ``record.json``, the runner record with the counters and
the stage times. The output goes to ``.perf/<sha>/<scenario>/``, so two commits
compare by a diff of two directories. ``uv run --with`` loads pyinstrument and
memray, so neither is a project dependency.
"""

from __future__ import annotations

import argparse
import contextlib
import os
import shutil
import subprocess
import sys
import tempfile
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(REPO_ROOT))

from tests.perf.gates import SCALE_SIZES  # noqa: E402 - needs the path above
from tests.perf.scenarios import SCENARIOS  # noqa: E402

#: Sizes of the #990 table. Any other scenario defaults to twice its gate size.
PROFILE_SIZES = {
    "add-many-collections": 100,
    "add-many-collections-thumbs": 100,
    "add-large-vector": 1_000_000,
    "thumbnail-large": 100_000,
}
TOOLS = {"time": "pyinstrument", "mem": "memray"}


def _sha() -> str:
    proc = subprocess.run(
        ["git", "rev-parse", "--short=12", "HEAD"],
        cwd=REPO_ROOT,
        capture_output=True,
        text=True,
        check=True,
    )
    return proc.stdout.strip()


def _python(mode: str) -> list[str]:
    """Return the command that starts Python with the profiler of ``mode``."""
    command = ["uv", "run", "--project", str(REPO_ROOT)]
    if mode in TOOLS:
        command += ["--with", TOOLS[mode]]
    return [*command, "python"]


def _run(
    command: list[str], cwd: Path, *, stdout: Path | None = None, stderr: Path | None = None
) -> None:
    """Run ``command`` in ``cwd``, with the output streams in files when given."""
    env = dict(os.environ)
    env["PYTHONPATH"] = os.pathsep.join(filter(None, (str(REPO_ROOT), env.get("PYTHONPATH"))))
    # memray resolves native symbols through debuginfod, which fetches debug
    # files over the network and stalls for minutes. The reports need none.
    env["DEBUGINFOD_URLS"] = ""
    with contextlib.ExitStack() as stack:
        out = stack.enter_context(stdout.open("w", encoding="utf-8")) if stdout else None
        err = stack.enter_context(stderr.open("w", encoding="utf-8")) if stderr else None
        subprocess.run(command, cwd=cwd, env=env, check=True, stdout=out, stderr=err)


def profile(name: str, mode: str, size: int, out: Path) -> None:
    """Build ``name`` at ``size``, run it under ``mode``, and write the reports to ``out``."""
    scenario = SCENARIOS[name]
    out.mkdir(parents=True, exist_ok=True)
    runner = ["-m", "tests.perf.runner", "--out", str(out / "record.json")]
    if mode == "mem":
        runner.append("--mem")
    runner += ["--", *scenario.command]
    with tempfile.TemporaryDirectory(prefix=f"profile-{name}-") as tmp:
        root = Path(tmp) / "catalog"
        scenario.build(root, size)
        python = _python(mode)
        if mode == "time":
            session = out / "profile.pyisession"
            _run(
                [*python, "-m", "pyinstrument", "-r", "pyisession", "-o", str(session), *runner],
                root,
            )
            for renderer, filename in (("html", "profile.html"), ("text", "profile.txt")):
                _run(
                    [
                        *python,
                        "-m",
                        "pyinstrument",
                        "--load",
                        str(session),
                        "-r",
                        renderer,
                        "-o",
                        str(out / filename),
                    ],
                    root,
                )
        elif mode == "mem":
            capture = out / "memray.bin"
            capture.unlink(missing_ok=True)
            _run([*python, "-m", "memray", "run", "--native", "-o", str(capture), *runner], root)
            _run(
                [
                    *python,
                    "-m",
                    "memray",
                    "flamegraph",
                    "-f",
                    "-o",
                    str(out / "memray.html"),
                    str(capture),
                ],
                root,
            )
            _run(
                [*python, "-m", "memray", "stats", str(capture)],
                root,
                stdout=out / "memray-stats.txt",
            )
        else:
            _run([*python, "-X", "importtime", *runner], root, stderr=out / "importtime.txt")


def main(argv: list[str] | None = None) -> int:
    """Parse the arguments and profile one scenario."""
    parser = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    parser.add_argument("scenario", choices=sorted(SCENARIOS))
    parser.add_argument("--mode", choices=("time", "mem", "import"), default="time")
    parser.add_argument("--size", type=int, help="Scenario size n (default: see PROFILE_SIZES).")
    args = parser.parse_args(argv)
    if shutil.which("uv") is None:
        parser.error("uv is not on PATH")
    size = args.size or PROFILE_SIZES.get(args.scenario, 2 * SCALE_SIZES.get(args.scenario, 50))
    out = REPO_ROOT / ".perf" / _sha() / args.scenario
    profile(args.scenario, args.mode, size, out)
    print(f"{args.scenario} n={size} --mode {args.mode}: {out.relative_to(REPO_ROOT)}")
    for path in sorted(out.iterdir()):
        print(f"  {path.name} ({path.stat().st_size:,} bytes)")
    return 0


if __name__ == "__main__":
    sys.exit(main())
