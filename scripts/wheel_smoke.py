"""Build the wheels and prove that the portolan-core wheel works on its own.

The ``--inside`` half runs with ``python -I`` in a fresh venv, so it imports the installed wheel only.
"""

from __future__ import annotations

import json
import os
import subprocess
import sys
import tempfile
import tomllib
import zipfile
from email import policy
from email.parser import BytesParser
from pathlib import Path
from typing import Any

ROOT = Path(__file__).resolve().parents[1]
EXAMPLE = ROOT / "examples" / "python-writer" / "write_catalog.py"
# Distribution names that the portolan-core install must not bring in.
FORBIDDEN_DISTRIBUTIONS = ["pyarrow", "rasterio", "geopandas", "duckdb", "portolan-cli"]
FORBIDDEN_MODULES = ["pyarrow", "rasterio", "geopandas", "duckdb", "portolan_cli"]
REMOVED_COLLECTION = {"parks/AGENTS.md", "parks/README.md", "parks/collection.json"}


class SmokeError(Exception):
    """One check of the smoke test failed."""


def check(condition: bool, message: str) -> None:
    """Raise :class:`SmokeError` with ``message`` when ``condition`` is false."""
    if not condition:
        raise SmokeError(message)


def run(command: list[str], *, cwd: Path = ROOT) -> str:
    """Run ``command`` and return its standard output. A non-zero exit fails the smoke test."""
    print("Run:", " ".join(command), flush=True)
    # The parent uv environment must not leak into the fresh environment.
    env = {
        k: v for k, v in os.environ.items() if k not in {"PYTHONPATH", "PYTHONHOME", "VIRTUAL_ENV"}
    }
    result = subprocess.run(
        command, cwd=cwd, env=env, capture_output=True, text=True, check=False, timeout=600
    )
    check(
        result.returncode == 0,
        f"{' '.join(command)} exited {result.returncode}:\n{result.stdout}\n{result.stderr}",
    )
    return result.stdout


def requires_dist(wheel: Path) -> list[str]:
    """Return the ``Requires-Dist`` lines of the wheel metadata."""
    with zipfile.ZipFile(wheel) as archive:
        (name,) = [n for n in archive.namelist() if n.endswith(".dist-info/METADATA")]
        metadata = BytesParser(policy=policy.default).parsebytes(archive.read(name))
    return [str(value) for value in metadata.get_all("Requires-Dist") or []]


def python_in(path: Path) -> Path:
    """Create a virtual environment at ``path`` and return its interpreter."""
    # uv makes the environment. A system Python can lack ensurepip, which venv needs.
    run(["uv", "venv", "--quiet", "--python", sys.executable, str(path)])
    return path / ("Scripts/python.exe" if os.name == "nt" else "bin/python")


def outside() -> None:
    """Build the wheels, install the portolan-core wheel alone, and run the inside checks."""
    pyproject = tomllib.loads((ROOT / "pyproject.toml").read_text(encoding="utf-8"))
    version = pyproject["project"]["version"]
    with tempfile.TemporaryDirectory(prefix="portolan-wheels-") as directory:
        tmp = Path(directory)
        wheels = tmp / "dist"
        run(["uv", "build", "--all-packages", "--wheel", "--out-dir", str(wheels)])
        (core,) = wheels.glob("portolan_core-*.whl")
        (cli,) = wheels.glob("portolan_cli-*.whl")
        check(core.name.startswith(f"portolan_core-{version}-"), f"{core.name} is not {version}")
        pin = f"portolan-core=={version}"
        check(pin in requires_dist(cli), f"{cli.name} does not require {pin}")
        python = python_in(tmp / "venv")
        run(["uv", "pip", "install", "--quiet", "--python", str(python), str(core)])
        # The work folder is the current folder, so nothing in the checkout is importable.
        work = tmp / "work"
        work.mkdir()
        print(run([str(python), "-I", str(Path(__file__).resolve()), "--inside"], cwd=work))
    print("The portolan-core wheel passed the isolated install checks.")


def check_catalog(folder: Path) -> None:
    """Run the rashid check without data on ``folder``. Any error finding fails."""
    result = subprocess.run(
        [sys.executable, "-I", "-m", "rashid_core", "check", "--no-data", "--json", str(folder)],
        capture_output=True,
        text=True,
        check=False,
        timeout=300,
    )
    try:
        findings: list[dict[str, Any]] = json.loads(result.stdout)["findings"]
    except (json.JSONDecodeError, KeyError) as exc:
        raise SmokeError(f"rashid check gave no report for {folder}: {result.stderr}") from exc
    errors = [f for f in findings if f["severity"] == "error"]
    check(errors == [], f"rashid check found errors in {folder}: {errors}")
    check(result.returncode == 0, f"rashid check exited {result.returncode}: {result.stderr}")
    print(f"rashid check: no errors in {folder.name}")


def files_under(folder: Path) -> set[str]:
    """Return the root-relative POSIX path of each file under ``folder``."""
    return {p.relative_to(folder).as_posix() for p in folder.rglob("*") if p.is_file()}


def inside() -> None:
    """Run the checks in the environment that holds only the portolan-core wheel."""
    import importlib
    import importlib.metadata
    import importlib.util
    import runpy

    installed = {
        str(d.metadata["Name"]).lower().replace("_", "-")
        for d in importlib.metadata.distributions()
    }
    check("portolan-core" in installed, "portolan-core is not installed")
    present = sorted(set(FORBIDDEN_DISTRIBUTIONS) & installed)
    check(present == [], f"the portolan-core install brought in {present}")
    importable = [m for m in FORBIDDEN_MODULES if importlib.util.find_spec(m) is not None]
    check(importable == [], f"importable without the distribution: {importable}")
    print(f"installed: {len(installed)} distributions, none of {FORBIDDEN_DISTRIBUTIONS}")

    # Each module imports with only the declared dependencies installed.
    import pkgutil

    import portolan_core

    modules = [m.name for m in pkgutil.walk_packages(portolan_core.__path__, "portolan_core.")]
    for name in modules:
        importlib.import_module(name)
    print(f"imported {len(modules)} portolan_core modules")

    import obstore
    from obstore.store import MemoryStore

    from portolan_core.writer import (
        DeleteOperation,
        LocalSink,
        ObstoreSink,
        PutOperation,
        apply_plan,
        plan_catalog,
    )

    example = runpy.run_path(str(EXAMPLE))
    documents, documentation = example["documents"], example["documentation"]
    plan = plan_catalog(documents, documentation)
    put_keys = {op.key for op in plan.operations if isinstance(op, PutOperation)}
    check(
        {"README.md", "AGENTS.md"} <= put_keys,
        f"the plan writes no root documentation: {put_keys}",
    )

    work = Path.cwd()
    local = work / "local"
    apply_plan(plan, LocalSink(local))
    check(files_under(local) == put_keys, f"LocalSink wrote {files_under(local)}")
    check_catalog(local)

    store = MemoryStore()
    apply_plan(plan, ObstoreSink(store))
    memory = work / "memory"
    for batch in obstore.list(store):
        for meta in batch:
            target = memory / meta["path"]
            target.parent.mkdir(parents=True, exist_ok=True)
            target.write_bytes(bytes(obstore.get(store, meta["path"]).bytes()))
    check(files_under(memory) == put_keys, f"ObstoreSink wrote {files_under(memory)}")
    check_catalog(memory)

    # The second plan drops the parks collection and its documentation.
    kept_documents = {k: v for k, v in documents.items() if not k.startswith("parks/")}
    kept_documentation = {k: v for k, v in documentation.items() if not k.startswith("parks/")}
    second = plan_catalog(kept_documents, kept_documentation, prior_managed_keys=put_keys)
    deleted = {op.key for op in second.operations if isinstance(op, DeleteOperation)}
    check(deleted == REMOVED_COLLECTION, f"the second plan deletes {deleted}")
    apply_plan(second, LocalSink(local))
    check(
        files_under(local) == put_keys - REMOVED_COLLECTION,
        f"after the deletes the folder holds {files_under(local)}",
    )
    check_catalog(local)
    print(f"managed delete: {sorted(deleted)}")


def main() -> None:
    """Run one half of the smoke test. Exit 1 with the reason when a check fails."""
    try:
        if "--inside" in sys.argv[1:]:
            inside()
        else:
            outside()
    except SmokeError as exc:
        print(f"wheel smoke failed: {exc}", file=sys.stderr)
        sys.exit(1)


if __name__ == "__main__":
    main()
