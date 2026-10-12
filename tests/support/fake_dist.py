"""Write fake installed distributions for entry point tests (#947).

``importlib.metadata`` finds a distribution through a ``*.dist-info``
directory on ``sys.path``. A test puts such a directory, plus the modules its
entry points name, in a temporary directory and adds it to ``PYTHONPATH`` of a
subprocess.
"""

from __future__ import annotations

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from pathlib import Path


def write_fake_dist(
    site: Path,
    name: str,
    entry_points: dict[str, dict[str, str]],
    modules: dict[str, str] | None = None,
) -> None:
    """Write ``<name>-0.1.dist-info`` and the given modules into ``site``.

    Args:
        site: Directory a subprocess will have on ``sys.path``.
        name: Distribution name.
        entry_points: Group name to ``{entry point name: object reference}``.
        modules: Module file name stem to source text.
    """
    dist_info = site / f"{name.replace('-', '_')}-0.1.dist-info"
    dist_info.mkdir(parents=True)
    (dist_info / "METADATA").write_text(
        f"Metadata-Version: 2.1\nName: {name}\nVersion: 0.1\n", encoding="utf-8"
    )
    lines: list[str] = []
    for group, entries in entry_points.items():
        lines.append(f"[{group}]")
        lines += [f"{ep_name} = {value}" for ep_name, value in entries.items()]
        lines.append("")
    (dist_info / "entry_points.txt").write_text("\n".join(lines), encoding="utf-8")
    for stem, source in (modules or {}).items():
        (site / f"{stem}.py").write_text(source, encoding="utf-8")
