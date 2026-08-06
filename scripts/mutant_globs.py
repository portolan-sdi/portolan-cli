#!/usr/bin/env python3
"""Translate source paths into the mutant-name globs ``mutmut run`` filters on.

``mutmut run`` takes mutant *names*, not file paths. A mutant name is the dotted
module path plus the mangled function name mutmut generates::

    portolan_cli/backends/iceberg/backend.py  ->  portolan_cli.backends.iceberg.backend.x_push__mutmut_1

Both mutation jobs (the PR-scoped diff run and the nightly rotating shard) select
work by file, so both need this translation. Passing a file path as the filter
matches nothing: mutmut raises ``AssertionError: Filtered for specific mutants,
but nothing matches`` and the run tests zero mutants. See
portolan-sdi/portolan-cli#612.

Mangled names all begin with ``x_`` or ``xǁ``, so the emitted pattern ends in
``.x*``. That anchors the glob to one module — a bare ``package.*`` would also
match every submodule beneath it and silently inflate a shard.

Files mutmut generates no mutants for are dropped, because a glob for one is
just as unmatchable as a file path: mutmut raises the same assertion and fails
the whole run over a module that was never going to be tested. About a fifth of
``portolan_cli`` is mutant-free — re-export ``__init__.py`` files, constant
tables, protocol stubs, model declarations — so a small shard can hold nothing
else, which is what broke nightly run 30735881276. See
portolan-sdi/portolan-cli#716.

Emptiness is decided by mutmut's own mutation pass, not by a heuristic. Nine
mutant-free modules do define functions, so "has a ``def``" would still emit
unmatchable globs for them.

Usage:
    python scripts/mutant_globs.py portolan_cli/readme.py portolan_cli/sync/upload.py

Exit codes: 0 = globs written to stdout, one per line (possibly none, when no
input file has mutants); 1 = no paths given.
"""

from __future__ import annotations

import sys
from collections.abc import Callable, Sequence
from pathlib import Path, PurePosixPath

# Every name mutmut mangles starts with one of these (function: ``x_name``,
# method: ``xǁClassǁname``), so this suffix selects a module's mutants without
# reaching into its submodules.
_MANGLE_GLOB = "x*"


def mutant_glob(path: str | PurePosixPath) -> str:
    """Return the fnmatch pattern matching every mutant name in ``path``.

    Mirrors ``mutmut.__main__.get_mutant_name``: drop the ``.py`` suffix, join
    the parts with dots, strip a ``src.`` layout prefix, and collapse
    ``__init__`` into its package.

    Raises:
        ValueError: ``path`` is not a ``.py`` source file.
    """
    pure = PurePosixPath(str(path).replace("\\", "/"))
    if pure.suffix != ".py":
        raise ValueError(f"not a Python source file: {path}")

    module = ".".join(pure.with_suffix("").parts)
    if module.startswith("src."):
        module = module[len("src.") :]
    # mutmut rewrites ``pkg.__init__.x_f`` to ``pkg.x_f``, so a package's
    # ``__init__`` mutants live under the bare package name.
    if module == "__init__":
        raise ValueError(f"cannot derive a module name from: {path}")
    if module.endswith(".__init__"):
        module = module[: -len(".__init__")]

    return f"{module}.{_MANGLE_GLOB}"


def has_mutants(path: str | Path) -> bool:
    """Return whether mutmut generates at least one mutant for ``path``.

    Asks mutmut to mutate the file and reports whether it named anything. The
    import is local because ``mutmut.__main__`` sets the multiprocessing start
    method at module scope, which raises in a process that already has one.

    Raises:
        OSError: ``path`` cannot be read.
    """
    from mutmut.__main__ import mutate_file_contents

    source = Path(path).read_text(encoding="utf-8")
    _mutated, names = mutate_file_contents(str(path), source)
    return bool(names)


def main(
    argv: Sequence[str] | None = None,
    *,
    probe: Callable[[str], bool] = has_mutants,
) -> int:
    """Print one mutant-name glob per source path that has mutants to test.

    Args:
        argv: Source paths. Defaults to the command line.
        probe: Decides whether a path has mutants. Injected by the tests, which
            cannot import mutmut in-process.
    """
    paths = list(sys.argv[1:] if argv is None else argv)
    if not paths:
        print("error: no source paths given; nothing to mutate", file=sys.stderr)
        return 1

    for path in paths:
        glob = mutant_glob(path)
        if not probe(path):
            # Reported, not counted: a shard that mutates nothing is legitimate,
            # but it should be legible in the log rather than look like a sweep.
            print(f"skipping {path}: mutmut generates no mutants for it", file=sys.stderr)
            continue
        print(glob)
    return 0


if __name__ == "__main__":
    sys.exit(main())
