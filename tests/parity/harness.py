"""Command parity: a CLI command and its library function return the same result.

Issue #1133 requires that each CLI command delegates to one library function.
That function accepts the same options and returns a result without printing.
A parity test proves it for one command.

How to add a parity test
------------------------

1. Put the test in ``tests/parity/test_<family>_parity.py`` and mark the
   module ``pytestmark = pytest.mark.integration``.
2. Take the ``parity`` fixture and call it with the command path as a string
   literal, the command arguments without ``--json``, a catalog factory, and a
   call of the library function with the same options::

       parity(
           "info",
           ["points"],
           catalog=points_catalog,
           call=lambda root: inspect_collection(Path("points")),
       )

   ``call`` receives the catalog root, and the working directory is that root,
   as it is for the command. ``tests/parity/test_info_parity.py`` is a full
   example.

3. Remove the command path from ``PARITY_PENDING`` in
   ``tests/parity/test_parity_harness.py``. That test reads each ``parity("...")``
   call and fails while a covered command stays in the list. It also fails for a
   command that is neither covered nor pending.

What one call does
------------------

The harness builds the catalog at ``tmp_path / "parity-catalog"`` and runs
``portolan <command> <args> --json`` there through ``run_cli``. Then it deletes
the catalog, builds it again at the same path, and runs the library call with
that directory as the working directory. Each side gets its own fresh catalog,
because commands write to it. Both sides use the same path, so absolute paths in
the result agree. Before the rebuild, the harness checks the catalog
invariants of ``tests/catalog_invariants.py`` on the catalog that the command
wrote. The ``run_cli`` teardown checks the catalog of the library call. The
library call and its ``to_dict()`` must not write to stdout or stderr.

The conversion rule
-------------------

The library function returns an object with ``to_dict() -> dict[str, Any]``.
The command passes ``result.to_dict()`` to ``emit_success`` as the envelope
``data``, and it changes nothing in it. The harness compares the envelope
``data`` with ``to_dict()`` after one ``json.dumps`` and ``json.loads``, the
same encoding that ``OutputEnvelope.to_json`` applies. Do not shape the data in
``cli.py``. Put each field in ``to_dict()``.

The ignore rule
---------------

A field that differs between two runs, such as a timestamp or a duration, goes
in ``ignore`` as a dotted key path: ``"created"``, ``"summary.elapsed"``, or
``"collections.*.updated"``. A number selects one list element and ``*`` selects
each one. The harness removes the path from both sides and fails when the path
is absent. Do not ignore a field to hide a real difference.

The failure path
----------------

``parity.error(command, args, catalog=..., call=..., raises=SomeError)`` runs
both sides on a case that fails. The library call must raise ``raises``. The
command must exit non-zero with ``success: false`` and exactly one error whose
``type``, ``message``, and ``code`` equal ``ErrorDetail`` built from that
exception. A failure case does not move a command out of ``PARITY_PENDING``.
"""

from __future__ import annotations

import contextlib
import json
import shutil
from collections.abc import Callable, Sequence
from pathlib import Path
from typing import Any, Protocol

import pytest

from portolan_cli import logging_setup
from portolan_cli.json_output import ErrorDetail
from portolan_core.errors import PortolanError
from tests.catalog_invariants import RunCli


class VerifyingRunCli(RunCli, Protocol):
    """The ``run_cli`` fixture, with the catalog invariant check it runs at teardown."""

    def verify(self) -> None: ...


class SupportsToDict(Protocol):
    """A library result: ``to_dict()`` returns the envelope ``data`` of its command."""

    def to_dict(self) -> dict[str, Any]: ...


CatalogFactory = Callable[[Path], object]


def drop_path(data: Any, path: str) -> None:
    """Delete the value at the dotted key ``path`` in ``data``. Fail when it is absent."""
    _drop(data, path.split("."), path)


def _drop(node: Any, keys: list[str], path: str) -> None:
    key, *rest = keys
    if key == "*":
        assert isinstance(node, list) and rest, f"ignore path {path!r}: '*' must select list items"
        for child in node:
            _drop(child, rest, path)
        return
    index: int | str = int(key) if isinstance(node, list) else key
    try:
        child = node[index]
    except (KeyError, IndexError, TypeError, ValueError):
        raise AssertionError(f"ignore path {path!r} is not in the result") from None
    if rest:
        _drop(child, rest, path)
    else:
        del node[index]


class Parity:
    """Run a command and its library function on identical catalogs and compare them."""

    def __init__(
        self, run_cli: VerifyingRunCli, capsys: pytest.CaptureFixture[str], base: Path
    ) -> None:
        self._run_cli = run_cli
        self._capsys = capsys
        self.root = base / "parity-catalog"

    def __call__(
        self,
        command: str,
        args: Sequence[str],
        *,
        catalog: CatalogFactory,
        call: Callable[[Path], SupportsToDict],
        ignore: Sequence[str] = (),
    ) -> None:
        """Assert that the envelope ``data`` equals ``call(root).to_dict()``."""
        envelope = self._command(command, args, catalog, expect=0)
        assert envelope["success"] is True, envelope
        # to_dict() is part of the library result, so it runs inside the capture too.
        expected = json.loads(json.dumps(self._library(catalog, lambda root: call(root).to_dict())))
        actual = envelope["data"]
        for path in ignore:
            drop_path(actual, path)
            drop_path(expected, path)
        assert actual == expected

    def error(
        self,
        command: str,
        args: Sequence[str],
        *,
        catalog: CatalogFactory,
        call: Callable[[Path], object],
        raises: type[Exception],
    ) -> None:
        """Assert that ``call`` raises ``raises`` and the command reports the same error."""
        envelope = self._command(command, args, catalog, expect=None)
        assert envelope["success"] is False, envelope
        with pytest.raises(raises) as caught:
            self._library(catalog, call)
        err = caught.value
        code = err.code if isinstance(err, PortolanError) else None
        assert envelope["errors"] == [ErrorDetail(type(err).__name__, str(err), code).to_dict()]

    def _fresh_catalog(self, catalog: CatalogFactory) -> None:
        if self.root.exists():
            shutil.rmtree(self.root)
        catalog(self.root)

    def _command(
        self, command: str, args: Sequence[str], catalog: CatalogFactory, *, expect: int | None
    ) -> dict[str, Any]:
        self._fresh_catalog(catalog)
        result = self._run_cli(*command.split(), *args, "--json", cwd=self.root, expect=expect)
        if expect is None:
            assert result.exit_code != 0, result.output
        # The rebuild deletes this catalog before the run_cli teardown check, so check it now.
        self._run_cli.verify()
        envelope: dict[str, Any] = json.loads(result.stdout)
        return envelope

    def _library(self, catalog: CatalogFactory, call: Callable[[Path], Any]) -> Any:
        self._fresh_catalog(catalog)
        # The command run leaves its stderr log handler. A library log record
        # is not printed output, so remove the handler before the call.
        logging_setup.reset_logging()
        self._capsys.readouterr()
        try:
            with contextlib.chdir(self.root):
                return call(self.root)
        finally:
            printed = self._capsys.readouterr()
            assert (printed.out, printed.err) == ("", ""), "the library call printed output"
