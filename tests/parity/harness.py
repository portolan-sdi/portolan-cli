"""Command parity: a CLI command and its library function return the same result.
``.claude/rules/testing.md`` gives the convention for a parity test.
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
        assert isinstance(node, list), f"ignore path {path!r}: '*' must select list items"
        assert rest, f"ignore path {path!r}: '*' must select list items"
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
