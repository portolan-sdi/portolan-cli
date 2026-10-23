"""Pin the static analysis gates and their ratchets (#1035).

Each ratchet below is a list of exemptions that exists today. A ratchet only
shrinks. When a module becomes clean, delete its line from the config and from
the pinned set here in the same commit. Never add a line to either side.

The tests read repo-root files, so they live under ``tests/docs/``. The mutmut
sandbox guard in ``tests/conftest.py`` fails such a read anywhere else.
"""

from __future__ import annotations

import ast
import json
import sys
from pathlib import Path
from typing import Any

import pytest

if sys.version_info >= (3, 11):
    import tomllib
else:  # pytest itself depends on tomli before 3.11
    import tomli as tomllib

REPO_ROOT = Path(__file__).resolve().parents[2]
PYPROJECT = REPO_ROOT / "pyproject.toml"
PREK = REPO_ROOT / "prek.toml"
JSCPD_CONFIG = REPO_ROOT / ".jscpd.json"
JSCPD_BASELINE = REPO_ROOT / ".jscpd-baseline.json"
VULTURE_WHITELIST = REPO_ROOT / "vulture_whitelist.py"

pytestmark = [pytest.mark.unit, pytest.mark.source_scan]

BANNED_API = {
    "portolan_cli.output": (
        "Library code logs through logging.getLogger. Only the CLI layer prints. See #1005."
    ),
    "obstore.put": (
        "Call the put_object helper in portolan_cli.sync.upload. "
        "It sets Content-Type and the part size. See #807 and #908."
    ),
    "obstore.put_async": (
        "Call the put_object_async helper in portolan_cli.sync.upload. See #807 and #908."
    ),
}

# The CLI layer prints by design. api.py re-exports output for plugins.
# sync/upload.py is the object-store transport.
PERMANENT_TID251 = frozenset(
    {
        "portolan_cli/cli.py",
        "portolan_cli/cli_types.py",
        "portolan_cli/emit.py",
        "portolan_cli/add_progress.py",
        "portolan_cli/scan/progress.py",
        "portolan_cli/plugins/commands.py",
        "portolan_cli/backends/iceberg/cli.py",
        "portolan_cli/extract/*/cli.py",
        "portolan_cli/extract/common/cli_helpers.py",
        "portolan_cli/extract/common/layer_cli.py",
        "portolan_cli/api.py",
        "portolan_cli/sync/upload.py",
    }
)
NOT_CLI_LAYER = frozenset({"portolan_cli/api.py", "portolan_cli/sync/upload.py"})

# Delete only. Each module here still imports portolan_cli.output or calls
# obstore.put directly. Its owning #1035 agent deletes the line when the
# module is clean.
RATCHET_TID251 = frozenset(
    {
        "portolan_cli/add.py",
        "portolan_cli/catalog.py",
        "portolan_cli/collection_thumbnail.py",
        "portolan_cli/conversion_config.py",
        "portolan_cli/convert.py",
        "portolan_cli/extract/arcgis/imageserver/extractor.py",
        "portolan_cli/extract/arcgis/imageserver/orchestrator.py",
        "portolan_cli/extract/arcgis/orchestrator.py",
        "portolan_cli/extract/common/orchestrator_base.py",
        "portolan_cli/extract/common/source.py",
        "portolan_cli/finalization.py",
        "portolan_cli/preparation.py",
        "portolan_cli/stac.py",
        "portolan_cli/sync/core.py",
        "portolan_cli/sync/download.py",
        "portolan_cli/sync/pull.py",
        "portolan_cli/sync/push.py",
        "portolan_cli/viz/pmtiles.py",
        "portolan_cli/viz/style.py",
    }
)

# Ruff lifts TID251 per file, not per banned name. sync/upload.py keeps its
# obstore.put calls, so ruff exempts it for good. import-linter still sees its
# output import, and that edge is a ratchet line there.
OUTPUT_RATCHET_ONLY_IN_IMPORT_LINTER = frozenset({"portolan_cli.sync.upload"})

OBJECT_STORE_IMPORTERS = frozenset(
    {
        "portolan_cli.sync.upload",
        "portolan_cli.sync.download",
        "portolan_cli.sync.push",
        "portolan_cli.sync.pull",
    }
)

# Delete only. These modules report `Statement is unreachable` today.
MYPY_UNREACHABLE_RATCHET = frozenset(
    {
        "portolan_cli.backends.iceberg.backend",
        "portolan_cli.backends.iceberg.hooks",
        "portolan_cli.conversion_config",
        "portolan_cli.readme",
        "portolan_cli.stac",
        "portolan_cli.viz.thumbnail",
    }
)

# Delete only. Lower these numbers when a name or a clone goes away.
VULTURE_WHITELIST_NAMES = 109
JSCPD_BASELINE_FINGERPRINTS = 937


def _pyproject() -> dict[str, Any]:
    with PYPROJECT.open("rb") as handle:
        config: dict[str, Any] = tomllib.load(handle)
    return config


def _ruff_lint() -> dict[str, Any]:
    lint: dict[str, Any] = _pyproject()["tool"]["ruff"]["lint"]
    return lint


def _tid251_ignored_paths() -> set[str]:
    ignores: dict[str, list[str]] = _ruff_lint()["per-file-ignores"]
    return {
        path
        for path, codes in ignores.items()
        if path.startswith("portolan_cli/") and "TID251" in codes
    }


def _contract(contract_id: str) -> dict[str, Any]:
    contracts: list[dict[str, Any]] = _pyproject()["tool"]["importlinter"]["contracts"]
    matches = [contract for contract in contracts if contract.get("id") == contract_id]
    assert len(matches) == 1, f"expected one import-linter contract {contract_id!r}"
    return matches[0]


def _ignored_importers(contract: dict[str, Any], target: str) -> set[str]:
    importers = set()
    for line in contract.get("ignore_imports", []):
        importer, _, imported = (part.strip() for part in line.partition("->"))
        assert imported == target, f"{line!r} does not ignore an edge to {target}"
        importers.add(importer)
    return importers


def _module_to_path(module: str) -> str:
    return module.replace(".", "/") + ".py"


def _path_to_module(path: str) -> str:
    return path.removesuffix(".py").replace("/", ".")


def _expand(patterns: frozenset[str] | set[str]) -> set[str]:
    expanded: set[str] = set()
    for pattern in patterns:
        hits = sorted(path.relative_to(REPO_ROOT).as_posix() for path in REPO_ROOT.glob(pattern))
        assert hits, f"{pattern} matches no file"
        expanded.update(hits)
    return expanded


def test_tid251_is_selected_and_bans_the_three_names() -> None:
    lint = _ruff_lint()
    assert "TID251" in lint["select"]
    banned = lint["flake8-tidy-imports"]["banned-api"]
    assert {name: entry["msg"] for name, entry in banned.items()} == BANNED_API


def test_tid251_ratchet_only_shrinks() -> None:
    """Delete only: the TID251 exemptions are the permanent set plus the ratchet.

    Delete a ratchet line when its module stops importing portolan_cli.output
    and stops calling obstore.put. Never add one.
    """
    ignores: dict[str, list[str]] = _ruff_lint()["per-file-ignores"]
    assert _tid251_ignored_paths() == PERMANENT_TID251 | RATCHET_TID251
    for path in PERMANENT_TID251 | RATCHET_TID251:
        assert ignores[path] == ["TID251"], path
    assert _expand(RATCHET_TID251) == set(RATCHET_TID251)


def test_permanent_tid251_allowlist_is_the_cli_layer() -> None:
    """The permanent exemptions, less api.py and sync/upload.py, are layer-cli."""
    protected = _contract("layer-cli")["protected_modules"]
    cli_layer = _expand({_module_to_path(module) for module in protected})
    permanent = _tid251_ignored_paths() - RATCHET_TID251 - NOT_CLI_LAYER
    assert _expand(permanent) == cli_layer


def test_import_linter_ratchet_matches_tid251_ratchet() -> None:
    """Delete only: both layers of the output ban carry the same ratchet."""
    contract = _contract("output-is-cli-only")
    assert contract["type"] == "forbidden"
    assert contract["source_modules"] == ["portolan_cli", "portolan_cli.**"]
    assert contract["forbidden_modules"] == ["portolan_cli.output"]
    importers = _ignored_importers(contract, "portolan_cli.output")
    permanent = {_path_to_module(path) for path in PERMANENT_TID251 - NOT_CLI_LAYER}
    permanent.add("portolan_cli.api")
    ratchet = importers - permanent
    assert ratchet == {_path_to_module(path) for path in RATCHET_TID251} | (
        OUTPUT_RATCHET_ONLY_IN_IMPORT_LINTER
    )


def test_object_store_contract_admits_only_the_sync_transport() -> None:
    contract = _contract("object-store-is-sync-transport-only")
    assert contract["type"] == "forbidden"
    assert contract["source_modules"] == ["portolan_cli", "portolan_cli.**"]
    assert contract["forbidden_modules"] == ["obstore"]
    assert _ignored_importers(contract, "obstore") == OBJECT_STORE_IMPORTERS


def _whitelist_names() -> list[str]:
    tree = ast.parse(VULTURE_WHITELIST.read_text(encoding="utf-8"))
    names: list[str] = []
    for node in tree.body:
        if isinstance(node, ast.Expr) and isinstance(node.value, ast.Name):
            names.append(node.value.id)
        elif isinstance(node, ast.Expr) and isinstance(node.value, ast.Attribute):
            names.append(f"_.{node.value.attr}")
        elif isinstance(node, ast.Assign):
            names.extend(target.id for target in node.targets if isinstance(target, ast.Name))
    return names


def test_vulture_whitelist_is_pinned() -> None:
    """Delete only: lower the pinned count when a name leaves the whitelist."""
    names = _whitelist_names()
    duplicates = sorted({name for name in names if names.count(name) > 1})
    assert duplicates == []
    assert len(names) == VULTURE_WHITELIST_NAMES


def test_vulture_min_confidence_is_60() -> None:
    vulture = _pyproject()["tool"]["vulture"]
    assert vulture["min_confidence"] == 60
    assert "vulture_whitelist.py" in vulture["paths"]


def test_prek_vulture_hook_has_no_args() -> None:
    """The hook reads ``[tool.vulture]``, so CI and a local run use one config.

    prek.toml uses multi-line inline tables, which tomllib (TOML 1.0) rejects.
    Each ``[[repos]]`` block is parsed on its own instead.
    """
    blocks = PREK.read_text(encoding="utf-8").split("[[repos]]")
    vulture = [block for block in blocks if "jendrikseipp/vulture" in block]
    assert len(vulture) == 1
    repo = tomllib.loads(vulture[0])
    assert (repo["rev"], repo["hooks"]) == ("v2.16", [{"id": "vulture"}])


def test_jscpd_baseline_only_shrinks() -> None:
    """Delete only: lower the pinned count after a real deduplication."""
    config = json.loads(JSCPD_CONFIG.read_text(encoding="utf-8"))
    assert config["minTokens"] == 50
    assert config["failOnEmpty"] is True
    baseline = json.loads(JSCPD_BASELINE.read_text(encoding="utf-8"))
    assert len(baseline["fingerprints"]) == JSCPD_BASELINE_FINGERPRINTS


def test_mypy_unreachable_ratchet_only_shrinks() -> None:
    """Delete only: drop a module when mypy no longer reports it unreachable."""
    mypy = _pyproject()["tool"]["mypy"]
    assert mypy["warn_unreachable"] is True
    blocks = [block for block in mypy["overrides"] if "warn_unreachable" in block]
    assert len(blocks) == 1
    assert blocks[0]["warn_unreachable"] is False
    assert set(blocks[0]["module"]) == MYPY_UNREACHABLE_RATCHET
    assert len(blocks[0]["module"]) == len(MYPY_UNREACHABLE_RATCHET)
