#!/usr/bin/env python3
# ruff: noqa: S101, W291, W293  (the self-check uses assert; the embedded patch keeps its context whitespace)
"""Move the light reader modules from portolan_cli into portolan_core (#1133).

A one-off codemod. It is re-runnable and idempotent:

    uv run python scripts/move_to_core.py --dry-run   # report only
    uv run python scripts/move_to_core.py             # apply

Steps, in order:

1. ``git mv`` each module in ``MOVES`` to ``packages/portolan-core/src/``.
2. In every tracked text file in ``SCOPE``:
   - split ``from portolan_cli import versions, stac`` style imports, and move
     the names in ``NAME_MOVES`` to their new module (Python files only);
   - rewrite each dotted reference ``portolan_cli.<moved>`` (imports, patch
     targets, ``import_module`` strings, docs) on a module boundary;
   - rewrite each path ``portolan_cli/<moved>.py`` and ``portolan_cli/models/``.
3. Apply ``MANUAL_PATCH`` (the hand edits: contracts, deptry, logging, the
   test helpers) with ``git apply --3way``. A patch that is already applied
   is skipped. A patch that does not apply is reported, and the script exits 1.

There are no re-export shims, so a patch target on the old path cannot pass
silently: the final grep in ``main`` fails when one remains.
Stdlib only.
"""

from __future__ import annotations

import argparse
import ast
import re
import shutil
import subprocess
import sys
import tempfile
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
CORE_SRC = "packages/portolan-core/src/portolan_core"

# Old dotted module -> new dotted module. A package moves with its subtree.
_FLAT = [
    "agents_md",
    "bbox",
    "catalog_list",
    "config",
    "constants",
    "errors",
    "extension_registry",
    "format_families",
    "json_io",
    "licensing",
    "query",
    "run_state",
    "stac_links",
    "status",
    "trace",
    "versions",
]
MOVES: dict[str, str] = {f"portolan_cli.{m}": f"portolan_core.{m}" for m in _FLAT}
MOVES["portolan_cli.models"] = "portolan_core.models"
MOVES["portolan_cli.sync.checksums"] = "portolan_core.checksums"

# Names that leave a moved module because they need a dependency core lacks.
# (new module of the source, name) -> module that now defines the name.
NAME_MOVES: dict[tuple[str, str], str] = {
    # python-dotenv. cli.py is the only caller.
    ("portolan_core.config", "load_dotenv_from_catalog"): "portolan_cli.cli",
}

SCOPE_DIRS = (
    "portolan_cli/",
    "packages/",
    "tests/",
    "scripts/",
    "docs/",
    "examples/",
    ".claude/rules/",
    ".github/",
)
SCOPE_FILES = {
    "AGENTS.md",
    "pyproject.toml",
    "vulture_whitelist.py",
    "prek.toml",
    "mkdocs.yml",
    ".jscpd.json",
    ".jscpd-baseline.json",
}
SKIP_FILES = {"scripts/move_to_core.py", "CHANGELOG.md"}
TEXT_SUFFIXES = {".py", ".md", ".toml", ".yml", ".yaml", ".json", ".txt", ".cfg", ".ini", ".sh"}


def _path_of(dotted: str) -> str:
    """Return the source path of a dotted module, as a file or package dir."""
    top, _, rest = dotted.partition(".")
    base = "portolan_cli" if top == "portolan_cli" else CORE_SRC
    rel = rest.replace(".", "/")
    return f"{base}/{rel}" if (ROOT / base / rel).is_dir() else f"{base}/{rel}.py"


def _old_alternation() -> str:
    # Longest first, so sync.checksums wins over a shorter name.
    names = sorted((k.removeprefix("portolan_cli.") for k in MOVES), key=len, reverse=True)
    # A dot may be regex-escaped, as in a pytest.raises(match=...) pattern.
    return "|".join(re.escape(n).replace(r"\.", r"\\?\.") for n in names)


# A dotted reference on a module boundary: not part of a longer identifier.
DOTTED = re.compile(rf"(?<![\w.\\])portolan_cli(\\?\.)({_old_alternation()})(?!\w)")
# A path reference: portolan_cli/<name>.py, or portolan_cli/models/ for a package.
_PATH_NAMES = "|".join(re.escape(k.removeprefix("portolan_cli.").replace(".", "/")) for k in MOVES)
PATHS = re.compile(rf"(?<![\w/.-])portolan_cli/({_PATH_NAMES})(\.py\b|/|(?![\w/.-]))")


def _new_path_tail(old_tail: str) -> str:
    return (
        MOVES["portolan_cli." + old_tail.replace("/", ".")]
        .removeprefix("portolan_core.")
        .replace(".", "/")
    )


def rewrite_dotted(text: str) -> tuple[str, int]:
    """Rewrite ``portolan_cli.<moved>`` and ``portolan_cli/<moved>.py`` references."""

    def dotted(m: re.Match[str]) -> str:
        sep = m.group(1)
        new = MOVES["portolan_cli." + m.group(2).replace(sep, ".")]
        return new.replace(".", sep)

    text, n1 = DOTTED.subn(dotted, text)
    text, n2 = PATHS.subn(lambda m: f"{CORE_SRC}/{_new_path_tail(m.group(1))}{m.group(2)}", text)
    return text, n1 + n2


def _canonical(module: str) -> str:
    for old, new in MOVES.items():
        if module == old or module.startswith(old + "."):
            return new + module[len(old) :]
    return module


def _module_of(path: str) -> str | None:
    for base, pkg in (("portolan_cli/", "portolan_cli"), (CORE_SRC + "/", "portolan_core")):
        if path.startswith(base) and path.endswith(".py"):
            parts = [pkg, *path[len(base) : -3].split("/")]
            if parts[-1] == "__init__":
                parts.pop()
            return ".".join(parts)
    return None


def _alias(a: ast.alias, name: str | None = None) -> str:
    name = name or a.name
    return f"{name} as {a.asname}" if a.asname else name


def rewrite_import_froms(source: str, path: str) -> tuple[str, int]:
    """Split ``from <pkg> import <moved module>`` and move ``NAME_MOVES`` names.

    Returns the new source and the count of statements changed. The dotted
    regex handles every other form, so this runs first.
    """
    try:
        tree = ast.parse(source)
    except SyntaxError:
        return source, 0
    own = _module_of(path)
    own_canonical = _canonical(own) if own else None
    lines = source.splitlines(keepends=True)
    edits: list[tuple[int, int, str]] = []
    for node in ast.walk(tree):
        if not isinstance(node, ast.ImportFrom) or node.level != 0 or node.module is None:
            continue
        module = node.module
        groups: dict[str, list[str]] = {}
        changed = False
        for a in node.names:
            sub = f"{module}.{a.name}"
            target_mod = NAME_MOVES.get((_canonical(module), a.name))
            if sub in MOVES:
                parent, _, leaf = MOVES[sub].rpartition(".")
                groups.setdefault(parent, []).append(_alias(a, leaf))
                changed = True
            elif target_mod is not None:
                changed = True
                if target_mod not in (own, own_canonical):
                    groups.setdefault(target_mod, []).append(_alias(a))
            else:
                groups.setdefault(module, []).append(_alias(a))
        if not changed:
            continue
        start, end = node.lineno - 1, (node.end_lineno or node.lineno) - 1
        head = lines[start][: node.col_offset]
        tail = lines[end][node.end_col_offset or 0 :]
        if head.strip() or (tail.strip() and not tail.strip().startswith("#")):
            raise SystemExit(f"{path}:{node.lineno}: import shares its line; edit it by hand")
        comment = f"  {tail.strip()}" if tail.strip() else ""
        new = "".join(
            f"{head}from {mod} import {', '.join(names)}{comment if i == 0 else ''}\n"
            for i, (mod, names) in enumerate(groups.items())
        )
        if not new:
            new = f"{head}pass\n"  # every name was the file's own; keep the block valid
        edits.append((start, end, new))
    for start, end, new in sorted(edits, reverse=True):
        lines[start : end + 1] = [new]
    return "".join(lines), len(edits)


def git(*args: str, check: bool = False) -> subprocess.CompletedProcess[str]:
    """Run git in the repository root."""
    return subprocess.run(["git", *args], cwd=ROOT, capture_output=True, text=True, check=check)


def _drop_pycache_dir(folder: Path) -> None:
    """Remove a folder that git mv emptied, when only __pycache__ is left in it."""
    if not folder.is_dir():
        return
    rest = [p for p in folder.rglob("*") if p.is_file() and "__pycache__" not in p.parts]
    if rest:
        raise SystemExit(f"{folder} still holds untracked files: {rest}")
    shutil.rmtree(folder)


def move_files(dry_run: bool) -> list[str]:
    """Git-move each module in MOVES that is still at its old path."""
    moved = []
    for old, new in MOVES.items():
        src = _path_of(old)
        rel_new = CORE_SRC + "/" + new.removeprefix("portolan_core.").replace(".", "/")
        is_package = (ROOT / src).is_dir() or (ROOT / rel_new).is_dir()
        src = src.removesuffix(".py") if is_package else src
        dst = rel_new if is_package else rel_new + ".py"
        if not git("ls-files", src).stdout.strip():  # only untracked __pycache__ may remain
            if not dry_run:
                _drop_pycache_dir(ROOT / src)
            if not (ROOT / dst).exists():
                raise SystemExit(f"neither {src} nor {dst} exists")
            continue
        if git("ls-files", dst).stdout.strip():
            raise SystemExit(f"both {src} and {dst} exist")
        _drop_pycache_dir(ROOT / dst)  # a stale __pycache__ from an earlier checkout
        moved.append(f"{src} -> {dst}")
        if not dry_run:
            (ROOT / dst).parent.mkdir(parents=True, exist_ok=True)
            git("mv", src, dst, check=True)
            _drop_pycache_dir(ROOT / src)
    return moved


def in_scope(path: str) -> bool:
    """Return True for a tracked text file that the rewrite covers."""
    if path in SKIP_FILES:
        return False
    if path in SCOPE_FILES:
        return True
    return path.startswith(SCOPE_DIRS) and Path(path).suffix in TEXT_SUFFIXES


def rewrite_tree(dry_run: bool) -> tuple[dict[str, int], list[str]]:
    """Rewrite the references in every file in scope."""
    changed: dict[str, int] = {}
    tracked = git("ls-files", check=True).stdout.splitlines()
    for path in tracked:
        if not in_scope(path) or not (ROOT / path).is_file():
            continue
        text = (ROOT / path).read_text(encoding="utf-8")
        new, count = text, 0
        if path.endswith(".py"):
            new, count = rewrite_import_froms(new, path)
        new, n = rewrite_dotted(new)
        count += n
        if new != text:
            changed[path] = count
            if not dry_run:
                (ROOT / path).write_text(new, encoding="utf-8")
    return changed, tracked


def apply_patch(dry_run: bool) -> str:
    """Apply MANUAL_PATCH unless it is already applied."""
    patch = MANUAL_PATCH.lstrip("\n")
    if not patch.strip():
        return "no manual patch"
    with tempfile.NamedTemporaryFile("w", suffix=".patch", delete=False) as handle:
        handle.write(patch)
    patch_file = Path(handle.name)
    try:
        if git("apply", "--reverse", "--check", str(patch_file), check=False).returncode == 0:
            return "manual patch already applied"
        if dry_run:
            ok = git("apply", "--check", str(patch_file), check=False)
            return "manual patch applies" if ok.returncode == 0 else f"manual patch: {ok.stderr}"
        # --3way needs the index to match the tree, so stage the mechanical rewrite first.
        git("add", "-A", "--", ".", ":!scripts/move_to_core.py", check=True)
        result = git("apply", "--3way", "--whitespace=nowarn", str(patch_file), check=False)
        if result.returncode != 0:
            print(result.stderr, file=sys.stderr)
            raise SystemExit("the manual patch did not apply; resolve the conflicts by hand")
        return "manual patch applied"
    finally:
        patch_file.unlink(missing_ok=True)


def leftovers() -> list[str]:
    """Return each remaining old reference outside CHANGELOG.md."""
    out = git("grep", "-n", "-F", "portolan_cli", "--", ".", *(f":!{f}" for f in SKIP_FILES))
    return [line for line in out.stdout.splitlines() if DOTTED.search(line) or PATHS.search(line)]


def main(argv: list[str] | None = None) -> int:
    """Run the codemod and print the summary."""
    parser = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    parser.add_argument("--dry-run", action="store_true", help="Report, change nothing.")
    args = parser.parse_args(argv)

    moved = move_files(args.dry_run)
    changed, _ = rewrite_tree(args.dry_run)
    if not args.dry_run:
        # The new paths sort into the first-party block in another order.
        py = [p for p in changed if p.endswith(".py")]
        if py:
            ruff = [sys.executable, "-m", "ruff"]
            subprocess.run(
                [*ruff, "check", "--select", "I", "--fix", "-q", *py], cwd=ROOT, check=False
            )
            subprocess.run([*ruff, "format", "-q", *py], cwd=ROOT, check=True)
    patch_status = apply_patch(args.dry_run) if not args.dry_run or not moved else "skipped"

    print(f"Files moved: {len(moved)}")
    for line in moved:
        print(f"  {line}")
    print(f"Files rewritten: {len(changed)}, references changed: {sum(changed.values())}")
    for path, n in sorted(changed.items()):
        print(f"  {n:4d}  {path}")
    print(f"Manual patch: {patch_status}")
    if not args.dry_run:
        rest = leftovers()
        if rest:
            print("Old references remain:", *rest, sep="\n  ")
            return 1
        print("No reference to an old module path remains.")
    return 0


def _self_check() -> None:
    assert rewrite_dotted("portolan_cli.status_x portolan_cli.status.f")[0] == (
        "portolan_cli.status_x portolan_core.status.f"
    )
    assert rewrite_dotted("a.portolan_cli.config portolan_cli.conversion_config")[1] == 0
    assert (
        rewrite_dotted(r"r'portolan_cli\.sync\.checksums: x'")[0]
        == r"r'portolan_core\.checksums: x'"
    )
    assert rewrite_dotted('"portolan_cli/sync/checksums.py" portolan_cli/models/**')[0] == (
        f'"{CORE_SRC}/checksums.py" {CORE_SRC}/models/**'
    )
    assert rewrite_dotted("portolan_cli/configs.py portolan_cli/config.pyi")[1] == 0
    src = "if x:\n    from portolan_cli import preparation, trace as t  # c\n"
    assert rewrite_import_froms(src, "tests/t.py")[0] == (
        "if x:\n    from portolan_cli import preparation  # c\n"
        "    from portolan_core import trace as t\n"
    )
    src = "from portolan_cli.sync import checksums\n"
    assert rewrite_import_froms(src, "tests/t.py")[0] == "from portolan_core import checksums\n"
    src = "from portolan_cli.config import (\n    get_setting,\n    load_dotenv_from_catalog,\n)\n"
    assert rewrite_import_froms(src, "tests/t.py")[0] == (
        "from portolan_cli.config import get_setting\n"
        "from portolan_cli.cli import load_dotenv_from_catalog\n"
    )
    src = "from portolan_core.config import a, load_dotenv_from_catalog\n"
    assert rewrite_import_froms(src, "portolan_cli/cli.py")[0] == (
        "from portolan_core.config import a\n"
    )


# The hand edits that the regex cannot make. Regenerate with
# `git diff <mechanical-commit> -- <files>` after a by-hand fix on a newer tree.
MANUAL_PATCH = r'''
diff --git a/.claude/rules/architecture.md b/.claude/rules/architecture.md
index c3704b8..fa9668d 100644
--- a/.claude/rules/architecture.md
+++ b/.claude/rules/architecture.md
@@ -20,6 +20,16 @@ Click shell and the geospatial stack). `packages/portolan-core` is
 `portolan_cli`, Click, or the geospatial stack. The import-linter contracts
 `core-no-cli` and `core-no-heavy` enforce this.
 
+`portolan_core` holds the writer and the light reader modules: `versions`,
+`json_io`, `bbox`, `models`, `query`, `status`, `catalog_list`, `agents_md`,
+`run_state`, `config`, `constants`, `errors`, `extension_registry`,
+`format_families`, `stac_links`, `checksums` (once `sync.checksums`), `trace`,
+and `licensing`. Import them from `portolan_core.<name>`. `portolan_cli` keeps
+no shim, so a patch target names the `portolan_core` path. `portolan_cli.api`
+still re-exports the plugin names. A function that needs a dependency of
+`portolan-cli` stays in `portolan_cli`: `load_dotenv_from_catalog` (python-dotenv)
+lives in `cli.py`.
+
 ## The layers and how a command flows down them
 
 ```mermaid
@@ -123,8 +133,9 @@ subsystem table below and the path-scoped rules for each.
   checking, not locking. Concurrency safety is an Iceberg-only property.
 - Import contracts (`uv run lint-imports`): `cli` must not import `backends`.
 - Three layer contracts (#944, #947). `layer-read` keeps the read modules
-  (`versions`, `json_io`, `bbox`, `models`, `query`, `status`, `catalog_list`,
-  `agents_md`) free of Click, rasterio, `cli`, and the write modules.
+  (`portolan_core.versions`, `json_io`, `bbox`, `models`, `query`, `status`,
+  `catalog_list`, `agents_md`, `run_state`) free of Click, rasterio, `cli`, and
+  the write modules.
   `layer-manage` keeps every module outside the CLI layer free of Click.
   `layer-cli` lets only the CLI layer (`cli`, `cli_types`, `emit`,
   `add_progress`, `scan.progress`) import a CLI module. Each contract follows
diff --git a/mkdocs.yml b/mkdocs.yml
index 69666af..c031a49 100644
--- a/mkdocs.yml
+++ b/mkdocs.yml
@@ -59,8 +59,11 @@ plugins:
   - mkdocstrings:
       handlers:
         python:
-          paths: [.]
+          paths: [., packages/portolan-core/src]
           options:
+            # portolan_cli.api re-exports names from portolan_core (#1133).
+            # griffe resolves an alias only into a loaded package.
+            preload_modules: [portolan_core]
             show_source: false
             show_root_heading: true
             show_root_full_path: false
diff --git a/packages/portolan-core/pyproject.toml b/packages/portolan-core/pyproject.toml
index b1a3874..32067cb 100644
--- a/packages/portolan-core/pyproject.toml
+++ b/packages/portolan-core/pyproject.toml
@@ -15,6 +15,7 @@ authors = [
 ]
 dependencies = [
     "rashid-core>=0.5.1,<0.6.0", # The validator core without its data extra. 0.5.1 is the floor because it is the first release with CatalogGraph.from_documents, which the writer uses (#1133)
+    "pyyaml>=6.0", # config and run_state read .portolan/config.yaml and metadata.yaml. Same floor as portolan-cli
     "obstore>=0.12.1", # The ObstoreSink of the writer. rashid-core already installs obstore, so an optional extra would make the install no smaller. Same floor as portolan-cli
 ]
 
@@ -26,3 +27,7 @@ dependencies = [
 packages = ["src/portolan_core"]
 
 # The root prek.toml runs deptry on this member as the deptry-core hook.
+[tool.deptry]
+# deptry reads the import name of this package from the source folder only
+# when it sits at the root. Here it sits under src/.
+known_first_party = ["portolan_core"]
diff --git a/packages/portolan-core/src/portolan_core/__init__.py b/packages/portolan-core/src/portolan_core/__init__.py
index 3311781..d861835 100644
--- a/packages/portolan-core/src/portolan_core/__init__.py
+++ b/packages/portolan-core/src/portolan_core/__init__.py
@@ -9,3 +9,10 @@ pyarrow, geopandas, duckdb, pystac, pyproj, shapely, numpy), or
 ``portolan_cli``. The import-linter contracts ``core-no-cli`` and
 ``core-no-heavy`` in the root ``pyproject.toml`` enforce this.
 """
+
+from __future__ import annotations
+
+import logging
+
+# A library adds only a NullHandler. The application configures logging.
+logging.getLogger(__name__).addHandler(logging.NullHandler())
diff --git a/packages/portolan-core/src/portolan_core/config.py b/packages/portolan-core/src/portolan_core/config.py
index 7aff3b7..3694058 100644
--- a/packages/portolan-core/src/portolan_core/config.py
+++ b/packages/portolan-core/src/portolan_core/config.py
@@ -1639,39 +1639,6 @@ def load_own_metadata(path: Path) -> dict[str, Any]:
     return copy.deepcopy(own_metadata(path))
 
 
-# =============================================================================
-# .env file support (Issue #356)
-# =============================================================================
-
-
-def load_dotenv_from_catalog(catalog_path: Path | None = None) -> bool:
-    """Load environment variables from .env file in catalog root.
-
-    Searches for .env file in catalog root directory and loads it using
-    python-dotenv. Does NOT override existing environment variables.
-
-    This enables storing sensitive settings (remote, profile, region) in
-    a .env file that won't be pushed to remote storage.
-
-    Args:
-        catalog_path: Path to catalog root. If None, attempts to find it.
-
-    Returns:
-        True if a .env file was found and loaded, False otherwise.
-    """
-    from dotenv import load_dotenv
-
-    if catalog_path is None:
-        return False
-
-    env_file = catalog_path / ".env"
-    if not env_file.is_file():
-        return False
-
-    load_dotenv(env_file, override=False)
-    return True
-
-
 def check_sensitive_settings_in_config(
     catalog_path: Path, *, state: ConfigSource | None = None
 ) -> list[str]:
diff --git a/portolan_cli/cli.py b/portolan_cli/cli.py
index 7d665e3..865d6e9 100644
--- a/portolan_cli/cli.py
+++ b/portolan_cli/cli.py
@@ -252,6 +252,37 @@ def output_json_envelope(envelope: Any) -> None:
     click.echo(envelope.to_json())
 
 
+def load_dotenv_from_catalog(catalog_path: Path | None = None) -> bool:
+    """Load environment variables from .env file in catalog root.
+
+    It lives in the CLI layer because python-dotenv is a portolan-cli
+    dependency. portolan-core does not install it (#1133).
+
+    Searches for .env file in catalog root directory and loads it using
+    python-dotenv. Does NOT override existing environment variables.
+
+    This enables storing sensitive settings (remote, profile, region) in
+    a .env file that won't be pushed to remote storage.
+
+    Args:
+        catalog_path: Path to catalog root. If None, attempts to find it.
+
+    Returns:
+        True if a .env file was found and loaded, False otherwise.
+    """
+    from dotenv import load_dotenv
+
+    if catalog_path is None:
+        return False
+
+    env_file = catalog_path / ".env"
+    if not env_file.is_file():
+        return False
+
+    load_dotenv(env_file, override=False)
+    return True
+
+
 def load_dotenv_and_warn_sensitive(catalog_path: Path, *, state: RunState | None = None) -> None:
     """Load .env from catalog and warn if config.yaml has sensitive settings.
 
diff --git a/portolan_cli/logging_setup.py b/portolan_cli/logging_setup.py
index ae11195..d903a78 100644
--- a/portolan_cli/logging_setup.py
+++ b/portolan_cli/logging_setup.py
@@ -44,7 +44,8 @@ LOG_ENV = "PORTOLAN_LOG"
 LOG_FILE_ENV = "PORTOLAN_LOG_FILE"
 GDAL_DEBUG_ENV = "PORTOLAN_GDAL_DEBUG"
 
-PACKAGE_LOGGER = "portolan_cli"
+# portolan_core holds the reader modules, so its records are ours too (#1133).
+PACKAGE_LOGGERS = ("portolan_cli", "portolan_core")
 THIRD_PARTY_LOGGERS = ("urllib3", "httpx", "obstore")
 # Rasterio sends GDAL messages to this logger. A CPL_DEBUG message arrives at DEBUG.
 GDAL_LOGGER = "rasterio._env"
@@ -92,7 +93,7 @@ class _RelativeFormatter(_RedactingFormatter):
 
 
 class _UserFormatter(_RelativeFormatter):
-    """Format a ``portolan_cli`` record as an output line: ``⚠ message``.
+    """Format a ``portolan_cli`` or ``portolan_core`` record as an output line: ``⚠ message``.
 
     At the default level a library warning is a message for the operator, so
     it reads like ``output.warn``. An error reads like ``output.error``. The
@@ -101,7 +102,7 @@ class _UserFormatter(_RelativeFormatter):
     """
 
     def _format(self, record: logging.LogRecord) -> str:
-        if record.name != PACKAGE_LOGGER and not record.name.startswith(f"{PACKAGE_LOGGER}."):
+        if record.name.split(".")[0] not in PACKAGE_LOGGERS:
             return super()._format(record)
         prefix = "✗" if record.levelno >= logging.ERROR else "⚠"
         return f"{prefix} {record.getMessage()}"
@@ -153,7 +154,8 @@ def reset_logging() -> None:
     if _saved_root_level is not None:
         root.setLevel(_saved_root_level)
         _saved_root_level = None
-    logging.getLogger(PACKAGE_LOGGER).setLevel(logging.NOTSET)
+    for name in PACKAGE_LOGGERS:
+        logging.getLogger(name).setLevel(logging.NOTSET)
     for name in (*THIRD_PARTY_LOGGERS, GDAL_LOGGER):
         logging.getLogger(name).setLevel(logging.NOTSET)
     logging.captureWarnings(False)
@@ -195,7 +197,8 @@ def configure_logging(*, verbosity: int = 0, quiet: bool = False) -> None:
     # the root level changes only for --quiet. Other libraries stay at
     # WARNING, as with Python's fallback handler.
     if level != logging.WARNING:
-        logging.getLogger(PACKAGE_LOGGER).setLevel(level)
+        for name in PACKAGE_LOGGERS:
+            logging.getLogger(name).setLevel(level)
     if quiet:
         _saved_root_level = root.level
         root.setLevel(logging.ERROR)
diff --git a/pyproject.toml b/pyproject.toml
index cece3ee..9812992 100644
--- a/pyproject.toml
+++ b/pyproject.toml
@@ -530,23 +530,6 @@ type = "forbidden"
 # styles, PMTiles). It is a leaf: it depends on neither scanning nor sync.
 source_modules = ["portolan_cli.viz"]
 forbidden_modules = ["portolan_cli.scan", "portolan_cli.sync"]
-ignore_imports = [
-    # PORTO-CORE-069 makes `file:size`/`file:checksum` on a style asset a claim
-    # about the style file's bytes, so `viz.style` must stamp them. It reuses
-    # `stac_parquet.stamp_file_fields`/`sync_file_extension` rather than hashing
-    # on its own terms, and those reach `sync.checksums` — a stdlib+rashid leaf,
-    # not the sync orchestration this contract exists to keep out of viz. The
-    # same reasoning is written into the scan contract below.
-    "portolan_cli.viz.style -> portolan_cli.stac_parquet",
-    # Same reasoning for the PMTiles writer, which was the one visual artifact
-    # shipping without file:size/file:checksum until issue #746.
-    "portolan_cli.viz.pmtiles -> portolan_cli.stac_parquet",
-    # versions.json records a checksum for each asset. versions and the run
-    # state hash through sync.checksums, the one hasher (issue #995). The
-    # same leaf reasoning applies.
-    "portolan_core.versions -> portolan_core.checksums",
-    "portolan_core.run_state -> portolan_core.checksums",
-]
 
 [[tool.importlinter.contracts]]
 id = "scan-below-sync"
@@ -557,7 +540,7 @@ type = "forbidden"
 #
 # Direct-only: `scan.check` is a command-level orchestrator that legitimately
 # fans out through shared root modules (catalog, metadata, convert), which in
-# turn depend on sync.checksums and viz.*. Those transitive chains are the
+# turn depend on viz.*. Those transitive chains are the
 # existing architecture, not a scan->sync/viz coupling. What we lock here is
 # the boundary that actually matters: no scan module may *directly* import the
 # sync or viz packages. (viz-is-a-leaf stays transitive because viz truly is.)
@@ -808,9 +791,6 @@ DEP002 = [
     "shapely",
     "pygeohash",
     "boto3",
-    # No portolan_cli module imports portolan_core yet. Delete this line when
-    # the first module moves into packages/portolan-core (#1133).
-    "portolan-core",
 ]
 
 [dependency-groups]
diff --git a/scripts/affected_tests.py b/scripts/affected_tests.py
index 3f87680..eef6e1c 100755
--- a/scripts/affected_tests.py
+++ b/scripts/affected_tests.py
@@ -11,7 +11,7 @@ The change is the working tree, committed or not, against the merge base with
 
 Selection:
 
-- A changed ``portolan_cli`` module selects each test file that imports it or
+- A changed ``portolan_cli`` or ``portolan_core`` module selects each test file that imports it or
   imports a module that imports it, directly or indirectly. grimp builds the
   import graph, and imports inside functions count. ``TYPE_CHECKING`` imports
   do not, because they never run.
@@ -51,7 +51,11 @@ from typing import TYPE_CHECKING
 if TYPE_CHECKING:
     from collections.abc import Callable, Iterable
 
-PACKAGE = "portolan_cli"
+# Import name -> source folder. portolan-core is a workspace member (#1133).
+PACKAGES = {
+    "portolan_cli": "portolan_cli",
+    "portolan_core": "packages/portolan-core/src/portolan_core",
+}
 TESTS_DIR = "tests"
 FULL_SUITE_FILES = frozenset({"pyproject.toml", "uv.lock", "tests/conftest.py"})
 FULL_SUITE_PREFIXES = ("tests/fixtures/",)
@@ -85,11 +89,16 @@ class Selection:
 
 
 def module_name(path: str) -> str | None:
-    """Return the dotted module name of a ``portolan_cli`` source file, or None."""
+    """Return the dotted module name of a package source file, or None."""
     pure = PurePosixPath(path)
-    if pure.suffix != ".py" or not pure.parts or pure.parts[0] != PACKAGE:
+    if pure.suffix != ".py":
+        return None
+    for package, folder in PACKAGES.items():
+        if pure.is_relative_to(folder):
+            parts = [package, *pure.relative_to(folder).with_suffix("").parts]
+            break
+    else:
         return None
-    parts = list(pure.with_suffix("").parts)
     if parts[-1] == "__init__":
         parts.pop()
     return ".".join(parts)
@@ -148,7 +157,8 @@ def referenced_names(source: str, path: str | None = None) -> set[str]:
         elif (
             isinstance(node, ast.Constant)
             and isinstance(node.value, str)
-            and node.value.startswith(f"{PACKAGE}.")
+            and node.value.split(".")[0] in PACKAGES
+            and "." in node.value
             and " " not in node.value
         ):
             names.add(node.value)
@@ -156,7 +166,7 @@ def referenced_names(source: str, path: str | None = None) -> set[str]:
 
 
 def imported_modules(source: str, known: set[str]) -> set[str]:
-    """Return the ``portolan_cli`` modules a test file's source refers to.
+    """Return the package modules a test file's source refers to.
 
     Args:
         source: Python source of the test file.
@@ -169,10 +179,10 @@ def imported_modules(source: str, known: set[str]) -> set[str]:
     """
     found = set()
     for name in referenced_names(source):
-        if name.split(".")[0] != PACKAGE:
+        if name.split(".")[0] not in PACKAGES:
             continue
         module = _longest_known_prefix(name, known)
-        if module is not None and module != PACKAGE:
+        if module is not None and module not in PACKAGES:
             found.add(module)
     return found
 
@@ -295,7 +305,18 @@ def _deleted_importers_at(repo: Path, base: str) -> Callable[[str], set[str]]:
         # for it finds every candidate. The parse then drops the false matches.
         leaf = module.rsplit(".", 1)[-1]
         result = subprocess.run(
-            ["git", "grep", "-l", "-w", "-F", leaf, merge_base(), "--", PACKAGE, TESTS_DIR],
+            [
+                "git",
+                "grep",
+                "-l",
+                "-w",
+                "-F",
+                leaf,
+                merge_base(),
+                "--",
+                *PACKAGES.values(),
+                TESTS_DIR,
+            ],
             cwd=repo,
             capture_output=True,
             text=True,
@@ -326,7 +347,7 @@ def select_for_repo(
     sys.path.insert(0, str(repo))
     # cache_dir=None: grimp writes its cache into the working directory by
     # default, which leaves .grimp_cache/ in the repository root.
-    graph = grimp.build_graph(PACKAGE, exclude_type_checking_imports=True, cache_dir=None)
+    graph = grimp.build_graph(*PACKAGES, exclude_type_checking_imports=True, cache_dir=None)
     known = set(graph.modules)
     test_imports, test_texts = _test_index(repo, known)
     return select(
diff --git a/scripts/wheel_smoke.py b/scripts/wheel_smoke.py
index e30052c..c702ef8 100644
--- a/scripts/wheel_smoke.py
+++ b/scripts/wheel_smoke.py
@@ -136,6 +136,7 @@ def files_under(folder: Path) -> set[str]:
 
 def inside() -> None:
     """Run the checks in the environment that holds only the portolan-core wheel."""
+    import importlib
     import importlib.metadata
     import importlib.util
     import runpy
@@ -151,6 +152,16 @@ def inside() -> None:
     check(importable == [], f"importable without the distribution: {importable}")
     print(f"installed: {len(installed)} distributions, none of {FORBIDDEN_DISTRIBUTIONS}")
 
+    # Each module imports with only the declared dependencies installed.
+    import pkgutil
+
+    import portolan_core
+
+    modules = [m.name for m in pkgutil.walk_packages(portolan_core.__path__, "portolan_core.")]
+    for name in modules:
+        importlib.import_module(name)
+    print(f"imported {len(modules)} portolan_core modules")
+
     import obstore
     from obstore.store import MemoryStore
 
diff --git a/tests/perf/runner.py b/tests/perf/runner.py
index 97af82b..5893752 100644
--- a/tests/perf/runner.py
+++ b/tests/perf/runner.py
@@ -56,7 +56,7 @@ _RELAYS = ("portolan_core.trace", "portolan_core.json_io")
 def _site(frame: FrameType | None) -> str:
     while frame is not None:
         module = str(frame.f_globals.get("__name__", ""))
-        if module.startswith("portolan_cli") and module not in _RELAYS:
+        if module.startswith(("portolan_cli", "portolan_core")) and module not in _RELAYS:
             return f"{module}:{frame.f_code.co_name}"
         frame = frame.f_back
     return "outside portolan_cli"
diff --git a/tests/unit/plugins/test_boundary.py b/tests/unit/plugins/test_boundary.py
index 06a2b4a..82f7069 100644
--- a/tests/unit/plugins/test_boundary.py
+++ b/tests/unit/plugins/test_boundary.py
@@ -49,7 +49,9 @@ def _private_imports(source: Path, own_package: str) -> list[str]:
             for module in modules:
                 if module.startswith(own_package):
                     continue
-                if module.split(".")[0] == "portolan_cli" and module not in PUBLIC_MODULES:
+                # portolan_core holds the reader modules, which are not the plugin API (#1133).
+                private = module.split(".")[0] in ("portolan_cli", "portolan_core")
+                if private and module not in PUBLIC_MODULES:
                     found.append(f"{path.relative_to(source)}:{node.lineno} imports {module}")
     return found
 
diff --git a/tests/unit/test_core_package.py b/tests/unit/test_core_package.py
index 06a6ea4..0262702 100644
--- a/tests/unit/test_core_package.py
+++ b/tests/unit/test_core_package.py
@@ -20,18 +20,21 @@ REPO_ROOT = Path(__file__).resolve().parents[2]
 FORBIDDEN = ["click", "rasterio", "pyarrow", "geopandas", "duckdb", "pystac", "portolan_cli"]
 
 _PROBE = f"""
-import json, sys
+import importlib, json, pkgutil, sys
 import portolan_core
-import portolan_core.writer
+modules = [m.name for m in pkgutil.walk_packages(portolan_core.__path__, "portolan_core.")]
+for name in modules:
+    importlib.import_module(name)
 print(json.dumps({{
     "file": portolan_core.__file__,
+    "modules": modules,
     "loaded": sorted(m for m in {FORBIDDEN!r} if m in sys.modules),
 }}))
 """
 
 
 def test_core_import_loads_no_cli_or_geospatial_module() -> None:
-    """`import portolan_core` and its writer load none of the forbidden modules."""
+    """Every portolan_core module imports without loading a forbidden module."""
     result = subprocess.run(
         [sys.executable, "-c", _PROBE], capture_output=True, text=True, check=True
     )
@@ -39,6 +42,7 @@ def test_core_import_loads_no_cli_or_geospatial_module() -> None:
 
     expected = REPO_ROOT / "packages" / "portolan-core" / "src" / "portolan_core" / "__init__.py"
     assert Path(probe["file"]).resolve() == expected
+    assert "portolan_core.versions" in probe["modules"]
     assert probe["loaded"] == []
 
 
diff --git a/tests/unit/test_logging_setup.py b/tests/unit/test_logging_setup.py
index 0b3ed5e..6398442 100644
--- a/tests/unit/test_logging_setup.py
+++ b/tests/unit/test_logging_setup.py
@@ -75,6 +75,13 @@ class TestLevel:
         logging_setup.configure_logging(verbosity=verbosity, quiet=quiet)
         assert PKG.getEffectiveLevel() == expected
 
+    def test_verbose_reaches_the_core_logger(self) -> None:
+        """The reader modules live in portolan_core, so -v covers its logger too (#1133)."""
+        logging_setup.configure_logging(verbosity=1, quiet=False)
+        assert logging.getLogger("portolan_core.config").getEffectiveLevel() == logging.INFO
+        logging_setup.reset_logging()
+        assert logging.getLogger("portolan_core").level == logging.NOTSET
+
     def test_default_leaves_third_party_at_warning(self) -> None:
         logging_setup.configure_logging(verbosity=1, quiet=False)
         assert logging.getLogger().level == logging.WARNING
@@ -341,12 +348,13 @@ class TestGdalDebug:
         assert logging.getLogger("rasterio._env").level == logging.NOTSET
 
 
-def test_package_logger_has_null_handler() -> None:
+@pytest.mark.parametrize("package", ["portolan_cli", "portolan_core"])
+def test_package_logger_has_null_handler(package: str) -> None:
     """A fresh interpreter sees the NullHandler that the package adds on import."""
     code = (
-        "import logging, portolan_cli; "
+        f"import logging, {package}; "
         "print(any(isinstance(h, logging.NullHandler) "
-        "for h in logging.getLogger('portolan_cli').handlers))"
+        f"for h in logging.getLogger('{package}').handlers))"
     )
     result = subprocess.run(
         [sys.executable, "-c", code], capture_output=True, text=True, check=True
diff --git a/uv.lock b/uv.lock
index 156d393..e5398ff 100644
--- a/uv.lock
+++ b/uv.lock
@@ -3785,12 +3785,14 @@ version = "0.9.0"
 source = { editable = "packages/portolan-core" }
 dependencies = [
     { name = "obstore" },
+    { name = "pyyaml" },
     { name = "rashid-core" },
 ]
 
 [package.metadata]
 requires-dist = [
     { name = "obstore", specifier = ">=0.12.1" },
+    { name = "pyyaml", specifier = ">=6.0" },
     { name = "rashid-core", specifier = ">=0.5.1,<0.6.0" },
 ]
 
'''


if __name__ == "__main__":
    _self_check()
    sys.exit(main())
