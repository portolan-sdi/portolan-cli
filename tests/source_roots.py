"""The source folders that the source-scanning gates read (#1133).

The repository holds two distributions. A gate that reads the source must read
both folders, or a module that moves into portolan-core loses the gate.
"""

from __future__ import annotations

from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[1]
CLI_ROOT = REPO_ROOT / "portolan_cli"
CORE_ROOT = REPO_ROOT / "packages" / "portolan-core" / "src" / "portolan_core"
SOURCE_ROOTS = (CLI_ROOT, CORE_ROOT)
